//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>
#include <arrow/util/key_value_metadata.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <string_view>

namespace tenzir::plugins::bitz {
namespace {

constexpr auto BITZ_MAGIC = std::array<char, 4>{'T', 'N', 'Z', '1'};

using message_length_type = uint64_t;

struct ReadBitzArgs {
  location operator_location = location::unknown;
};

class ReadBitz final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadBitz(ReadBitzArgs args) : args_{std::move(args)} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      co_return;
    }
    append(input);
    while (true) {
      if (state_ == State::magic) {
        if (available() < BITZ_MAGIC.size()) {
          co_return;
        }
        auto magic = consume(BITZ_MAGIC.size());
        if (std::memcmp(magic.data(), BITZ_MAGIC.data(), BITZ_MAGIC.size())
            != 0) {
          emit(diagnostic::error("unexpected BITZ magic")
                 .note("expected {}",
                       std::string_view{BITZ_MAGIC.data(), BITZ_MAGIC.size()}),
               ctx.dh());
        }
        state_ = State::header;
      }
      if (state_ == State::header) {
        if (available() < sizeof(message_length_type)) {
          co_return;
        }
        auto header = consume(sizeof(message_length_type));
        std::memcpy(&message_length_, header.data(), sizeof(message_length_));
        message_length_ = detail::to_host_order(message_length_);
        state_ = State::message;
      }
      if (state_ == State::message) {
        auto const message_size = detail::narrow<size_t>(message_length_);
        if (available() < message_size) {
          co_return;
        }
        auto const* data
          = reinterpret_cast<std::byte const*>(buffer_.data() + offset_);
        auto message = std::span{data, message_size};
        offset_ += message_size;
        state_ = State::magic;
        message_length_ = 0;
        co_await parse_message(message, push, ctx.dh());
      }
      if (available() == 0) {
        resize_buffer(0);
        co_return;
      }
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    resize_buffer(available());
    auto const remaining = buffer_.size();
    switch (state_) {
      case State::magic:
        if (remaining != 0) {
          emit(diagnostic::error("unexpected BITZ magic length {}", remaining)
                 .note("expected {}", BITZ_MAGIC.size()),
               ctx.dh());
        }
        break;
      case State::header:
        emit(diagnostic::error("unexpected BITZ header length {}", remaining)
               .note("expected {}", sizeof(message_length_type)),
             ctx.dh());
        break;
      case State::message:
        emit(diagnostic::error("unexpected message length {}", remaining)
               .note("expected {}", message_length_),
             ctx.dh());
        break;
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& serde) -> void override {
    resize_buffer(available());
    auto state = static_cast<uint8_t>(state_);
    serde("buffer", buffer_);
    serde("state", state);
    serde("message_length", message_length_);
    state_ = static_cast<State>(state);
  }

private:
  enum class State : uint8_t { magic, header, message };

  auto emit(diagnostic_builder diag, diagnostic_handler& dh) const -> void {
    if (args_.operator_location) {
      std::move(diag).primary(args_.operator_location).emit(dh);
      return;
    }
    std::move(diag).emit(dh);
  }

  auto append(chunk_ptr const& input) -> void {
    TENZIR_ASSERT(input);
    auto const old_size = available();
    resize_buffer(old_size + input->size());
    auto const* data = reinterpret_cast<char const*>(input->data());
    std::memcpy(buffer_.data() + old_size, data, input->size());
  }

  auto available() const -> size_t {
    TENZIR_ASSERT(buffer_.size() >= offset_);
    return buffer_.size() - offset_;
  }

  auto consume(size_t size) -> std::string_view {
    TENZIR_ASSERT(available() >= size);
    auto result = std::string_view{buffer_.data() + offset_, size};
    offset_ += size;
    return result;
  }

  auto resize_buffer(size_t target_size) -> void {
    auto const remaining = available();
    TENZIR_ASSERT(target_size >= remaining);
    if (offset_ == 0) {
      buffer_.resize(target_size);
      return;
    }
    if (buffer_.capacity() >= target_size) {
      std::memmove(buffer_.data(), buffer_.data() + offset_, remaining);
      buffer_.resize(target_size);
      offset_ = 0;
      return;
    }
    auto new_buffer = std::string{};
    new_buffer.resize(target_size);
    std::memcpy(new_buffer.data(), buffer_.data() + offset_, remaining);
    buffer_.swap(new_buffer);
    offset_ = 0;
  }

  auto parse_message(std::span<std::byte const> message,
                     Push<table_slice>& push, diagnostic_handler& dh) const
    -> Task<void> {
    auto input = std::make_shared<arrow::io::BufferReader>(
      arrow::Buffer::Wrap(message.data(), message.size()));
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(
      input, arrow_ipc_read_options());
    if (not reader_result.ok()) {
      emit(diagnostic::error("failed to decode BITZ payload as Feather stream")
             .note("{}", reader_result.status().ToStringWithoutContextLines()),
           dh);
      co_return;
    }
    auto reader = reader_result.MoveValueUnsafe();
    while (true) {
      auto next = reader->ReadNext();
      if (not next.ok()) {
        emit(diagnostic::error("failed to read record batch from BITZ payload")
               .note("{}", next.status().ToStringWithoutContextLines()),
             dh);
        co_return;
      }
      if (not next->batch) {
        break;
      }
      auto validate_status = next->batch->Validate();
      TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
      auto const& metadata = next->batch->schema()->metadata();
      if (not metadata
          or std::find(metadata->keys().begin(), metadata->keys().end(),
                       "TENZIR:name:0")
               == metadata->keys().end()) {
        emit(diagnostic::error("not implemented")
               .note("cannot convert Feather without Tenzir metadata"),
             dh);
        co_return;
      }
      co_await push(table_slice{next->batch});
    }
    std::ignore = reader->Close();
  }

  ReadBitzArgs args_;
  std::string buffer_;
  size_t offset_ = 0;
  State state_ = State::magic;
  message_length_type message_length_ = 0;
};

struct WriteBitzArgs {
  location operator_location = location::unknown;
};

class WriteBitz final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteBitz(WriteBitzArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto default_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD);
    if (not default_level.ok()) {
      emit(diagnostic::error("failed to get default Zstd compression level")
             .note("{}", default_level.status().ToStringWithoutContextLines()),
           ctx.dh());
      co_return;
    }
    auto codec_result
      = arrow::util::Codec::Create(arrow::Compression::ZSTD, *default_level);
    if (not codec_result.ok()) {
      emit(diagnostic::error("failed to create Zstd codec")
             .note("{}", codec_result.status().ToStringWithoutContextLines()),
           ctx.dh());
      co_return;
    }
    codec_ = codec_result.MoveValueUnsafe();
    co_return;
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    auto payload = serialize(std::move(input), ctx.dh());
    if (not payload) {
      co_return;
    }
    auto message_length = detail::to_network_order(
      detail::narrow<message_length_type>((*payload)->size()));
    co_await push(chunk::copy(BITZ_MAGIC.data(), BITZ_MAGIC.size()));
    co_await push(chunk::copy(&message_length, sizeof(message_length)));
    co_await push(std::move(*payload));
  }

private:
  auto emit(diagnostic_builder diag, diagnostic_handler& dh) const -> void {
    if (args_.operator_location) {
      std::move(diag).primary(args_.operator_location).emit(dh);
      return;
    }
    std::move(diag).emit(dh);
  }

  auto serialize(table_slice input, diagnostic_handler& dh) const
    -> Option<chunk_ptr> {
    TENZIR_ASSERT(codec_);
    auto has_secrets = false;
    std::tie(has_secrets, input) = replace_secrets(std::move(input));
    if (has_secrets) {
      emit(diagnostic::warning("`secret` is serialized as text")
             .note("fields will be `\"***\"`"),
           dh);
    }
    auto batch = to_record_batch(input);
    auto validate_status = batch->Validate();
    TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
    auto sink_result
      = arrow::io::BufferOutputStream::Create(4096, arrow_memory_pool());
    if (not sink_result.ok()) {
      emit(diagnostic::error("failed to create BufferOutputStream")
             .note("{}", sink_result.status().ToStringWithoutContextLines()),
           dh);
      return None{};
    }
    auto sink = sink_result.MoveValueUnsafe();
    auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
    write_options.memory_pool = arrow_memory_pool();
    write_options.codec = codec_;
    auto writer_result
      = arrow::ipc::MakeStreamWriter(sink, batch->schema(), write_options);
    if (not writer_result.ok()) {
      emit(diagnostic::error("failed to initialize Feather stream writer")
             .note("{}", writer_result.status().ToStringWithoutContextLines()),
           dh);
      return None{};
    }
    auto writer = writer_result.MoveValueUnsafe();
    auto write_status = writer->WriteRecordBatch(*batch);
    if (not write_status.ok()) {
      emit(diagnostic::error("failed to write record batch")
             .note("{}", write_status.ToStringWithoutContextLines()),
           dh);
      return None{};
    }
    auto close_status = writer->Close();
    if (not close_status.ok()) {
      emit(diagnostic::error("failed to close Feather stream writer")
             .note("{}", close_status.ToStringWithoutContextLines()),
           dh);
      return None{};
    }
    auto buffer_result = sink->Finish();
    if (not buffer_result.ok()) {
      emit(diagnostic::error("failed to finish Feather stream")
             .note("{}", buffer_result.status().ToStringWithoutContextLines()),
           dh);
      return None{};
    }
    return chunk::make(buffer_result.MoveValueUnsafe());
  }

  WriteBitzArgs args_;
  std::shared_ptr<arrow::util::Codec> codec_;
};

class bitz_parser final : public plugin_parser {
public:
  bitz_parser() = default;

  auto name() const -> std::string override {
    return "bitz";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return std::invoke(
      [](auto byte_reader,
         operator_control_plane& ctrl) -> generator<table_slice> {
        while (true) {
          auto magic = byte_reader(BITZ_MAGIC.size());
          while (not magic) {
            co_yield {};
            magic = byte_reader(BITZ_MAGIC.size());
          }
          if (magic->size() < BITZ_MAGIC.size()) {
            if (magic->size() != 0) {
              diagnostic::error("unexpected BITZ magic length {}",
                                magic->size())
                .note("expected {}", BITZ_MAGIC.size())
                .emit(ctrl.diagnostics());
            }
            co_return;
          }
          if (std::memcmp(magic->data(), BITZ_MAGIC.data(), BITZ_MAGIC.size())
              != 0) {
            diagnostic::error("unexpected BITZ magic")
              .note("expected {}",
                    std::string_view{BITZ_MAGIC.data(), BITZ_MAGIC.size()})
              .emit(ctrl.diagnostics());
          }
          auto header = byte_reader(sizeof(uint64_t));
          while (not header) {
            co_yield {};
            header = byte_reader(sizeof(uint64_t));
          }
          if (header->size() < sizeof(uint64_t)) {
            diagnostic::error("unexpected BITZ header length {}",
                              header->size())
              .note("expected {}", sizeof(uint64_t))
              .emit(ctrl.diagnostics());
            co_return;
          }
          auto message_length = uint64_t{};
          std::memcpy(&message_length, header->data(), sizeof(uint64_t));
          message_length = detail::to_host_order(message_length);
          auto message = byte_reader(message_length);
          while (not message) {
            co_yield {};
            message = byte_reader(message_length);
          }
          if (message->size() < message_length) {
            diagnostic::error("unexpected message length {}", message->size())
              .note("expected {}", message_length)
              .emit(ctrl.diagnostics());
            co_return;
          }
          auto parser
            = check(pipeline::internal_parse_as_operator("read feather"));
          TENZIR_ASSERT(parser);
          auto untyped_instance = check(parser->instantiate(
            [](auto chunk) -> generator<chunk_ptr> {
              co_yield std::move(chunk);
            }(std::move(message)),
            ctrl));
          auto* instance
            = std::get_if<generator<table_slice>>(&untyped_instance);
          TENZIR_ASSERT(instance);
          while (auto result = instance->next()) {
            if (size(*result) == 0) {
              continue;
            }
            co_yield std::move(*result);
          }
        }
      },
      make_byte_reader(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, bitz_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class bitz_printer final : public plugin_printer {
public:
  bitz_printer() = default;

  auto name() const -> std::string override {
    return "bitz";
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    (void)input_schema;
    return printer_instance::make(
      // NOLINTNEXTLINE(cppcoreguidelines-avoid-capturing-lambda-coroutines)
      [&ctrl](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer = check(pipeline::internal_parse_as_operator(
          "write feather --compression-type zstd"));
        TENZIR_ASSERT(printer);
        auto untyped_instance = check(printer->instantiate(
          [](auto slice) -> generator<table_slice> {
            co_yield std::move(slice);
          }(std::move(slice)),
          ctrl));
        auto* instance = std::get_if<generator<chunk_ptr>>(&untyped_instance);
        TENZIR_ASSERT(instance);
        auto total_size = uint64_t{0};
        auto results = std::vector<chunk_ptr>{};
        while (auto result = instance->next()) {
          auto const chunk_size = size(*result);
          if (chunk_size == 0) {
            continue;
          }
          total_size += size(*result);
          results.push_back(std::move(*result));
        }
        TENZIR_ASSERT(not results.empty());
        TENZIR_ASSERT(total_size > 0);
        total_size = detail::to_network_order(total_size);
        co_yield chunk::copy(BITZ_MAGIC.data(), BITZ_MAGIC.size());
        co_yield chunk::copy(&total_size, sizeof(total_size));
        for (auto&& result : results) {
          co_yield std::move(result);
        }
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  auto prints_utf8() const -> bool override {
    return false;
  }

  friend auto inspect(auto& f, bitz_printer& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<bitz_parser>,
                     public virtual printer_plugin<bitz_printer> {
  auto name() const -> std::string override {
    return "bitz";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"bitz", "https://docs.tenzir.com/"
                                          "formats/bitz"};
    parser.parse(p);
    return std::make_unique<bitz_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"bitz", "https://docs.tenzir.com/"
                                          "formats/bitz"};
    parser.parse(p);
    return std::make_unique<bitz_printer>();
  }
};

class read_bitz_plugin final : public virtual operator_factory_plugin,
                               public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_bitz";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadBitzArgs, ReadBitz>{};
    d.operator_location(&ReadBitzArgs::operator_location);
    return d.without_optimize();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TENZIR_UNUSED(inv, ctx);
    return check(pipeline::internal_parse_as_operator("read bitz"));
  }
};

class write_bitz_plugin final : public virtual operator_factory_plugin,
                                public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_bitz";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteBitzArgs, WriteBitz>{};
    d.operator_location(&WriteBitzArgs::operator_location);
    return d.without_optimize();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TENZIR_UNUSED(inv, ctx);
    return check(pipeline::internal_parse_as_operator("write bitz"));
  }
};

} // namespace
} // namespace tenzir::plugins::bitz

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::read_bitz_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::write_bitz_plugin)
