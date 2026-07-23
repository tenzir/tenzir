//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/read_detection.hpp>
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
    append(input);
    while (true) {
      if (state_ == State::magic) {
        if (available() < BITZ_MAGIC.size()) {
          co_return;
        }
        auto magic = consume(BITZ_MAGIC.size());
        if (std::memcmp(magic.data(), BITZ_MAGIC.data(), BITZ_MAGIC.size())
            != 0) {
          state_ = State::invalid_magic;
          co_return;
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
      case State::invalid_magic:
        emit(diagnostic::error("unexpected BITZ magic")
               .note("expected {}",
                     std::string_view{BITZ_MAGIC.data(), BITZ_MAGIC.size()}),
             ctx.dh());
        break;
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return state_ == State::invalid_magic ? OperatorState::done
                                          : OperatorState::normal;
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
  enum class State : uint8_t { magic, header, message, invalid_magic };

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

// Decodes a BITZ payload (a Zstd-compressed Feather IPC stream) into slices.
auto decode_bitz_payload(chunk_ptr message, diagnostic_handler& dh)
  -> generator<table_slice> {
  auto input = std::make_shared<arrow::io::BufferReader>(
    arrow::Buffer::Wrap(message->data(), message->size()));
  auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(
    input, arrow_ipc_read_options());
  if (not reader_result.ok()) {
    diagnostic::error("failed to decode BITZ payload as Feather stream")
      .note("{}", reader_result.status().ToStringWithoutContextLines())
      .emit(dh);
    co_return;
  }
  auto reader = reader_result.MoveValueUnsafe();
  while (true) {
    auto next = reader->ReadNext();
    if (not next.ok()) {
      diagnostic::error("failed to read record batch from BITZ payload")
        .note("{}", next.status().ToStringWithoutContextLines())
        .emit(dh);
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
      diagnostic::error("not implemented")
        .note("cannot convert Feather without Tenzir metadata")
        .emit(dh);
      co_return;
    }
    auto slice = table_slice{next->batch};
    if (slice.rows() == 0) {
      continue;
    }
    co_yield std::move(slice);
  }
  std::ignore = reader->Close();
}

// Serializes a slice into a BITZ payload (a Zstd-compressed Feather IPC stream).
auto encode_bitz_payload(table_slice input, diagnostic_handler& dh)
  -> Option<chunk_ptr> {
  auto default_level
    = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD);
  if (not default_level.ok()) {
    diagnostic::error("failed to get default Zstd compression level")
      .note("{}", default_level.status().ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  auto codec_result
    = arrow::util::Codec::Create(arrow::Compression::ZSTD, *default_level);
  if (not codec_result.ok()) {
    diagnostic::error("failed to create Zstd codec")
      .note("{}", codec_result.status().ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  std::shared_ptr<arrow::util::Codec> codec = codec_result.MoveValueUnsafe();
  auto has_secrets = false;
  std::tie(has_secrets, input) = replace_secrets(std::move(input));
  if (has_secrets) {
    diagnostic::warning("`secret` is serialized as text")
      .note("fields will be `\"***\"`")
      .emit(dh);
  }
  auto batch = to_record_batch(input);
  auto validate_status = batch->Validate();
  TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
  auto sink_result
    = arrow::io::BufferOutputStream::Create(4096, arrow_memory_pool());
  if (not sink_result.ok()) {
    diagnostic::error("failed to create BufferOutputStream")
      .note("{}", sink_result.status().ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  auto sink = sink_result.MoveValueUnsafe();
  auto write_options = arrow::ipc::IpcWriteOptions::Defaults();
  write_options.memory_pool = arrow_memory_pool();
  write_options.codec = codec;
  auto writer_result
    = arrow::ipc::MakeStreamWriter(sink, batch->schema(), write_options);
  if (not writer_result.ok()) {
    diagnostic::error("failed to initialize Feather stream writer")
      .note("{}", writer_result.status().ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  auto writer = writer_result.MoveValueUnsafe();
  auto write_status = writer->WriteRecordBatch(*batch);
  if (not write_status.ok()) {
    diagnostic::error("failed to write record batch")
      .note("{}", write_status.ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  auto close_status = writer->Close();
  if (not close_status.ok()) {
    diagnostic::error("failed to close Feather stream writer")
      .note("{}", close_status.ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  auto buffer_result = sink->Finish();
  if (not buffer_result.ok()) {
    diagnostic::error("failed to finish Feather stream")
      .note("{}", buffer_result.status().ToStringWithoutContextLines())
      .emit(dh);
    return None{};
  }
  return chunk::make(buffer_result.MoveValueUnsafe());
}

// Old-executor operator that reads a BITZ stream into table slices.
class read_bitz_operator final : public crtp_operator<read_bitz_operator> {
public:
  read_bitz_operator() = default;

  auto name() const -> std::string override {
    return "read_bitz";
  }

  auto
  operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    co_yield {};
    auto byte_reader = make_byte_reader(std::move(input));
    while (true) {
      auto magic = byte_reader(BITZ_MAGIC.size());
      while (not magic) {
        co_yield {};
        magic = byte_reader(BITZ_MAGIC.size());
      }
      if (magic->size() < BITZ_MAGIC.size()) {
        if (magic->size() != 0) {
          diagnostic::error("unexpected BITZ magic length {}", magic->size())
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
        co_return;
      }
      auto header = byte_reader(sizeof(uint64_t));
      while (not header) {
        co_yield {};
        header = byte_reader(sizeof(uint64_t));
      }
      if (header->size() < sizeof(uint64_t)) {
        diagnostic::error("unexpected BITZ header length {}", header->size())
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
      for (auto&& slice :
           decode_bitz_payload(std::move(message), ctrl.diagnostics())) {
        co_yield std::move(slice);
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, read_bitz_operator& x) -> bool {
    return f.object(x).fields();
  }
};

// Old-executor operator that writes table slices as a BITZ stream.
class write_bitz_operator final : public crtp_operator<write_bitz_operator> {
public:
  write_bitz_operator() = default;

  auto name() const -> std::string override {
    return "write_bitz";
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    co_yield {};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto payload = encode_bitz_payload(std::move(slice), ctrl.diagnostics());
      if (not payload) {
        co_return;
      }
      auto total_size = detail::to_network_order(
        detail::narrow<uint64_t>((*payload)->size()));
      co_yield chunk::copy(BITZ_MAGIC.data(), BITZ_MAGIC.size());
      co_yield chunk::copy(&total_size, sizeof(total_size));
      co_yield std::move(*payload);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, write_bitz_operator& x) -> bool {
    return f.object(x).fields();
  }
};

class read_bitz_plugin final
  : public virtual operator_plugin2<read_bitz_operator>,
    public virtual ReadOperatorPlugin {
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
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<read_bitz_operator>();
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"bitz"}};
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate("read_bitz", read_detection::specificity::magic,
                                [](read_detection_input input) {
                                  return read_detection::magic_prefix(input,
                                                                      "TNZ1");
                                }),
    };
  }
};

class write_bitz_plugin final
  : public virtual operator_plugin2<write_bitz_operator>,
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
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<write_bitz_operator>();
  }
};

} // namespace
} // namespace tenzir::plugins::bitz

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::read_bitz_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::write_bitz_plugin)
