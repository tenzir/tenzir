//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/byteswap.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/fundamental_array.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/storage_fwd.hpp"
#include "tenzir/nova/type_system.hpp"

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/feather.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/nova/arrow_export.hpp>
#include <tenzir/nova/arrow_import.hpp>
#include <tenzir/nova/arrow_metadata.hpp>
#include <tenzir/nova/bitz.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/util/compression.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <new>
#include <stdexcept>
#include <string_view>

namespace tenzir::plugins::bitz {
namespace {

constexpr auto BITZ_V1_MAGIC = std::array<char, 4>{'T', 'N', 'Z', '1'};
constexpr auto BITZ_V2_MAGIC = std::array<char, 4>{'T', 'N', 'Z', '2'};

using message_length_type = uint64_t;

enum class BitzVersion : uint8_t { v1, v2 };

static_assert(std::endian::native == std::endian::little
              or std::endian::native == std::endian::big);

auto to_little_endian(message_length_type value) -> message_length_type {
  if constexpr (std::endian::native == std::endian::big) {
    return std::byteswap(value);
  }
  return value;
}

auto from_little_endian(message_length_type value) -> message_length_type {
  return to_little_endian(value);
}

struct ReadBitzArgs {
  location operator_location = location::unknown;
};

class ReadBitz final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadBitz(ReadBitzArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    try {
      append(input);
    } catch (std::length_error const&) {
      emit(diagnostic::error("BITZ input requests an invalid allocation"),
           ctx.dh());
      state_ = State::invalid_magic;
      co_return;
    } catch (std::bad_alloc const&) {
      emit(diagnostic::error("insufficient memory for BITZ input"), ctx.dh());
      state_ = State::invalid_magic;
      co_return;
    }
    while (true) {
      if (state_ == State::magic) {
        if (available() < BITZ_V1_MAGIC.size()) {
          co_return;
        }
        auto magic = consume(BITZ_V1_MAGIC.size());
        if (std::memcmp(magic.data(), BITZ_V1_MAGIC.data(),
                        BITZ_V1_MAGIC.size())
            == 0) {
          version_ = BitzVersion::v1;
        } else if (std::memcmp(magic.data(), BITZ_V2_MAGIC.data(),
                               BITZ_V2_MAGIC.size())
                   == 0) {
          version_ = BitzVersion::v2;
        } else {
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
        message_length_ = version_ == BitzVersion::v1
                            ? detail::to_host_order(message_length_)
                            : from_little_endian(message_length_);
        if (message_length_ == 0) {
          emit(diagnostic::error("unexpected empty BITZ message"), ctx.dh());
          co_return;
        }
        if (version_ == BitzVersion::v2
            and message_length_
                  > nova::bitz::default_decode_limits.max_frame_bytes) {
          emit(diagnostic::error("BITZ message exceeds the frame-size limit"),
               ctx.dh());
          state_ = State::invalid_magic;
          co_return;
        }
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
        if (version_ == BitzVersion::v1) {
          co_await parse_v1(message, push, ctx.dh());
        } else {
          co_await parse_v2(message, push, ctx.dh());
        }
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
                 .note("expected {}", BITZ_V1_MAGIC.size()),
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
               .note("expected TNZ1 or TNZ2"),
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
    auto version = static_cast<uint8_t>(version_);
    serde("buffer", buffer_);
    serde("state", state);
    serde("version", version);
    serde("message_length", message_length_);
    state_ = static_cast<State>(state);
    version_ = static_cast<BitzVersion>(version);
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

  static auto parse_v1(std::span<std::byte const> message,
                       Push<table_slice>& push, diagnostic_handler& dh)
    -> Task<void> {
    // A BITZ message payload is a complete, self-contained Feather stream, so
    // we hand the already-buffered message to the shared Feather decoder as a
    // single chunk. The chunk view is non-owning; `message` stays valid until
    // this task completes.
    auto payload = [](chunk_ptr chunk) -> generator<chunk_ptr> {
      co_yield std::move(chunk);
    };
    auto chunk = chunk::make(message, []() noexcept {});
    for (auto&& slice : detail::parse_feather(payload(std::move(chunk)), dh)) {
      if (slice.rows() == 0) {
        continue;
      }
      co_await push(std::move(slice));
    }
  }

  auto parse_v2(std::span<std::byte const> message, Push<table_slice>& push,
                diagnostic_handler& dh) const -> Task<void> {
    auto decoded = nova::bitz::decode(message);
    if (not decoded) {
      emit(diagnostic::error("failed to decode BITZ v2 message")
             .note("{}", decoded.unwrap_err()),
           dh);
      co_return;
    }
    auto batch = std::move(decoded).unwrap();
    auto records = std::move(batch.data).try_as<nova::Record>();
    if (not records) {
      emit(diagnostic::error("BITZ v2 message has a non-record top-level value")
             .note("legacy event pipelines require record events"),
           dh);
      co_return;
    }
    auto events = nova::Events{std::move(*records), std::move(batch.mask),
                               std::move(batch.meta)};
    auto begin = nova::storage::Index{0};
    while (begin < events.length()) {
      while (begin < events.length() and not events.mask.get(begin)) {
        ++begin;
      }
      if (begin == events.length()) {
        break;
      }
      auto const name = *events.meta.name.get(begin);
      auto const import_time = *events.meta.import_time.get(begin);
      auto const internal = *events.meta.internal.get(begin);
      auto end = begin + 1;
      while (end < events.length()) {
        if (events.mask.get(end)
            and (*events.meta.name.get(end) != name
                 or *events.meta.import_time.get(end) != import_time
                 or *events.meta.internal.get(end) != internal)) {
          break;
        }
        ++end;
      }
      auto rows = nova::subslice(events, begin, end);
      for (auto& slice : nova::to_table_slices(rows)) {
        slice.import_time(import_time);
        co_await push(std::move(slice));
      }
      begin = end;
    }
  }

  ReadBitzArgs args_;
  std::string buffer_;
  size_t offset_ = 0;
  State state_ = State::magic;
  BitzVersion version_ = BitzVersion::v1;
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
    co_await push(chunk::copy(BITZ_V1_MAGIC.data(), BITZ_V1_MAGIC.size()));
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

class ReadBitzEvents final : public Operator<chunk_ptr, nova::Events> {
public:
  explicit ReadBitzEvents(ReadBitzArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    try {
      append(input);
    } catch (std::length_error const&) {
      report(diagnostic::error("BITZ input requests an invalid allocation"),
             ctx.dh());
      state_ = State::invalid_magic;
      co_return;
    } catch (std::bad_alloc const&) {
      report(diagnostic::error("insufficient memory for BITZ input"), ctx.dh());
      state_ = State::invalid_magic;
      co_return;
    }
    while (true) {
      if (state_ == State::magic) {
        if (available() < BITZ_V2_MAGIC.size()) {
          co_return;
        }
        auto magic = consume(BITZ_V2_MAGIC.size());
        if (std::memcmp(magic.data(), BITZ_V2_MAGIC.data(),
                        BITZ_V2_MAGIC.size())
            == 0) {
          version_ = Version::v2;
        } else if (std::memcmp(magic.data(), BITZ_V1_MAGIC.data(),
                               BITZ_V1_MAGIC.size())
                   == 0) {
          version_ = Version::v1;
        } else {
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
        message_length_ = version_ == Version::v1
                            ? detail::to_host_order(message_length_)
                            : from_little_endian(message_length_);
        if (message_length_ == 0) {
          report(diagnostic::error("unexpected empty BITZ v2 message"),
                 ctx.dh());
          co_return;
        }
        if (version_ == Version::v2
            and message_length_
                  > nova::bitz::default_decode_limits.max_frame_bytes) {
          report(diagnostic::error("BITZ message exceeds the frame-size limit"),
                 ctx.dh());
          state_ = State::invalid_magic;
          co_return;
        }
        state_ = State::message;
      }
      if (state_ == State::message) {
        auto const message_size = static_cast<size_t>(message_length_);
        if (available() < message_size) {
          co_return;
        }
        auto const* data
          = reinterpret_cast<std::byte const*>(buffer_.data() + offset_);
        auto message = std::span{data, message_size};
        offset_ += message_size;
        state_ = State::magic;
        message_length_ = 0;
        if (version_ == Version::v1) {
          co_await parse_v1(message, push, ctx.dh());
          continue;
        }
        auto decoded = nova::bitz::decode(message);
        if (not decoded) {
          report(diagnostic::error("failed to decode BITZ v2 message")
                   .note("{}", decoded.unwrap_err()),
                 ctx.dh());
          continue;
        }
        auto batch = std::move(decoded).unwrap();
        auto records = std::move(batch.data).try_as<nova::Record>();
        if (not records) {
          report(diagnostic::error("BITZ v2 message has a non-record top-level "
                                   "value")
                   .note("the current event pipeline requires record events"),
                 ctx.dh());
          continue;
        }
        if (batch.mask.any()) {
          co_await push(nova::Events{std::move(*records), std::move(batch.mask),
                                     std::move(batch.meta)});
        }
      }
      if (available() == 0) {
        resize_buffer(0);
        co_return;
      }
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    resize_buffer(available());
    auto const remaining = buffer_.size();
    switch (state_) {
      case State::magic:
        if (remaining != 0) {
          report(diagnostic::error("unexpected BITZ v2 magic length {}",
                                   remaining)
                   .note("expected {}", BITZ_V2_MAGIC.size()),
                 ctx.dh());
        }
        break;
      case State::header:
        report(diagnostic::error("unexpected BITZ v2 header length {}",
                                 remaining)
                 .note("expected {}", sizeof(message_length_type)),
               ctx.dh());
        break;
      case State::message:
        report(diagnostic::error("unexpected BITZ v2 message length {}",
                                 remaining)
                 .note("expected {}", message_length_),
               ctx.dh());
        break;
      case State::invalid_magic:
        report(diagnostic::error("unexpected BITZ v2 magic")
                 .note("expected {}", std::string_view{BITZ_V2_MAGIC.data(),
                                                       BITZ_V2_MAGIC.size()}),
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
    auto version = static_cast<uint8_t>(version_);
    serde("buffer", buffer_);
    serde("state", state);
    serde("version", version);
    serde("message_length", message_length_);
    state_ = static_cast<State>(state);
    version_ = static_cast<Version>(version);
  }

private:
  enum class State : uint8_t { magic, header, message, invalid_magic };
  enum class Version : uint8_t { v1, v2 };

  auto parse_v1(std::span<std::byte const> message, Push<nova::Events>& push,
                diagnostic_handler& dh) -> Task<void> {
    auto payload = [](chunk_ptr chunk) -> generator<chunk_ptr> {
      co_yield std::move(chunk);
    };
    auto chunk = chunk::make(message, []() noexcept {});
    for (auto&& slice : detail::parse_feather(payload(std::move(chunk)), dh)) {
      if (slice.rows() == 0) {
        continue;
      }
      auto import_time = slice.import_time();
      auto batch = to_record_batch(slice);
      auto metadata
        = nova::ArrowMetadata::from_arrow(*batch->schema(), "undefined");
      auto array = batch->ToStructArray();
      if (not array.ok()) {
        report(diagnostic::error("failed to convert BITZ v1 record batch")
                 .note("{}", array.status().ToStringWithoutContextLines()),
               dh);
        continue;
      }
      batch.reset();
      auto imported = nova::import_arrow_array(array.MoveValueUnsafe());
      if (not imported) {
        report(diagnostic::error("failed to import BITZ v1 record batch")
                 .note("{}", imported.unwrap_err()),
               dh);
        continue;
      }
      auto records = std::move(imported).unwrap().try_as<nova::Record>();
      TENZIR_ASSERT(records);
      auto length = records->length();
      auto meta = metadata.to_meta(length);
      meta.import_time = nova::Array<nova::Time>{
        nova::storage::ConstantStorage<nova::Time>{length, import_time}};
      co_await push(nova::Events{std::move(*records),
                                 nova::storage::BitMap{length, true},
                                 std::move(meta)});
    }
  }

  auto report(diagnostic_builder diag, diagnostic_handler& dh) const -> void {
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
    std::memcpy(buffer_.data() + old_size, input->data(), input->size());
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

  ReadBitzArgs args_;
  std::string buffer_;
  size_t offset_ = 0;
  State state_ = State::magic;
  Version version_ = Version::v2;
  message_length_type message_length_ = 0;
};

class WriteBitzEvents final : public Operator<nova::Events, chunk_ptr> {
public:
  explicit WriteBitzEvents(WriteBitzArgs args) : args_{args} {
  }

  auto process(nova::Events input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    for (auto&& result : nova::bitz::encode_batches(
           nova::bitz::Batch{nova::Array<nova::Data>{std::move(input.data)},
                             std::move(input.mask), std::move(input.meta)})) {
      if (not result) {
        report(diagnostic::error("failed to encode BITZ v2 message")
                 .note("{}", result.unwrap_err()),
               ctx.dh());
        co_return;
      }
      auto payload = std::move(result).unwrap();
      auto message_length
        = to_little_endian(detail::narrow<message_length_type>(payload.size()));
      co_await push(chunk::copy(BITZ_V2_MAGIC.data(), BITZ_V2_MAGIC.size()));
      co_await push(chunk::copy(&message_length, sizeof(message_length)));
      co_await push(chunk::copy(payload.data(), payload.size()));
    }
  }

private:
  auto report(diagnostic_builder diag, diagnostic_handler& dh) const -> void {
    if (args_.operator_location) {
      std::move(diag).primary(args_.operator_location).emit(dh);
      return;
    }
    std::move(diag).emit(dh);
  }

  WriteBitzArgs args_;
};

// Yields exactly `remaining` bytes pulled from `byte_reader` in bounded pieces,
// decrementing `remaining` as bytes are produced. Yields an empty chunk for
// backpressure and stops early if the upstream is exhausted.
template <class ByteReader>
auto take_bytes(ByteReader& byte_reader, uint64_t& remaining)
  -> generator<chunk_ptr> {
  constexpr auto piece = size_t{1} << 16;
  while (remaining > 0) {
    auto want = detail::narrow<size_t>(std::min<uint64_t>(remaining, piece));
    auto chunk = byte_reader(want);
    if (not chunk) {
      co_yield {};
      continue;
    }
    if (chunk->size() == 0) {
      co_return;
    }
    remaining -= chunk->size();
    co_yield std::move(chunk);
  }
}

class read_bitz_plugin final : public virtual operator_factory_plugin,
                               public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_bitz";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadBitzArgs, ReadBitz, ReadBitzEvents>{};
    d.operator_location(&ReadBitzArgs::operator_location);
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"bitz"}};
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_bitz", read_detection::specificity::magic,
        [](read_detection_input input) {
          auto result = read_detection::magic_prefix(input, "TNZ1");
          if (result.state == read_detection_result::result_state::reject) {
            return read_detection::magic_prefix(input, "TNZ2");
          }
          return result;
        }),
    };
  }
};

class write_bitz_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_bitz";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteBitzArgs, WriteBitz, WriteBitzEvents>{};
    d.operator_location(&WriteBitzArgs::operator_location);
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::bitz

TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::read_bitz_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::bitz::write_bitz_plugin)
