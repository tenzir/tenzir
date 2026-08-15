//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/detail/byteswap.hpp>
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/pcapng.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <bit>
#include <concepts>
#include <cstring>
#include <limits>

namespace tenzir::plugins::pcap {

namespace {

using namespace tenzir::pcap;

enum class CaptureFormat : uint8_t {
  unknown,
  pcap,
  pcapng,
};

constexpr auto output_interface_block_size = uint32_t{32};
constexpr auto timestamp_resolution_option_size = uint16_t{1};

// Bound operator and checkpoint memory for blocks from untrusted captures.
constexpr auto maximum_pcapng_block_size = uint32_t{64 * 1024 * 1024};
constexpr auto maximum_pcapng_interfaces_per_section = size_t{4'096};
constexpr auto pcapng_output_flush_size = size_t{1 * 1024 * 1024};

auto snapshot_file_header(Serde& serde, FileHeader& header) -> void {
  auto magic_number = uint32_t{header.magic_number};
  auto major_version = uint16_t{header.major_version};
  auto minor_version = uint16_t{header.minor_version};
  auto reserved1 = uint32_t{header.reserved1};
  auto reserved2 = uint32_t{header.reserved2};
  auto snaplen = uint32_t{header.snaplen};
  auto linktype = uint32_t{header.linktype};
  serde("magic_number", magic_number);
  serde("major_version", major_version);
  serde("minor_version", minor_version);
  serde("reserved1", reserved1);
  serde("reserved2", reserved2);
  serde("snaplen", snaplen);
  serde("linktype", linktype);
  if (serde.is_loading()) {
    header = {
      .magic_number = magic_number,
      .major_version = major_version,
      .minor_version = minor_version,
      .reserved1 = reserved1,
      .reserved2 = reserved2,
      .snaplen = snaplen,
      .linktype = linktype,
    };
  }
}

auto snapshot_packet_header(Serde& serde, PacketHeader& header) -> void {
  auto timestamp = uint32_t{header.timestamp};
  auto timestamp_fraction = uint32_t{header.timestamp_fraction};
  auto captured_packet_length = uint32_t{header.captured_packet_length};
  auto original_packet_length = uint32_t{header.original_packet_length};
  serde("timestamp", timestamp);
  serde("timestamp_fraction", timestamp_fraction);
  serde("captured_packet_length", captured_packet_length);
  serde("original_packet_length", original_packet_length);
  if (serde.is_loading()) {
    header = {
      .timestamp = timestamp,
      .timestamp_fraction = timestamp_fraction,
      .captured_packet_length = captured_packet_length,
      .original_packet_length = original_packet_length,
    };
  }
}

template <std::integral T>
auto read_number(std::span<std::byte const> bytes, size_t offset,
                 bool need_swap) -> T {
  TENZIR_ASSERT(offset + sizeof(T) <= bytes.size());
  auto result = T{};
  std::memcpy(&result, bytes.data() + offset, sizeof(T));
  if (need_swap and sizeof(T) > 1) {
    result = detail::byteswap(result);
  }
  return result;
}

template <std::integral T>
auto append_little_endian(std::vector<std::byte>& buffer, T value) -> void {
  if constexpr (std::endian::native == std::endian::big) {
    value = detail::byteswap(value);
  }
  auto bytes = std::as_bytes(std::span{std::addressof(value), size_t{1}});
  buffer.insert(buffer.end(), bytes.begin(), bytes.end());
}

auto append_pcapng_section_header(std::vector<std::byte>& buffer) -> void {
  append_little_endian(buffer, pcapng::magic_number);
  append_little_endian(buffer, pcapng::section_header_block_min_size);
  append_little_endian(buffer, pcapng::byte_order_magic);
  append_little_endian(buffer, pcapng::current_major_version);
  append_little_endian(buffer, pcapng::current_minor_version);
  append_little_endian(buffer, uint64_t{std::numeric_limits<uint64_t>::max()});
  append_little_endian(buffer, pcapng::section_header_block_min_size);
}

auto append_pcapng_interface(std::vector<std::byte>& buffer, uint16_t linktype)
  -> void {
  append_little_endian(buffer, pcapng::interface_description_block);
  append_little_endian(buffer, output_interface_block_size);
  append_little_endian(buffer, linktype);
  append_little_endian(buffer, uint16_t{0});
  append_little_endian(buffer, pcapng::unlimited_snaplen);
  append_little_endian(buffer, pcapng::interface_timestamp_resolution_option);
  append_little_endian(buffer, timestamp_resolution_option_size);
  append_little_endian(buffer, pcapng::nanosecond_timestamp_resolution);
  auto padding = detail::narrow<size_t>(
    pcapng::padded_size(timestamp_resolution_option_size)
    - timestamp_resolution_option_size);
  buffer.insert(buffer.end(), padding, std::byte{0});
  append_little_endian(buffer, pcapng::end_of_options);
  append_little_endian(buffer, uint16_t{0});
  append_little_endian(buffer, output_interface_block_size);
}

constexpr auto pcapng_packet_block_size(uint32_t captured_packet_length)
  -> uint64_t {
  return uint64_t{pcapng::packet_block_min_size}
         + pcapng::padded_size(captured_packet_length);
}

// The caller must ensure that `raw_timestamp` uses nanosecond resolution.
auto append_pcapng_packet(std::vector<std::byte>& buffer, uint32_t interface_id,
                          PacketRecord const& packet, uint64_t raw_timestamp)
  -> bool {
  if (packet.header.captured_packet_length != packet.data.size()) {
    return false;
  }
  auto data_size = packet.header.captured_packet_length;
  auto padding
    = detail::narrow_cast<size_t>(pcapng::padded_size(data_size) - data_size);
  auto block_size = pcapng_packet_block_size(data_size);
  if (block_size > maximum_pcapng_block_size) {
    return false;
  }
  append_little_endian(buffer, pcapng::enhanced_packet_block);
  append_little_endian(buffer, detail::narrow_cast<uint32_t>(block_size));
  append_little_endian(buffer, interface_id);
  append_little_endian(buffer,
                       detail::narrow_cast<uint32_t>(raw_timestamp >> 32));
  append_little_endian(buffer, detail::narrow_cast<uint32_t>(raw_timestamp
                                                             & 0xffffffff));
  append_little_endian(buffer, packet.header.captured_packet_length);
  append_little_endian(buffer, packet.header.original_packet_length);
  buffer.insert(buffer.end(), packet.data.begin(), packet.data.end());
  buffer.insert(buffer.end(), padding, std::byte{0});
  append_little_endian(buffer, detail::narrow_cast<uint32_t>(block_size));
  return true;
}

auto normalized_magic_number(uint32_t raw_magic) -> Option<uint32_t> {
  auto need_swap = tenzir::pcap::need_byte_swap(raw_magic);
  if (not need_swap) {
    return None{};
  }
  return *need_swap ? detail::byteswap(raw_magic) : raw_magic;
}

auto uses_microsecond_precision(uint32_t raw_magic) -> bool {
  auto normalized = normalized_magic_number(raw_magic);
  TENZIR_ASSERT(normalized);
  return *normalized == magic_number_1;
}

auto serialize_file_header(FileHeader header) -> FileHeader {
  auto need_swap = tenzir::pcap::need_byte_swap(header.magic_number);
  TENZIR_ASSERT(need_swap);
  if (not *need_swap) {
    return header;
  }
  header.major_version = detail::byteswap(header.major_version);
  header.minor_version = detail::byteswap(header.minor_version);
  header.reserved1 = detail::byteswap(header.reserved1);
  header.reserved2 = detail::byteswap(header.reserved2);
  header.snaplen = detail::byteswap(header.snaplen);
  header.linktype = detail::byteswap(header.linktype);
  return header;
}

auto serialize_packet_header(PacketHeader header, uint32_t raw_magic)
  -> PacketHeader {
  auto need_swap = tenzir::pcap::need_byte_swap(raw_magic);
  TENZIR_ASSERT(need_swap);
  return *need_swap ? byteswap(header) : header;
}

auto make_file_header_table_slice(FileHeader const& header, uint32_t raw_magic)
  -> table_slice {
  auto builder = series_builder{type{
    "pcap.file_header",
    record_type{
      {"magic_number", uint64_type{}},  // uint32
      {"major_version", uint64_type{}}, // uint32
      {"minor_version", uint64_type{}}, // uint32
      {"reserved1", uint64_type{}},     // uint32
      {"reserved2", uint64_type{}},     // uint32
      {"snaplen", uint64_type{}},       // uint32
      {"linktype", uint64_type{}},      // uint16
    },
  }};
  auto event = builder.record();
  event.field("magic_number").data(uint64_t{raw_magic});
  event.field("major_version").data(uint64_t{header.major_version});
  event.field("minor_version").data(uint64_t{header.minor_version});
  event.field("reserved1").data(uint64_t{header.reserved1});
  event.field("reserved2").data(uint64_t{header.reserved2});
  event.field("snaplen").data(uint64_t{header.snaplen});
  event.field("linktype").data(uint64_t{header.linktype});
  return builder.finish_assert_one_slice();
}

struct ParserArgs {
  Option<location> emit_file_headers;

  template <class Inspector>
  friend auto inspect(Inspector& f, ParserArgs& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("emit_file_headers", x.emit_file_headers));
  }
};

class PcapParser final : public plugin_parser {
public:
  PcapParser() = default;

  explicit PcapParser(ParserArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "pcap";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> Option<generator<table_slice>> override {
    auto make = [](auto& ctrl, generator<chunk_ptr> input,
                   bool emit_file_headers) -> generator<table_slice> {
      // A PCAP file starts with a 24-byte header.
      auto input_file_header = FileHeader{};
      auto read_n = make_byte_view_reader(std::move(input));
      co_yield {};
      while (true) {
        auto length = sizeof(FileHeader);
        auto bytes = read_n(length);
        if (not bytes) {
          co_yield {};
          continue;
        }
        if (bytes->size() != length) {
          diagnostic::error("PCAP file header to short")
            .note("from `pcap`")
            .note("expected {} bytes, but got {}", length, bytes->size())
            .emit(ctrl.diagnostics());
          co_return;
        }
        std::memcpy(&input_file_header, bytes->data(), bytes->size());
        break;
      }
      if (input_file_header.magic_number == pcapng::magic_number) {
        diagnostic::error("PCAPng currently unsupported")
          .hint("use `shell \"tshark -F pcap -r - -w -\"` to convert to PCAP")
          .note("visit https://github.com/tenzir/public-roadmap/issues/75")
          .emit(ctrl.diagnostics());
        co_return;
      }
      auto raw_magic = input_file_header.magic_number;
      auto need_swap = need_byte_swap(raw_magic);
      if (not need_swap) {
        diagnostic::error("invalid PCAP magic number: {0:x}",
                          uint32_t{raw_magic})
          .note("from `pcap`")
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (*need_swap) {
        TENZIR_DEBUG("detected different byte order in file and host");
        input_file_header = byteswap(input_file_header);
      } else {
        TENZIR_DEBUG("detected identical byte order in file and host");
      }
      if (emit_file_headers) {
        co_yield make_file_header_table_slice(input_file_header, raw_magic);
      }
      // After the header, the remainder of the file are typically Packet
      // Records, consisting of a 16-byte header and variable-length payload.
      // However, our parser is a bit smarter and also supports concatenated
      // PCAP traces.
      auto builder = series_builder{type{
        "pcap.packet",
        record_type{
          {"linktype", uint64_type{}}, // uint16 would suffice
          {"timestamp", time_type{}},
          {"captured_packet_length", uint64_type{}},
          {"original_packet_length", uint64_type{}},
          {"data", type{blob_type{}, {{"skip"}}}},
        },
      }};
      auto num_packets = size_t{0};
      auto last_finish = std::chrono::steady_clock::now();
      while (true) {
        auto const now = std::chrono::steady_clock::now();
        if (builder.length() >= detail::narrow_cast<int64_t>(
              defaults::import::table_slice_size)
            or last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish_assert_one_slice();
        }
        PacketRecord packet;
        // We first try to parse a packet header first.
        while (true) {
          TENZIR_DEBUG("reading packet header");
          auto length = sizeof(PacketHeader);
          auto bytes = read_n(length);
          if (not bytes) {
            if (last_finish != now) {
              co_yield {};
            }
            continue;
          }
          if (bytes->empty()) {
            TENZIR_DEBUG("completed trace of {} packets", num_packets);
            if (builder.length() > 0) {
              co_yield builder.finish_assert_one_slice();
            }
            co_return;
          }
          if (bytes->size() < length) {
            diagnostic::error("PCAP packet header to short")
              .note("from `pcap`")
              .note("expected {} bytes, but got {}", length, bytes->size())
              .emit(ctrl.diagnostics());
            co_return;
          }
          std::memcpy(&packet.header, bytes->data(), sizeof(PacketHeader));
          if (is_file_header(packet.header)) {
            TENZIR_DEBUG("detected new PCAP file header");
            auto file_header_bytes = as_writeable_bytes(input_file_header);
            auto packet_header_bytes = as_bytes(packet.header);
            std::copy(packet_header_bytes.begin(), packet_header_bytes.end(),
                      file_header_bytes.begin());
            // Read the remaining two fields of the packet header.
            while (true) {
              constexpr auto length
                = sizeof(FileHeader::snaplen) + sizeof(FileHeader::linktype);
              auto bytes = read_n(length);
              if (not bytes) {
                co_yield {};
                continue;
              }
              if (bytes->size() != length) {
                diagnostic::error("failed to read remaining PCAP file header")
                  .hint("got {} bytes but needed {}", bytes->size(), length)
                  .emit(ctrl.diagnostics());
                co_return;
              }
              TENZIR_ASSERT(sizeof(FileHeader) - sizeof(PacketHeader)
                            == bytes->size());
              auto remainder
                = file_header_bytes.subspan<sizeof(PacketHeader)>();
              std::copy(bytes->begin(), bytes->end(), remainder.begin());
              break;
            }
            raw_magic = input_file_header.magic_number;
            need_swap = need_byte_swap(raw_magic);
            TENZIR_ASSERT(need_swap); // checked in is_file_header
            if (*need_swap) {
              TENZIR_DEBUG("detected different byte order in file and host");
              input_file_header = byteswap(input_file_header);
            } else {
              TENZIR_DEBUG("detected identical byte order in file and host");
            }
            // Before emitting the new file header, flush all buffered packets
            // from the previous trace.
            if (builder.length() > 0) {
              last_finish = now;
              co_yield builder.finish_assert_one_slice();
            }
            if (emit_file_headers) {
              co_yield make_file_header_table_slice(input_file_header,
                                                    raw_magic);
            }
            //  Jump back to the while loop that reads pairs of packet header
            //  and packet data.
            continue;
          }
          // Okay, we got a packet header, let's proceed.
          if (*need_swap) {
            packet.header = byteswap(packet.header);
          }
          break;
        }
        // Read the packet.
        while (true) {
          TENZIR_DEBUG("reading packet data of size {}",
                       uint32_t{packet.header.captured_packet_length});
          auto length = packet.header.captured_packet_length;
          auto bytes = read_n(length);
          if (not bytes) {
            if (last_finish != now) {
              co_yield {};
            }
            continue;
          }
          if (bytes->size() != length) {
            co_yield builder.finish_assert_one_slice();
            diagnostic::error("truncated last packet; expected {} but got {}",
                              length, bytes->size())
              .note("from `pcap`")
              .emit(ctrl.diagnostics());
            co_return;
          }
          packet.data = *bytes;
          break;
        }
        ++num_packets;
        TENZIR_DEBUG("packet #{} got size: {}", num_packets,
                     packet.data.size());
        /// Build record.
        auto seconds = std::chrono::seconds(packet.header.timestamp);
        auto timestamp = time{std::chrono::duration_cast<duration>(seconds)};
        if (input_file_header.magic_number == magic_number_1) {
          timestamp
            += std::chrono::microseconds(packet.header.timestamp_fraction);
        } else if (input_file_header.magic_number == magic_number_2) {
          timestamp
            += std::chrono::nanoseconds(packet.header.timestamp_fraction);
        } else {
          // we validated the magic number earlier
          TENZIR_UNREACHABLE();
        }
        auto data = view<blob>{packet.data.data(), packet.data.size()};
        auto event = builder.record();
        event.field("timestamp").data(timestamp);
        event.field("linktype")
          .data(uint64_t{input_file_header.linktype & 0x0000FFFF});
        event.field("captured_packet_length")
          .data(uint64_t{packet.header.captured_packet_length});
        event.field("original_packet_length")
          .data(uint64_t{packet.header.original_packet_length});
        event.field("data").data(data);
      }
      if (builder.length() > 0) {
        co_yield builder.finish_assert_one_slice();
      }
    };
    return make(ctrl, std::move(input), args_.emit_file_headers.has_value());
  }

  friend auto inspect(auto& f, PcapParser& x) -> bool {
    return f.object(x)
      .pretty_name("pcap_parser")
      .fields(f.field("args", x.args_));
  }

private:
  ParserArgs args_;
};

auto make_packet_table_slice_type() -> type {
  return type{
    "pcap.packet",
    record_type{
      {"linktype", uint64_type{}}, // uint16 would suffice
      {"timestamp", time_type{}},
      {"captured_packet_length", uint64_type{}},
      {"original_packet_length", uint64_type{}},
      {"data", type{blob_type{}, {{"skip"}}}},
    },
  };
}

auto make_pcapng_packet_table_slice_type() -> type {
  return type{
    "pcap.packet",
    record_type{
      {"linktype", uint64_type{}},
      {"timestamp", time_type{}},
      {"captured_packet_length", uint64_type{}},
      {"original_packet_length", uint64_type{}},
      {"data", type{blob_type{}, {{"skip"}}}},
      {"section_id", uint64_type{}},
      {"interface_id", uint64_type{}},
    },
  };
}

struct PcapngInterface {
  uint16_t linktype = 0;
  uint32_t snaplen = 0;
  uint8_t timestamp_resolution = pcapng::default_timestamp_resolution;
  int64_t timestamp_offset = 0;

  template <class Inspector>
  friend auto inspect(Inspector& f, PcapngInterface& x) -> bool {
    return f.object(x).fields(
      f.field("linktype", x.linktype), f.field("snaplen", x.snaplen),
      f.field("timestamp_resolution", x.timestamp_resolution),
      f.field("timestamp_offset", x.timestamp_offset));
  }
};

struct ReadPcapArgs {
  bool emit_file_headers = false;
};

class ReadPcap final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadPcap(ReadPcapArgs args)
    : args_{std::move(args)}, builder_{make_packet_table_slice_type()} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    append(as_bytes(input));
    auto now = series_builder::clock::now();
    co_await parse_available(push, ctx.dh());
    co_await pusher_.push(builder_.yield_ready("", now), push);
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await pusher_.push(builder_.yield_ready(), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (not failed_) {
      co_await parse_available(push, ctx.dh());
    }
    if (builder_.length() > 0) {
      co_await flush_packets(push);
    }
    if (failed_) {
      co_return FinalizeBehavior::done;
    }
    if (format_ == CaptureFormat::pcapng) {
      if (available() > 0) {
        diagnostic::error("truncated PCAPNG block")
          .note("got {} trailing bytes", available())
          .emit(ctx.dh());
      }
      co_return FinalizeBehavior::done;
    }
    if (pending_packet_header_) {
      auto const captured_packet_length
        = pending_packet_header_->captured_packet_length;
      diagnostic::error("truncated last packet; expected {} but got {}",
                        captured_packet_length, available())
        .note("from `pcap`")
        .emit(ctx.dh());
      co_return FinalizeBehavior::done;
    }
    if (available() == 0) {
      co_return FinalizeBehavior::done;
    }
    if (not have_file_header_) {
      diagnostic::error("PCAP file header to short")
        .note("from `pcap`")
        .note("expected {} bytes, but got {}", sizeof(FileHeader), available())
        .emit(ctx.dh());
      co_return FinalizeBehavior::done;
    }
    if (available() < sizeof(PacketHeader)) {
      diagnostic::error("PCAP packet header to short")
        .note("from `pcap`")
        .note("expected {} bytes, but got {}", sizeof(PacketHeader),
              available())
        .emit(ctx.dh());
      co_return FinalizeBehavior::done;
    }
    auto header = PacketHeader{};
    auto bytes = view(sizeof(PacketHeader));
    TENZIR_ASSERT(bytes);
    std::memcpy(&header, bytes->data(), bytes->size());
    if (is_file_header(header)) {
      diagnostic::error("failed to read remaining PCAP file header")
        .hint("got {} bytes but needed {}", available() - sizeof(PacketHeader),
              sizeof(FileHeader) - sizeof(PacketHeader))
        .emit(ctx.dh());
      co_return FinalizeBehavior::done;
    }
    if (need_swap_) {
      header = byteswap(header);
    }
    auto const captured_packet_length = header.captured_packet_length;
    diagnostic::error("truncated last packet; expected {} but got {}",
                      captured_packet_length,
                      available() - sizeof(PacketHeader))
      .note("from `pcap`")
      .emit(ctx.dh());
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    co_await flush_packets(push);
  }

  auto snapshot(Serde& serde) -> void override {
    // An incomplete header, block, or packet cannot be flushed at a checkpoint.
    compact();
    serde("buffer", buffer_);
    auto format = static_cast<uint8_t>(format_);
    serde("format", format);
    TENZIR_ASSERT(format <= static_cast<uint8_t>(CaptureFormat::pcapng));
    format_ = static_cast<CaptureFormat>(format);
    serde("failed", failed_);
    serde("have_file_header", have_file_header_);
    serde("need_swap", need_swap_);
    serde("current_file_header_raw_magic", current_file_header_raw_magic_);
    snapshot_file_header(serde, current_file_header_);
    auto has_pending_packet_header = pending_packet_header_.has_value();
    serde("has_pending_packet_header", has_pending_packet_header);
    if (serde.is_loading()) {
      pending_packet_header_
        = has_pending_packet_header ? Option{PacketHeader{}} : None{};
    }
    if (pending_packet_header_) {
      snapshot_packet_header(serde, *pending_packet_header_);
    }
    serde("pcapng_have_section", pcapng_have_section_);
    serde("pcapng_need_swap", pcapng_need_swap_);
    serde("pcapng_section_id", pcapng_section_id_);
    serde("pcapng_interfaces", pcapng_interfaces_);
    if (serde.is_loading()) {
      builder_ = series_builder{format_ == CaptureFormat::pcapng
                                  ? make_pcapng_packet_table_slice_type()
                                  : make_packet_table_slice_type()};
    }
  }

private:
  auto compact() -> void {
    if (offset_ == 0) {
      return;
    }
    if (offset_ == buffer_.size()) {
      buffer_.clear();
      offset_ = 0;
      return;
    }
    auto remaining = available();
    std::memmove(buffer_.data(), buffer_.data() + offset_, remaining);
    buffer_.resize(remaining);
    offset_ = 0;
  }

  auto append(std::span<std::byte const> bytes) -> void {
    if (offset_ > 0
        and (offset_ == buffer_.size() or offset_ * 2 >= buffer_.size())) {
      compact();
    }
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
  }

  auto available() const -> size_t {
    return buffer_.size() - offset_;
  }

  auto view(size_t size) const -> Option<std::span<std::byte const>> {
    if (available() < size) {
      return None{};
    }
    return std::span<std::byte const>{buffer_.data() + offset_, size};
  }

  auto consume(size_t size) -> void {
    TENZIR_ASSERT(available() >= size);
    offset_ += size;
    if (offset_ == buffer_.size()) {
      buffer_.clear();
      offset_ = 0;
    }
  }

  auto parse_file_header_bytes(std::span<std::byte const> bytes,
                               diagnostic_handler& dh) -> Option<FileHeader> {
    TENZIR_ASSERT(bytes.size() == sizeof(FileHeader));
    auto header = FileHeader{};
    std::memcpy(&header, bytes.data(), bytes.size());
    auto raw_magic = header.magic_number;
    auto need_swap = tenzir::pcap::need_byte_swap(raw_magic);
    if (not need_swap) {
      diagnostic::error("invalid PCAP magic number: {0:x}", uint32_t{raw_magic})
        .note("from `pcap`")
        .emit(dh);
      failed_ = true;
      return None{};
    }
    need_swap_ = *need_swap;
    current_file_header_raw_magic_ = raw_magic;
    if (*need_swap) {
      TENZIR_DEBUG("detected different byte order in file and host");
      header = byteswap(header);
    } else {
      TENZIR_DEBUG("detected identical byte order in file and host");
    }
    return header;
  }

  auto emit_file_header(FileHeader const& header, Push<table_slice>& push)
    -> Task<void> {
    if (builder_.length() > 0) {
      co_await flush_packets(push);
    }
    if (args_.emit_file_headers) {
      co_await push(
        make_file_header_table_slice(header, current_file_header_raw_magic_));
    }
  }

  auto append_packet(PacketHeader const& header,
                     std::span<std::byte const> data) -> void {
    auto seconds = std::chrono::seconds(header.timestamp);
    auto timestamp = time{std::chrono::duration_cast<duration>(seconds)};
    if (uses_microsecond_precision(current_file_header_raw_magic_)) {
      timestamp += std::chrono::microseconds(header.timestamp_fraction);
    } else {
      timestamp += std::chrono::nanoseconds(header.timestamp_fraction);
    }
    auto event = builder_.record();
    event.field("timestamp").data(timestamp);
    event.field("linktype")
      .data(uint64_t{current_file_header_.linktype & 0xFFFF});
    event.field("captured_packet_length")
      .data(uint64_t{header.captured_packet_length});
    event.field("original_packet_length")
      .data(uint64_t{header.original_packet_length});
    event.field("data").data(tenzir::view<blob>{data.data(), data.size()});
  }

  auto flush_packets(Push<table_slice>& push) -> Task<void> {
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice());
    }
  }

  auto flush_packets_if_full(Push<table_slice>& push) -> Task<void> {
    if (builder_.length()
        >= detail::narrow_cast<int64_t>(defaults::import::table_slice_size)) {
      co_await flush_packets(push);
    }
  }

  auto fail_pcapng(std::string message, diagnostic_handler& dh) -> void {
    diagnostic::error("{}", message).note("from `pcapng`").emit(dh);
    failed_ = true;
  }

  auto parse_pcapng_section(std::span<std::byte const> block, bool need_swap,
                            diagnostic_handler& dh) -> bool {
    if (block.size() < pcapng::section_header_block_min_size) {
      fail_pcapng("PCAPNG section header block is too short", dh);
      return false;
    }
    auto major = read_number<uint16_t>(block, 12, need_swap);
    auto minor = read_number<uint16_t>(block, 14, need_swap);
    if (major != pcapng::current_major_version
        or (minor != pcapng::current_minor_version
            and minor != pcapng::compatible_minor_version)) {
      diagnostic::error("unsupported PCAPNG version {}.{}", major, minor)
        .note("from `pcapng`")
        .emit(dh);
      failed_ = true;
      return false;
    }
    pcapng_need_swap_ = need_swap;
    pcapng_have_section_ = true;
    ++pcapng_section_id_;
    pcapng_interfaces_.clear();
    return true;
  }

  auto parse_pcapng_interface(std::span<std::byte const> block,
                              diagnostic_handler& dh) -> bool {
    if (block.size() < pcapng::interface_description_block_min_size) {
      fail_pcapng("PCAPNG interface description block is too short", dh);
      return false;
    }
    if (pcapng_interfaces_.size() >= maximum_pcapng_interfaces_per_section) {
      diagnostic::error("PCAPNG section exceeds maximum interface count")
        .note("maximum is {}", maximum_pcapng_interfaces_per_section)
        .note("from `pcapng`")
        .emit(dh);
      failed_ = true;
      return false;
    }
    auto interface = PcapngInterface{
      .linktype = read_number<uint16_t>(block, 8, pcapng_need_swap_),
      .snaplen = read_number<uint32_t>(block, 12, pcapng_need_swap_),
    };
    auto offset = size_t{16};
    auto options_end = block.size() - sizeof(uint32_t);
    while (offset < options_end) {
      if (options_end - offset < 4) {
        fail_pcapng("truncated PCAPNG interface option", dh);
        return false;
      }
      auto code = read_number<uint16_t>(block, offset, pcapng_need_swap_);
      auto length = read_number<uint16_t>(block, offset + 2, pcapng_need_swap_);
      offset += 4;
      auto padded_length
        = detail::narrow_cast<size_t>(pcapng::padded_size(length));
      if (padded_length > options_end - offset) {
        fail_pcapng("invalid PCAPNG interface option length", dh);
        return false;
      }
      if (code == pcapng::end_of_options) {
        break;
      }
      if (code == pcapng::interface_timestamp_resolution_option
          and length == 1) {
        interface.timestamp_resolution
          = std::to_integer<uint8_t>(block[offset]);
      } else if (code == pcapng::interface_timestamp_offset_option
                 and length == 8) {
        auto raw = read_number<uint64_t>(block, offset, pcapng_need_swap_);
        interface.timestamp_offset = std::bit_cast<int64_t>(raw);
      }
      offset += padded_length;
    }
    pcapng_interfaces_.push_back(interface);
    return true;
  }

  auto parse_pcapng_packet(std::span<std::byte const> block,
                           diagnostic_handler& dh) -> bool {
    if (block.size() < pcapng::packet_block_min_size) {
      fail_pcapng("PCAPNG packet block is too short", dh);
      return false;
    }
    auto block_type = read_number<uint32_t>(block, 0, pcapng_need_swap_);
    auto interface_id
      = block_type == pcapng::packet_block
          ? uint32_t{read_number<uint16_t>(block, 8, pcapng_need_swap_)}
          : read_number<uint32_t>(block, 8, pcapng_need_swap_);
    if (interface_id >= pcapng_interfaces_.size()) {
      diagnostic::error("PCAPNG packet references unknown interface {}",
                        interface_id)
        .note("from `pcapng`")
        .emit(dh);
      failed_ = true;
      return false;
    }
    auto timestamp_high = read_number<uint32_t>(block, 12, pcapng_need_swap_);
    auto timestamp_low = read_number<uint32_t>(block, 16, pcapng_need_swap_);
    auto captured_length = read_number<uint32_t>(block, 20, pcapng_need_swap_);
    auto original_length = read_number<uint32_t>(block, 24, pcapng_need_swap_);
    if (captured_length > original_length) {
      fail_pcapng("PCAPNG captured packet length exceeds original length", dh);
      return false;
    }
    auto packet_end = uint64_t{pcapng::packet_data_offset}
                      + pcapng::padded_size(captured_length);
    if (packet_end + sizeof(uint32_t) > block.size()) {
      fail_pcapng("PCAPNG packet data exceeds its block", dh);
      return false;
    }
    auto const& interface = pcapng_interfaces_[interface_id];
    if (interface.snaplen != pcapng::unlimited_snaplen
        and captured_length > interface.snaplen) {
      fail_pcapng("PCAPNG captured packet length exceeds interface snaplen",
                  dh);
      return false;
    }
    auto raw_timestamp = (uint64_t{timestamp_high} << 32) | timestamp_low;
    auto timestamp = pcapng::decode_timestamp(
      raw_timestamp, {.resolution = interface.timestamp_resolution,
                      .offset_seconds = interface.timestamp_offset});
    if (not timestamp) {
      fail_pcapng("PCAPNG packet timestamp is out of range", dh);
      return false;
    }
    auto data = block.subspan(pcapng::packet_data_offset, captured_length);
    auto event = builder_.record();
    event.field("timestamp").data(*timestamp);
    event.field("linktype").data(uint64_t{interface.linktype});
    event.field("captured_packet_length").data(uint64_t{captured_length});
    event.field("original_packet_length").data(uint64_t{original_length});
    event.field("data").data(tenzir::view<blob>{data.data(), data.size()});
    event.field("section_id").data(pcapng_section_id_);
    event.field("interface_id").data(uint64_t{interface_id});
    return true;
  }

  auto parse_pcapng_simple_packet(std::span<std::byte const> block,
                                  diagnostic_handler& dh) -> bool {
    if (block.size() < pcapng::simple_packet_block_min_size) {
      fail_pcapng("PCAPNG simple packet block is too short", dh);
      return false;
    }
    if (pcapng_interfaces_.empty()) {
      fail_pcapng("PCAPNG simple packet references unknown interface 0", dh);
      return false;
    }
    auto original_length = read_number<uint32_t>(block, 8, pcapng_need_swap_);
    auto const& interface = pcapng_interfaces_.front();
    auto captured_length = interface.snaplen == pcapng::unlimited_snaplen
                             ? original_length
                             : std::min(original_length, interface.snaplen);
    auto packet_end = uint64_t{pcapng::simple_packet_data_offset}
                      + pcapng::padded_size(captured_length);
    if (packet_end + sizeof(uint32_t) != block.size()) {
      fail_pcapng("PCAPNG simple packet data size does not match its block",
                  dh);
      return false;
    }
    auto data
      = block.subspan(pcapng::simple_packet_data_offset, captured_length);
    auto event = builder_.record();
    event.field("linktype").data(uint64_t{interface.linktype});
    event.field("captured_packet_length").data(uint64_t{captured_length});
    event.field("original_packet_length").data(uint64_t{original_length});
    event.field("data").data(tenzir::view<blob>{data.data(), data.size()});
    event.field("section_id").data(pcapng_section_id_);
    event.field("interface_id").data(uint64_t{0});
    return true;
  }

  auto parse_pcapng_available(Push<table_slice>& push, diagnostic_handler& dh)
    -> Task<void> {
    while (not failed_) {
      auto header = view(12);
      if (not header) {
        co_return;
      }
      auto raw_type = read_number<uint32_t>(*header, 0, false);
      auto block_need_swap = pcapng_need_swap_;
      if (raw_type == pcapng::magic_number) {
        auto raw_byte_order = read_number<uint32_t>(*header, 8, false);
        if (raw_byte_order == pcapng::byte_order_magic) {
          block_need_swap = false;
        } else if (detail::byteswap(raw_byte_order)
                   == pcapng::byte_order_magic) {
          block_need_swap = true;
        } else {
          fail_pcapng("invalid PCAPNG byte-order magic", dh);
          co_return;
        }
      } else if (not pcapng_have_section_) {
        fail_pcapng("PCAPNG file does not start with a section header", dh);
        co_return;
      }
      auto block_length = read_number<uint32_t>(*header, 4, block_need_swap);
      if (block_length < pcapng::block_min_size
          or block_length % pcapng::block_alignment != 0) {
        fail_pcapng("invalid PCAPNG block length", dh);
        co_return;
      }
      if (block_length > maximum_pcapng_block_size) {
        diagnostic::error("PCAPNG block exceeds maximum supported size")
          .note("declared {} bytes but maximum is {}", block_length,
                maximum_pcapng_block_size)
          .note("from `pcapng`")
          .emit(dh);
        failed_ = true;
        co_return;
      }
      auto block = view(block_length);
      if (not block) {
        co_return;
      }
      auto trailing_length = read_number<uint32_t>(
        *block, block->size() - sizeof(uint32_t), block_need_swap);
      if (trailing_length != block_length) {
        fail_pcapng("PCAPNG block lengths do not match", dh);
        co_return;
      }
      auto block_type = read_number<uint32_t>(*block, 0, block_need_swap);
      if (block_type == pcapng::magic_number) {
        if (not parse_pcapng_section(*block, block_need_swap, dh)) {
          co_return;
        }
      } else if (block_type == pcapng::interface_description_block) {
        if (not parse_pcapng_interface(*block, dh)) {
          co_return;
        }
      } else if (block_type == pcapng::enhanced_packet_block
                 or block_type == pcapng::packet_block) {
        if (not parse_pcapng_packet(*block, dh)) {
          co_return;
        }
        co_await flush_packets_if_full(push);
      } else if (block_type == pcapng::simple_packet_block) {
        if (not parse_pcapng_simple_packet(*block, dh)) {
          co_return;
        }
        co_await flush_packets_if_full(push);
      }
      consume(block_length);
    }
  }

  auto parse_pcap_available(Push<table_slice>& push, diagnostic_handler& dh)
    -> Task<void> {
    while (not failed_) {
      if (not have_file_header_) {
        auto bytes = view(sizeof(FileHeader));
        if (not bytes) {
          break;
        }
        auto header = parse_file_header_bytes(*bytes, dh);
        if (not header) {
          co_return;
        }
        current_file_header_ = *header;
        have_file_header_ = true;
        consume(sizeof(FileHeader));
        co_await emit_file_header(current_file_header_, push);
        continue;
      }
      if (pending_packet_header_) {
        auto bytes = view(pending_packet_header_->captured_packet_length);
        if (not bytes) {
          break;
        }
        append_packet(*pending_packet_header_, *bytes);
        pending_packet_header_ = None{};
        consume(bytes->size());
        co_await flush_packets_if_full(push);
        continue;
      }
      auto bytes = view(sizeof(PacketHeader));
      if (not bytes) {
        break;
      }
      auto header = PacketHeader{};
      std::memcpy(&header, bytes->data(), bytes->size());
      if (is_file_header(header)) {
        auto full_header = view(sizeof(FileHeader));
        if (not full_header) {
          break;
        }
        auto next_file_header = parse_file_header_bytes(*full_header, dh);
        if (not next_file_header) {
          co_return;
        }
        current_file_header_ = *next_file_header;
        consume(sizeof(FileHeader));
        co_await emit_file_header(current_file_header_, push);
        continue;
      }
      consume(sizeof(PacketHeader));
      if (need_swap_) {
        header = byteswap(header);
      }
      pending_packet_header_ = header;
    }
  }

  auto parse_available(Push<table_slice>& push, diagnostic_handler& dh)
    -> Task<void> {
    if (format_ == CaptureFormat::unknown) {
      auto magic = view(sizeof(uint32_t));
      if (not magic) {
        co_return;
      }
      auto raw_magic = read_number<uint32_t>(*magic, 0, false);
      if (raw_magic == pcapng::magic_number) {
        format_ = CaptureFormat::pcapng;
        builder_ = series_builder{make_pcapng_packet_table_slice_type()};
      } else {
        format_ = CaptureFormat::pcap;
      }
    }
    if (format_ == CaptureFormat::pcapng) {
      co_await parse_pcapng_available(push, dh);
    } else {
      co_await parse_pcap_available(push, dh);
    }
  }

  ReadPcapArgs args_;
  std::vector<std::byte> buffer_;
  size_t offset_ = 0;
  CaptureFormat format_ = CaptureFormat::unknown;
  bool failed_ = false;
  bool have_file_header_ = false;
  bool need_swap_ = false;
  uint32_t current_file_header_raw_magic_ = magic_number_2;
  FileHeader current_file_header_{};
  Option<PacketHeader> pending_packet_header_;
  bool pcapng_have_section_ = false;
  bool pcapng_need_swap_ = false;
  uint64_t pcapng_section_id_ = std::numeric_limits<uint64_t>::max();
  std::vector<PcapngInterface> pcapng_interfaces_;
  series_builder builder_;
  SeriesPusher pusher_;
};

struct WritePcapArgs {
  Option<std::string> format;
};

struct PrinterArgs {};

auto make_file_header(view3<record> row) -> FileHeader {
  auto result = FileHeader{};
  for (auto const& [key, value] : row) {
    // TODO: Make this more robust, and give a helpful error message if the
    // types are not as expected. This also applies to `to_packet_event`.
    if (key == "magic_number") {
      auto magic_number = try_as<uint64_t>(&value);
      TENZIR_ASSERT(magic_number);
      result.magic_number = detail::narrow_cast<uint32_t>(*magic_number);
      continue;
    }
    if (key == "major_version") {
      auto major_version = try_as<uint64_t>(&value);
      TENZIR_ASSERT(major_version);
      result.major_version = detail::narrow_cast<uint16_t>(*major_version);
      continue;
    }
    if (key == "minor_version") {
      auto minor_version = try_as<uint64_t>(&value);
      TENZIR_ASSERT(minor_version);
      result.minor_version = detail::narrow_cast<uint16_t>(*minor_version);
      continue;
    }
    if (key == "reserved1") {
      auto reserved1 = try_as<uint64_t>(&value);
      TENZIR_ASSERT(reserved1);
      result.reserved1 = detail::narrow_cast<uint32_t>(*reserved1);
      continue;
    }
    if (key == "reserved2") {
      auto reserved2 = try_as<uint64_t>(&value);
      TENZIR_ASSERT(reserved2);
      result.reserved2 = detail::narrow_cast<uint32_t>(*reserved2);
      continue;
    }
    if (key == "snaplen") {
      auto snaplen = try_as<uint64_t>(&value);
      TENZIR_ASSERT(snaplen);
      result.snaplen = detail::narrow_cast<uint32_t>(*snaplen);
      continue;
    }
    if (key == "linktype") {
      auto linktype = try_as<uint64_t>(&value);
      TENZIR_ASSERT(linktype);
      result.linktype = detail::narrow_cast<uint32_t>(*linktype);
      continue;
    }
    TENZIR_DEBUG("ignoring unknown PCAP file header key '{}' with value {}",
                 key, value);
  }
  return result;
}

auto make_file_headers(table_slice const& slice) -> std::vector<FileHeader> {
  if (slice.schema().name() != "pcap.file_header" or slice.rows() == 0) {
    return {};
  }
  auto result = std::vector<FileHeader>{};
  result.reserve(slice.rows());
  for (auto row : values3(slice)) {
    result.push_back(make_file_header(row));
  }
  return result;
}

/// Constructs a PCAP file header with a given link type.
auto make_file_header(uint32_t linktype) -> FileHeader {
  return {
    .magic_number = magic_number_2,
    .major_version = 2,
    .minor_version = 4,
    .reserved1 = 0,
    .reserved2 = 0,
    .snaplen = maximum_snaplen,
    .linktype = linktype,
  };
}

struct PacketEventRecord : PacketRecord {
  uint64_t declared_captured_packet_length = 0;
  uint64_t declared_original_packet_length = 0;
};

struct PacketEvent {
  PacketEventRecord packet{};
  uint64_t linktype = 0;
  Option<time> timestamp;
};

/// Extracts a packet and its timestamp from an event.
auto to_packet_event(auto row) -> PacketEvent {
  auto result = PacketEvent{};
  // NB: the API for record_view feels iffy. It should expose a field-based
  // access method, as opposed to just key-value pairs.
  for (auto const& [key, value] : row) {
    if (key == "linktype") {
      auto linktype_ptr = try_as<uint64_t>(&value);
      TENZIR_ASSERT(linktype_ptr);
      result.linktype = *linktype_ptr;
    } else if (key == "timestamp") {
      if (auto timestamp_ptr = try_as<time>(&value)) {
        result.timestamp = *timestamp_ptr;
      } else {
        TENZIR_ASSERT(is<caf::none_t>(value));
      }
    } else if (key == "captured_packet_length") {
      auto captured_packet_length = try_as<uint64_t>(&value);
      TENZIR_ASSERT(captured_packet_length);
      result.packet.declared_captured_packet_length = *captured_packet_length;
      if (*captured_packet_length <= std::numeric_limits<uint32_t>::max()) {
        result.packet.header.captured_packet_length
          = detail::narrow_cast<uint32_t>(*captured_packet_length);
      }
    } else if (key == "original_packet_length") {
      auto original_packet_length = try_as<uint64_t>(&value);
      TENZIR_ASSERT(original_packet_length);
      result.packet.declared_original_packet_length = *original_packet_length;
      if (*original_packet_length <= std::numeric_limits<uint32_t>::max()) {
        result.packet.header.original_packet_length
          = detail::narrow_cast<uint32_t>(*original_packet_length);
      }
    } else if (key == "data") {
      if (auto str_data = try_as<view3<std::string>>(&value)) {
        // TODO: Remove this fallback eventually.
        result.packet.data = std::span<std::byte const>{
          reinterpret_cast<std::byte const*>(str_data->data()),
          str_data->size()};
      } else {
        auto data = try_as<view3<blob>>(&value);
        TENZIR_ASSERT(data);
        result.packet.data = *data;
      }
    } else if (key != "section_id" and key != "interface_id") {
      TENZIR_WARN("got invalid PCAP header field '{}'", key);
    }
  }
  return result;
}

auto validate_packet_length_ranges(PacketEventRecord const& packet)
  -> Option<diagnostic> {
  if (packet.declared_captured_packet_length
      > std::numeric_limits<uint32_t>::max()) {
    return diagnostic::error("packet captured length is out of range")
      .note("got {} but maximum is {}", packet.declared_captured_packet_length,
            std::numeric_limits<uint32_t>::max())
      .done();
  }
  if (packet.declared_original_packet_length
      > std::numeric_limits<uint32_t>::max()) {
    return diagnostic::error("packet original length is out of range")
      .note("got {} but maximum is {}", packet.declared_original_packet_length,
            std::numeric_limits<uint32_t>::max())
      .done();
  }
  return None{};
}

auto validate_packet(PacketRecord const& packet) -> Option<diagnostic> {
  if (packet.header.captured_packet_length != packet.data.size()) {
    return diagnostic::error("packet captured length does not match data size")
      .note("declared {} bytes but got {}",
            packet.header.captured_packet_length, packet.data.size())
      .done();
  }
  if (packet.header.captured_packet_length
      > packet.header.original_packet_length) {
    return diagnostic::error("packet captured length exceeds original length")
      .note("captured {} bytes from a {}-byte packet",
            packet.header.captured_packet_length,
            packet.header.original_packet_length)
      .done();
  }
  return None{};
}

/// Sets a classic PCAP timestamp with nanosecond resolution.
auto set_pcap_timestamp(PacketRecord& packet, time timestamp) -> void {
  auto ns = timestamp.time_since_epoch();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
  auto fraction = ns - secs;
  packet.header.timestamp = detail::narrow_cast<uint32_t>(secs.count());
  packet.header.timestamp_fraction
    = detail::narrow_cast<uint32_t>(fraction.count());
}

class WritePcap final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WritePcap(WritePcapArgs args) : args_{std::move(args)} {
    if (args_.format) {
      if (*args_.format == "pcap") {
        format_ = CaptureFormat::pcap;
      } else if (*args_.format == "pcapng") {
        format_ = CaptureFormat::pcapng;
      }
    }
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    if (format_ == CaptureFormat::unknown) {
      if (input.schema().name() == "pcap.file_header") {
        format_ = CaptureFormat::pcap;
      } else if (input.schema().name() == "pcap.packet") {
        auto const& record = as<record_type>(input.schema());
        format_ = record.resolve_key("section_id")
                      and record.resolve_key("interface_id")
                    ? CaptureFormat::pcapng
                    : CaptureFormat::pcap;
      } else if (input.schema().name() == "tenzir.packet") {
        auto const& record = as<record_type>(input.schema());
        if (auto pcap = record.field("pcap")) {
          if (auto const* nested = try_as<record_type>(&*pcap)) {
            format_ = nested->resolve_key("section_id")
                          and nested->resolve_key("interface_id")
                        ? CaptureFormat::pcapng
                        : CaptureFormat::pcap;
          }
        }
      }
    }
    if (format_ == CaptureFormat::pcapng) {
      co_await process_pcapng(std::move(input), push, ctx);
      co_return;
    }
    if (input.schema().name() == "pcap.file_header") {
      TENZIR_DEBUG("got new PCAP file header");
      auto headers = make_file_headers(input);
      if (headers.empty()) {
        diagnostic::warning("failed to parse PCAP file header").emit(ctx.dh());
        co_return;
      }
      auto buffer = std::vector<std::byte>{};
      buffer.reserve(headers.size() * sizeof(FileHeader));
      for (auto const& header : headers) {
        if (not normalized_magic_number(header.magic_number)) {
          diagnostic::warning("failed to parse PCAP file header")
            .note("invalid magic number")
            .emit(ctx.dh());
          continue;
        }
        current_file_header_ = header;
        auto serialized_header = serialize_file_header(*current_file_header_);
        auto bytes = as_bytes(serialized_header);
        buffer.insert(buffer.end(), bytes.begin(), bytes.end());
      }
      if (not buffer.empty()) {
        co_await push(chunk::make(std::move(buffer), metadata_));
      }
      co_return;
    }
    auto buffer = std::vector<std::byte>{};
    auto emit_file_header = false;
    auto process_packet_row = [&](auto row) -> bool {
      auto [packet, linktype, timestamp] = to_packet_event(row);
      if (not timestamp) {
        diagnostic::error("packet timestamp is missing").emit(ctx.dh());
        failed_ = true;
        return false;
      }
      if (auto error = validate_packet_length_ranges(packet)) {
        ctx.dh().emit(std::move(*error));
        failed_ = true;
        return false;
      }
      if (auto error = validate_packet(packet)) {
        ctx.dh().emit(std::move(*error));
        failed_ = true;
        return false;
      }
      if (linktype > std::numeric_limits<uint32_t>::max()) {
        diagnostic::error("PCAP link type {} is out of range", linktype)
          .emit(ctx.dh());
        failed_ = true;
        return false;
      }
      auto output_linktype = detail::narrow_cast<uint32_t>(linktype);
      set_pcap_timestamp(packet, *timestamp);
      if (not current_file_header_) {
        TENZIR_DEBUG("generating PCAP file header");
        current_file_header_ = make_file_header(output_linktype);
        emit_file_header = true;
      } else if (output_linktype != current_file_header_->linktype) {
        diagnostic::error("packet linktype doesn't match file header")
          .emit(ctx.dh());
        failed_ = true;
        return false;
      } else if (uses_microsecond_precision(
                   current_file_header_->magic_number)) {
        packet.header.timestamp_fraction /= 1'000;
      }
      auto serialized_packet_header = serialize_packet_header(
        packet.header, current_file_header_->magic_number);
      auto header = as_bytes(serialized_packet_header);
      buffer.reserve(buffer.size() + sizeof(PacketHeader) + packet.data.size());
      buffer.insert(buffer.end(), header.begin(), header.end());
      buffer.insert(buffer.end(), packet.data.begin(), packet.data.end());
      return true;
    };
    auto const& input_record = as<record_type>(input.schema());
    if (input.schema().name() == "pcap.packet") {
      auto resolved_slice = resolve_enumerations(input);
      for (auto row : values3(resolved_slice)) {
        if (not process_packet_row(row)) {
          co_return;
        }
      }
    } else if (input.schema().name() == "tenzir.packet") {
      auto const pcap_index = input_record.resolve_key("pcap");
      if (not pcap_index) {
        TENZIR_VERBOSE("ignoring tenzir.packet events without pcap field");
        co_return;
      }
      auto [pcap_type, pcap_array] = pcap_index->get(input);
      auto const* pcap_record_type = try_as<record_type>(&pcap_type);
      auto const* pcap_values = try_as<arrow::StructArray>(&*pcap_array);
      if (not(pcap_record_type and pcap_values)) {
        diagnostic::warning("got a malformed 'tenzir.packet' event")
          .note("field 'pcap' not a record")
          .emit(ctx.dh());
        co_return;
      }
      for (auto row : values3(*pcap_values)) {
        if (not row) {
          continue;
        }
        if (not process_packet_row(*row)) {
          co_return;
        }
      }
    } else {
      diagnostic::warning("received unprocessable schema")
        .note("cannot handle", input.schema().name())
        .emit(ctx.dh());
      co_return;
    }
    if (buffer.empty()) {
      co_return;
    }
    if (emit_file_header) {
      TENZIR_DEBUG("emitting generated PCAP file header");
      TENZIR_ASSERT(current_file_header_);
      auto serialized_header = serialize_file_header(*current_file_header_);
      co_await push(chunk::copy(as_bytes(serialized_header), metadata_));
    }
    co_await push(chunk::make(std::move(buffer), metadata_));
  }

  auto snapshot(Serde& serde) -> void override {
    auto format = static_cast<uint8_t>(format_);
    serde("format", format);
    TENZIR_ASSERT(format <= static_cast<uint8_t>(CaptureFormat::pcapng));
    format_ = static_cast<CaptureFormat>(format);
    auto has_current_file_header = current_file_header_.has_value();
    serde("has_current_file_header", has_current_file_header);
    if (serde.is_loading()) {
      current_file_header_
        = has_current_file_header ? Option{FileHeader{}} : None{};
    }
    if (current_file_header_) {
      snapshot_file_header(serde, *current_file_header_);
    }
    serde("pcapng_interface_ids", pcapng_interface_ids_);
    serde("pcapng_section_emitted", pcapng_section_emitted_);
    serde("failed", failed_);
  }

private:
  auto flush_pcapng_output(std::vector<std::byte>& buffer,
                           Push<chunk_ptr>& push) -> Task<void> {
    if (buffer.empty()) {
      co_return;
    }
    co_await push(chunk::make(std::exchange(buffer, {}), pcapng_metadata_));
    pcapng_section_emitted_ = true;
  }

  auto process_pcapng(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> {
    if (input.schema().name() == "pcap.file_header") {
      diagnostic::warning("ignoring classic PCAP file header for PCAPNG output")
        .emit(ctx.dh());
      co_return;
    }
    auto buffer = std::vector<std::byte>{};
    if (not pcapng_section_emitted_) {
      append_pcapng_section_header(buffer);
    }
    auto process_packet_row = [&](auto row) -> bool {
      auto [packet, linktype, timestamp] = to_packet_event(row);
      if (not timestamp) {
        diagnostic::error("packet timestamp is missing").emit(ctx.dh());
        failed_ = true;
        return false;
      }
      if (auto error = validate_packet_length_ranges(packet)) {
        ctx.dh().emit(std::move(*error));
        failed_ = true;
        return false;
      }
      auto block_size
        = pcapng_packet_block_size(packet.header.captured_packet_length);
      if (block_size > maximum_pcapng_block_size) {
        diagnostic::error("PCAPNG packet exceeds maximum block size")
          .note("requires {} bytes but maximum is {}", block_size,
                maximum_pcapng_block_size)
          .emit(ctx.dh());
        failed_ = true;
        return false;
      }
      if (auto error = validate_packet(packet)) {
        ctx.dh().emit(std::move(*error));
        failed_ = true;
        return false;
      }
      if (linktype > std::numeric_limits<uint16_t>::max()) {
        diagnostic::error("PCAPNG link type {} is out of range", linktype)
          .emit(ctx.dh());
        failed_ = true;
        return false;
      }
      auto output_linktype = detail::narrow_cast<uint16_t>(linktype);
      auto it = pcapng_interface_ids_.find(output_linktype);
      if (it == pcapng_interface_ids_.end()) {
        auto output_id = detail::narrow<uint32_t>(pcapng_interface_ids_.size());
        append_pcapng_interface(buffer, output_linktype);
        it = pcapng_interface_ids_.emplace(output_linktype, output_id).first;
      }
      auto raw_timestamp = pcapng::encode_timestamp(
        *timestamp, {.resolution = pcapng::nanosecond_timestamp_resolution});
      if (not raw_timestamp) {
        diagnostic::error("PCAPNG packet timestamp is out of range")
          .emit(ctx.dh());
        failed_ = true;
        return false;
      }
      if (not append_pcapng_packet(buffer, it->second, packet,
                                   *raw_timestamp)) {
        diagnostic::error("failed to serialize PCAPNG packet").emit(ctx.dh());
        failed_ = true;
        return false;
      }
      return true;
    };
    auto const& input_record = as<record_type>(input.schema());
    if (input.schema().name() == "pcap.packet") {
      auto resolved_slice = resolve_enumerations(input);
      for (auto row : values3(resolved_slice)) {
        if (not process_packet_row(row)) {
          co_return;
        }
        if (buffer.size() >= pcapng_output_flush_size) {
          co_await flush_pcapng_output(buffer, push);
        }
      }
    } else if (input.schema().name() == "tenzir.packet") {
      auto pcap_index = input_record.resolve_key("pcap");
      if (not pcap_index) {
        TENZIR_VERBOSE("ignoring tenzir.packet events without pcap field");
        co_return;
      }
      auto [pcap_type, pcap_array] = pcap_index->get(input);
      auto const* pcap_record_type = try_as<record_type>(&pcap_type);
      auto const* pcap_values = try_as<arrow::StructArray>(&*pcap_array);
      if (not(pcap_record_type and pcap_values)) {
        diagnostic::warning("got a malformed 'tenzir.packet' event")
          .note("field 'pcap' not a record")
          .emit(ctx.dh());
        co_return;
      }
      for (auto row : values3(*pcap_values)) {
        if (not row) {
          continue;
        }
        if (not process_packet_row(*row)) {
          co_return;
        }
        if (buffer.size() >= pcapng_output_flush_size) {
          co_await flush_pcapng_output(buffer, push);
        }
      }
    } else {
      diagnostic::warning("received unprocessable schema")
        .note("cannot handle", input.schema().name())
        .emit(ctx.dh());
      co_return;
    }
    co_await flush_pcapng_output(buffer, push);
  }

  WritePcapArgs args_;
  chunk_metadata metadata_{.content_type = std::string{pcap::content_type}};
  chunk_metadata pcapng_metadata_{.content_type = "application/x-pcapng"};
  CaptureFormat format_ = CaptureFormat::unknown;
  Option<FileHeader> current_file_header_;
  detail::flat_map<uint16_t, uint32_t> pcapng_interface_ids_;
  bool pcapng_section_emitted_ = false;
  bool failed_ = false;
};

class PcapPrinter final : public plugin_printer {
public:
  PcapPrinter() = default;

  explicit PcapPrinter(PrinterArgs args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "pcap";
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    // When the printer receives table slices, it can be a wild mix of file
    // headers and packet records. We may receive an ordered event stream
    // beginning with a file header, but we may also receive a random sequence
    // of packet events coming from a historical query.
    auto meta = chunk_metadata{.content_type = std::string{pcap::content_type}};
    return printer_instance::make(
      [&ctrl, input_schema = std::move(input_schema),
       current_file_header = Option<FileHeader>{}, file_header_printed = false,
       buffer = std::vector<std::byte>{}, meta = std::move(meta)](
        table_slice slice) mutable -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        // We may receive multiple file headers. If we receive any, we take
        // it into consideration for timestamp resolution.
        if (slice.schema().name() == "pcap.file_header") {
          TENZIR_DEBUG("got new PCAP file header");
          auto headers = make_file_headers(slice);
          if (headers.empty()) {
            diagnostic::warning("failed to parse PCAP file header")
              .emit(ctrl.diagnostics());
            co_yield {};
            co_return;
          }
          auto header_buffer = std::vector<std::byte>{};
          header_buffer.reserve(headers.size() * sizeof(FileHeader));
          for (auto const& header : headers) {
            if (not normalized_magic_number(header.magic_number)) {
              diagnostic::warning("failed to parse PCAP file header")
                .note("invalid magic number")
                .emit(ctrl.diagnostics());
              continue;
            }
            current_file_header = header;
            auto serialized_header
              = serialize_file_header(*current_file_header);
            auto bytes = as_bytes(serialized_header);
            header_buffer.insert(header_buffer.end(), bytes.begin(),
                                 bytes.end());
          }
          if (header_buffer.empty()) {
            co_yield {};
            co_return;
          }
          file_header_printed = true;
          co_yield chunk::make(std::move(header_buffer), meta);
          co_return;
        }
        // Helper function to process a row in a table slice of packets.
        auto process_packet_row = [&](auto row) -> Option<diagnostic> {
          auto [packet, linktype, timestamp] = to_packet_event(row);
          if (not timestamp) {
            return diagnostic::error("packet timestamp is missing").done();
          }
          if (auto error = validate_packet(packet)) {
            return error;
          }
          set_pcap_timestamp(packet, *timestamp);
          // Generate file header based on first packet or fail if the packet
          // is incompatible with the known file header.
          if (not current_file_header) {
            TENZIR_DEBUG("generating PCAP file header");
            current_file_header = make_file_header(linktype);
          } else if (linktype != current_file_header->linktype) {
            return diagnostic::error(
                     "packet linktype doesn't match file header")
              .done();
          } else if (uses_microsecond_precision(
                       current_file_header->magic_number)) {
            packet.header.timestamp_fraction /= 1'000;
          }
          auto serialized_packet_header = serialize_packet_header(
            packet.header, current_file_header->magic_number);
          auto bytes = as_bytes(serialized_packet_header);
          buffer.reserve(sizeof(PacketHeader) + packet.data.size());
          buffer.insert(buffer.end(), bytes.begin(), bytes.end());
          buffer.insert(buffer.end(), packet.data.begin(), packet.data.end());
          return {};
        };
        // Extract PCAP data from input.
        auto const& input_record = as<record_type>(slice.schema());
        if (slice.schema().name() == "pcap.packet") {
          auto resolved_slice = resolve_enumerations(slice);
          for (auto row : values3(resolved_slice)) {
            if (auto diag = process_packet_row(row)) {
              ctrl.diagnostics().emit(std::move(*diag));
              co_return;
            }
          }
        } else if (slice.schema().name() == "tenzir.packet") {
          auto const pcap_index = input_record.resolve_key("pcap");
          if (not pcap_index) {
            TENZIR_VERBOSE("ignoring tenzir.packet events without pcap field");
            co_yield {};
            co_return;
          }
          auto [pcap_type, pcap_array] = pcap_index->get(slice);
          auto const* pcap_record_type = try_as<record_type>(&pcap_type);
          auto const* pcap_values = try_as<arrow::StructArray>(&*pcap_array);
          if (not(pcap_record_type and pcap_values)) {
            diagnostic::warning("got a malformed 'tenzir.packet' event")
              .note("field 'pcap' not a record")
              .emit(ctrl.diagnostics());
            co_yield {};
            co_return;
          }
          for (auto row : values3(*pcap_values)) {
            if (not row) {
              continue;
            }
            if (auto diag = process_packet_row(*row)) {
              ctrl.diagnostics().emit(std::move(*diag));
              co_return;
            }
          }
        } else {
          diagnostic::warning("received unprocessable schema")
            .note("cannot handle", slice.schema().name())
            .emit(ctrl.diagnostics());
          co_yield {};
          co_return;
        }
        if (not file_header_printed) {
          TENZIR_DEBUG("emitting PCAP file header");
          TENZIR_ASSERT(current_file_header);
          auto serialized_header = serialize_file_header(*current_file_header);
          co_yield chunk::copy(as_bytes(serialized_header), meta);
          file_header_printed = true;
        }
        co_yield chunk::copy(buffer, meta);
        buffer.clear();
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  }

  auto prints_utf8() const -> bool override {
    return false;
  }

  friend auto inspect(auto& f, PcapPrinter& x) -> bool {
    return f.object(x)
      .pretty_name("pcap_printer")
      .fields(f.field("args", x.args_));
  }

private:
  PrinterArgs args_;
};

class Plugin final : public virtual parser_plugin<PcapParser>,
                     public virtual printer_plugin<PcapPrinter> {
public:
  auto initialize(record const& config, record const& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://tenzir.com/docs/formats/{}", name())};
    auto args = ParserArgs{};
    parser.add("-e,--emit-file-headers", args.emit_file_headers);
    parser.parse(p);
    return std::make_unique<PcapParser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{
      name(), fmt::format("https://tenzir.com/docs/formats/{}", name())};
    auto args = PrinterArgs{};
    parser.parse(p);
    return std::make_unique<PcapPrinter>(std::move(args));
  }

  auto name() const -> std::string override {
    return "pcap";
  }

private:
  record config_;
};

class ReadPlugin final
  : public virtual operator_plugin2<parser_adapter<PcapParser>>,
    public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_pcap";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = ParserArgs{};
    TRY(argument_parser2::operator_(name())
          .named("emit_file_headers", args.emit_file_headers)
          .parse(inv, ctx));
    return std::make_unique<parser_adapter<PcapParser>>(
      PcapParser{std::move(args)});
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadPcapArgs, ReadPcap>{};
    d.named("emit_file_headers", &ReadPcapArgs::emit_file_headers);
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"pcap", "pcapng"},
      .mime_types = {"application/vnd.tcpdump.pcap", "application/x-pcapng"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    auto detect = [](read_detection_input input) {
      auto raw_magic = uint32_t{};
      if (input.bytes.size() < sizeof(raw_magic)) {
        return input.eof ? read_detection::reject()
                         : read_detection::need_more();
      }
      std::memcpy(&raw_magic, input.bytes.data(), sizeof(raw_magic));
      if (normalized_magic_number(raw_magic)
          or raw_magic == pcapng::magic_number) {
        return read_detection::match();
      }
      return read_detection::reject();
    };
    return {
      read_detection::candidate("read_pcap", read_detection::specificity::magic,
                                detect),
    };
  }
};

class WritePlugin final
  : public virtual operator_plugin2<writer_adapter<PcapPrinter>>,
    public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_pcap";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<writer_adapter<PcapPrinter>>(PcapPrinter{});
  }

  auto describe() const -> Description override {
    auto d = Describer<WritePcapArgs, WritePcap>{};
    auto format = d.named("format", &WritePcapArgs::format);
    d.validate([format](DescribeCtx& ctx) -> Empty {
      auto value = ctx.get(format).value_or("auto");
      if (value != "auto" and value != "pcap" and value != "pcapng") {
        diagnostic::error("`format` must be one of `auto`, `pcap`, or `pcapng`")
          .primary(ctx.get_location(format).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {"pcap"}};
  }
};

} // namespace

} // namespace tenzir::plugins::pcap

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::ReadPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::WritePlugin)
