//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/detail/byteswap.hpp>
#include <tenzir/die.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <arrow/record_batch.h>

namespace tenzir::plugins::pcap {

namespace {

using namespace tenzir::pcap;

/// Returns a stateful function that retrieves a given number of bytes in a
/// contiguous buffer from a generator of chunks. The last span is underful,
/// i.e., smaller than the number of bytes requested, and zero-sized if the
/// input boundaries are aligned. The function returns nullopt whenever it
/// merges buffers from multiple chunks. This does not indicate completion.
auto make_byte_reader(generator<chunk_ptr> input) {
  input.begin(); // prime the pump
  return
    [input = std::move(input), chunk = chunk_ptr{}, chunk_offset = size_t{0},
     buffer = std::vector<std::byte>{}, buffer_offset = size_t{0}](
      size_t num_bytes) mutable -> std::optional<std::span<const std::byte>> {
      // The internal chunk is not available when we first enter this function
      // and as well when we have no more chunks (at the end).
      if (!chunk) {
        TENZIR_ASSERT(chunk_offset == 0);
        // Can we fulfill our request from the buffer?
        if (buffer.size() - buffer_offset >= num_bytes) {
          auto result = as_bytes(buffer).subspan(buffer_offset, num_bytes);
          buffer_offset += num_bytes;
          return result;
        }
        // Can we get more chunks?
        auto current = input.unsafe_current();
        if (current == input.end()) {
          // We're done and return an underful chunk.
          auto result = as_bytes(buffer).subspan(buffer_offset);
          buffer_offset = buffer.size();
          TENZIR_ASSERT(result.size() < num_bytes);
          return result;
        }
        chunk = std::move(*current);
        ++current;
        if (!chunk)
          return std::nullopt;
      }
      // We have a chunk.
      TENZIR_ASSERT(chunk != nullptr);
      if (buffer.size() == buffer_offset) {
        // Have consumed the entire chunk last time? Then reset and try again.
        if (chunk_offset == chunk->size()) {
          chunk_offset = 0;
          chunk = nullptr;
          return std::nullopt;
        }
        TENZIR_ASSERT(chunk_offset < chunk->size());
        // If we have a chunk, but not enough bytes, then we must buffer.
        if (chunk->size() - chunk_offset < num_bytes) {
          buffer = {chunk->begin() + chunk_offset, chunk->end()};
          buffer_offset = 0;
          chunk = nullptr;
          chunk_offset = 0;
          return std::nullopt;
        }
        // Enough in the chunk, simply yield from it.
        auto result = as_bytes(*chunk).subspan(chunk_offset, num_bytes);
        chunk_offset += num_bytes;
        return result;
      }
      // If we need to process both a buffer and chunk, we copy over the chunk
      // remainder into the buffer.
      buffer.erase(buffer.begin(), buffer.begin() + buffer_offset);
      buffer_offset = 0;
      buffer.reserve(buffer.size() + chunk->size() - chunk_offset);
      buffer.insert(buffer.end(), chunk->begin() + chunk_offset, chunk->end());
      chunk = nullptr;
      chunk_offset = 0;
      if (buffer.size() >= num_bytes) {
        auto result = as_bytes(buffer).subspan(0, num_bytes);
        buffer_offset = num_bytes;
        return result;
      }
      return std::nullopt;
    };
}

auto make_file_header_table_slice(const file_header& header) -> table_slice {
  auto builder = table_slice_builder{file_header_type()};
  auto okay = builder.add(header.magic_number)
              && builder.add(header.major_version)
              && builder.add(header.minor_version)
              && builder.add(header.reserved1) && builder.add(header.reserved2)
              && builder.add(header.snaplen) && builder.add(header.linktype);
  TENZIR_ASSERT(okay);
  return builder.finish();
}

struct parser_args {
  std::optional<location> emit_file_header;

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("emit-file-header", x.emit_file_header));
  }
};

class pcap_parser final : public plugin_parser {
public:
  pcap_parser() = default;

  explicit pcap_parser(parser_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "pcap";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto make = [](auto& ctrl, generator<chunk_ptr> input,
                   bool emit_file_header) -> generator<table_slice> {
      // A PCAP file starts with a 24-byte header.
      auto input_file_header = file_header{};
      auto read_n = make_byte_reader(std::move(input));
      co_yield {};
      while (true) {
        auto length = sizeof(file_header);
        auto bytes = read_n(length);
        if (!bytes) {
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
      auto need_swap = need_byte_swap(input_file_header.magic_number);
      if (!need_swap) {
        diagnostic::error("invalid PCAP magic number: {0:x}",
                          uint32_t{input_file_header.magic_number})
          .note("from `pcap`")
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (*need_swap) {
        TENZIR_VERBOSE("detected different byte order in file and host");
        input_file_header = byteswap(input_file_header);
      } else {
        TENZIR_DEBUG("detected identical byte order in file and host");
      }
      if (emit_file_header)
        co_yield make_file_header_table_slice(input_file_header);
      // After the header, the remainder of the file are typically Packet
      // Records, consisting of a 16-byte header and variable-length payload.
      // However, our parser is a bit smarter and also supports concatenated
      // PCAP traces.
      auto builder = table_slice_builder{packet_record_type()};
      auto num_packets = size_t{0};
      auto last_finish = std::chrono::steady_clock::now();
      while (true) {
        const auto now = std::chrono::steady_clock::now();
        if (builder.rows() >= defaults::import::table_slice_size
            or last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish();
        }
        packet_record packet;
        // We first try to parse a packet header first.
        while (true) {
          TENZIR_DEBUG("reading packet header");
          auto length = sizeof(packet_header);
          auto bytes = read_n(length);
          if (!bytes) {
            if (last_finish != now) {
              co_yield {};
            }
            continue;
          }
          if (bytes->empty()) {
            TENZIR_DEBUG("completed trace of {} packets", num_packets);
            if (builder.rows() > 0) {
              co_yield builder.finish();
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
          std::memcpy(&packet.header, bytes->data(), sizeof(packet_header));
          if (is_file_header(packet.header)) {
            TENZIR_DEBUG("detected new PCAP file header");
            std::memcpy(&input_file_header, &packet.header,
                        sizeof(packet_header));
            // Read the remaining two fields of the packet header.
            while (true) {
              constexpr auto length
                = sizeof(file_header::snaplen) + sizeof(file_header::linktype);
              auto bytes = read_n(length);
              if (!bytes) {
                co_yield {};
                continue;
              }
              if (bytes->size() != length) {
                diagnostic::error("failed to read remaining PCAP file header")
                  .hint("got {} bytes but needed {}", bytes->size(), length)
                  .emit(ctrl.diagnostics());
                co_return;
              }
              TENZIR_ASSERT(sizeof(file_header) - sizeof(packet_header)
                            == bytes->size());
              std::memcpy(&input_file_header + sizeof(packet_header),
                          bytes->data(), bytes->size());
              break;
            }
            need_swap = need_byte_swap(input_file_header.magic_number);
            TENZIR_ASSERT(need_swap); // checked in is_file_header
            if (*need_swap) {
              TENZIR_VERBOSE("detected different byte order in file and host");
              input_file_header = byteswap(input_file_header);
            } else {
              TENZIR_DEBUG("detected identical byte order in file and host");
            }
            if (emit_file_header)
              co_yield make_file_header_table_slice(input_file_header);
            //  Jump back to the while loop that reads pairs of packet header
            //  and packet data.
            continue;
          }
          // Okay, we got a packet header, let's proceed.
          if (*need_swap)
            packet.header = byteswap(packet.header);
          break;
        }
        // Read the packet.
        while (true) {
          TENZIR_DEBUG("reading packet data of size {}",
                       packet.header.captured_packet_length);
          auto length = packet.header.captured_packet_length;
          auto bytes = read_n(length);
          if (!bytes) {
            if (last_finish != now) {
              co_yield {};
            }
            continue;
          }
          if (bytes->size() != length) {
            co_yield builder.finish();
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
          die("invalid magic number"); // validated earlier
        }
        const auto* ptr = reinterpret_cast<const char*>(packet.data.data());
        auto data = std::string_view{ptr, packet.data.size()};
        if (!(builder.add(input_file_header.linktype & 0x0000FFFF)
              && builder.add(timestamp)
              && builder.add(packet.header.captured_packet_length)
              && builder.add(packet.header.original_packet_length)
              && builder.add(data))) {
          diagnostic::error("failed to add packet #{}", num_packets)
            .note("from `pcap`")
            .emit(ctrl.diagnostics());
          co_return;
        }
      }
      if (builder.rows() > 0) {
        co_yield builder.finish();
      }
    };
    return make(ctrl, std::move(input), !!args_.emit_file_header);
  }

  friend auto inspect(auto& f, pcap_parser& x) -> bool {
    return f.object(x)
      .pretty_name("pcap_parser")
      .fields(f.field("args", x.args_));
  }

private:
  parser_args args_;
};

struct printer_args {
  // template <class Inspector>
  // friend auto inspect(Inspector& f, printer_args& x) -> bool {
  //   return f.object(x).pretty_name("printer_args");
  // }
};

class pcap_printer final : public plugin_printer {
public:
  pcap_printer() = default;

  explicit pcap_printer(printer_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "pcap";
  }

  static auto pack(const file_header& header) -> chunk_ptr {
    return chunk::copy(as_bytes(header));
  }

  static auto make_file_header(const table_slice& slice)
    -> std::optional<file_header> {
    if (slice.schema().name() != "pcap.file_header" || slice.rows() == 0)
      return std::nullopt;
    auto result = file_header{};
    const auto& input_record = caf::get<record_type>(slice.schema());
    auto array = to_record_batch(slice)->ToStructArray().ValueOrDie();
    auto xs = values(input_record, *array);
    auto begin = xs.begin();
    if (begin == xs.end() || *begin == std::nullopt)
      return std::nullopt;
    for (const auto& [key, value] : **begin) {
      if (key == "magic_number")
        result.magic_number
          = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
      if (key == "major_version")
        result.major_version
          = detail::narrow_cast<uint16_t>(caf::get<uint64_t>(value));
      if (key == "minor_version")
        result.minor_version
          = detail::narrow_cast<uint16_t>(caf::get<uint64_t>(value));
      if (key == "reserved1")
        result.reserved1
          = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
      if (key == "reserved2")
        result.reserved2
          = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
      if (key == "snaplen")
        result.snaplen
          = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
      if (key == "linktype")
        result.linktype
          = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
    }
    return result;
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return printer_instance::make(
      [&ctrl, input_schema = std::move(input_schema),
       output_file_header = std::optional<file_header>{}, need_swap = false,
       file_header_printed = false, buffer = std::vector<std::byte>{}](
        table_slice slice) mutable -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        // We may receive a PCAP file header as first event. The header magic
        // tells us how we should write timestamps and if we need to byte-swap
        // packet headers.
        if (slice.schema().name() == "pcap.file_header") {
          TENZIR_DEBUG("got external PCAP file header");
          if (output_file_header) {
            diagnostic::warning("ignoring external PCAP file header")
              .note("cannot re-emit file header after having emitted packets")
              .note("from `pcap`")
              .emit(ctrl.diagnostics());
          } else if (auto header = make_file_header(slice)) {
            output_file_header = *header;
          } else {
            diagnostic::error("failed to parse external PCAP file header")
              .note("from `pcap`")
              .emit(ctrl.diagnostics());
          }
          co_yield {};
          co_return;
        } else if (slice.schema().name() != "pcap.packet") {
          diagnostic::warning("received invalid schema")
            .note("got '{}' but expected pcap.packet", slice.schema().name())
            .note("from `pcap`")
            .emit(ctrl.diagnostics());
          co_yield {};
          co_return;
        }
        // Iterate row-wise.
        const auto& input_record = caf::get<record_type>(slice.schema());
        auto resolved_slice = resolve_enumerations(slice);
        auto array
          = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
        for (const auto& row : values(input_record, *array)) {
          TENZIR_ASSERT_CHEAP(row);
          // NB: the API for record_view is just wrong. It should expose a
          // field-based access method, as opposed to just key-value pairs.
          auto timestamp = time{};
          auto captured_packet_length = uint64_t{0};
          auto original_packet_length = uint64_t{0};
          auto data = std::string_view{};
          auto linktype = uint32_t{0};
          for (const auto& [key, value] : *row) {
            if (key == "linktype")
              linktype
                = detail::narrow_cast<uint32_t>(caf::get<uint64_t>(value));
            else if (key == "timestamp")
              timestamp = caf::get<time>(value);
            else if (key == "captured_packet_length")
              captured_packet_length = caf::get<uint64_t>(value);
            else if (key == "original_packet_length")
              original_packet_length = caf::get<uint64_t>(value);
            else if (key == "data")
              data = caf::get<std::string_view>(value);
            else
              diagnostic::error("got invalid PCAP header field ''", key)
                .note("from `pcap`")
                .emit(ctrl.diagnostics());
          }
          // Print the file header once.
          if (!file_header_printed) {
            if (output_file_header) {
              TENZIR_DEBUG("using provided PCAP file header");
              auto swap = need_byte_swap(output_file_header->magic_number);
              if (!swap) {
                diagnostic::error("got invalid PCAP magic number: {0:x}",
                                  uint32_t{output_file_header->magic_number})
                  .note("from `pcap`")
                  .emit(ctrl.diagnostics());
                co_return;
              }
              need_swap = *swap;
              if (need_swap)
                co_yield pack(byteswap(*output_file_header));
              else
                co_yield pack(*output_file_header);
            } else {
              TENZIR_DEBUG("generating a PCAP file header");
              auto header = file_header{
                .magic_number = magic_number_2,
                .major_version = 2,
                .minor_version = 4,
                .reserved1 = 0,
                .reserved2 = 0,
                .snaplen = maximum_snaplen,
                .linktype = linktype,
              };
              co_yield pack(header);
            }
            file_header_printed = true;
          } else if (linktype != output_file_header->linktype) {
            diagnostic::error("packet with new linktype {}, first was {}",
                              linktype, uint32_t{output_file_header->linktype})
              .note("from `pcap`")
              .emit(ctrl.diagnostics());
            co_return;
          }
          // Split timestamp in two pieces.
          auto ns = timestamp.time_since_epoch();
          auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
          auto fraction = ns - secs;
          auto timestamp_fraction
            = detail::narrow_cast<uint32_t>(fraction.count());
          if (output_file_header->magic_number == magic_number_1)
            timestamp_fraction /= 1'000;
          auto header = packet_header{
            .timestamp = detail::narrow_cast<uint32_t>(secs.count()),
            .timestamp_fraction = timestamp_fraction,
            .captured_packet_length
            = detail::narrow_cast<uint32_t>(captured_packet_length),
            .original_packet_length
            = detail::narrow_cast<uint32_t>(original_packet_length),
          };
          if (need_swap)
            header = byteswap(header);
          // Copy over header.
          buffer.resize(sizeof(packet_header) + data.size());
          std::memcpy(buffer.data(), &header, sizeof(header));
          std::memcpy(buffer.data() + sizeof(packet_header), data.data(),
                      data.size());
          co_yield chunk::copy(buffer);
        }
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, pcap_printer& x) -> bool {
    return f.object(x)
      .pretty_name("pcap_printer")
      .fields(f.field("args", x.args_));
  }

private:
  printer_args args_;
};

class plugin final : public virtual parser_plugin<pcap_parser>,
                     public virtual printer_plugin<pcap_printer> {
public:
  auto initialize(const record& config, const record& /* global_config */)
    -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/formats/{}", name())};
    auto args = parser_args{};
    parser.add("-e,--emit-file-header", args.emit_file_header);
    parser.parse(p);
    return std::make_unique<pcap_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{
      name(),
      fmt::format("https://docs.tenzir.com/docs/next/formats/{}", name())};
    auto args = printer_args{};
    parser.parse(p);
    return std::make_unique<pcap_printer>(std::move(args));
  }

  auto name() const -> std::string override {
    return "pcap";
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::pcap

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::plugin)
