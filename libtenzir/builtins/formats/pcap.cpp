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
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/pcap.hpp>
#include <tenzir/pcapng.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>
#include <tenzir/view.hpp>

#include <arrow/record_batch.h>

namespace tenzir::plugins::pcap {

namespace {

using namespace tenzir::pcap;

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
  std::optional<location> emit_file_headers;

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("emit_file_headers", x.emit_file_headers));
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
                   bool emit_file_headers) -> generator<table_slice> {
      // A PCAP file starts with a 24-byte header.
      auto input_file_header = file_header{};
      auto read_n = make_byte_view_reader(std::move(input));
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
      if (input_file_header.magic_number == pcapng::magic_number) {
        diagnostic::error("PCAPng currently unsupported")
          .hint("use `shell \"tshark -F pcap -r - -w -\"` to convert to PCAP")
          .note("visit https://github.com/tenzir/public-roadmap/issues/75")
          .emit(ctrl.diagnostics());
        co_return;
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
        TENZIR_DEBUG("detected different byte order in file and host");
        input_file_header = byteswap(input_file_header);
      } else {
        TENZIR_DEBUG("detected identical byte order in file and host");
      }
      if (emit_file_headers) {
        co_yield make_file_header_table_slice(input_file_header);
      }
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
            auto file_header_bytes = as_writeable_bytes(input_file_header);
            auto packet_header_bytes = as_bytes(packet.header);
            std::copy(packet_header_bytes.begin(), packet_header_bytes.end(),
                      file_header_bytes.begin());
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
              auto remainder
                = file_header_bytes.subspan<sizeof(packet_header)>();
              std::copy(bytes->begin(), bytes->end(), remainder.begin());
              break;
            }
            need_swap = need_byte_swap(input_file_header.magic_number);
            TENZIR_ASSERT(need_swap); // checked in is_file_header
            if (*need_swap) {
              TENZIR_DEBUG("detected different byte order in file and host");
              input_file_header = byteswap(input_file_header);
            } else {
              TENZIR_DEBUG("detected identical byte order in file and host");
            }
            // Before emitting the new file header, flush all buffered packets
            // from the previous trace.
            if (builder.rows() > 0) {
              last_finish = now;
              co_yield builder.finish();
            }
            if (emit_file_headers) {
              co_yield make_file_header_table_slice(input_file_header);
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
    return make(ctrl, std::move(input), !!args_.emit_file_headers);
  }

  friend auto inspect(auto& f, pcap_parser& x) -> bool {
    return f.object(x)
      .pretty_name("pcap_parser")
      .fields(f.field("args", x.args_));
  }

private:
  parser_args args_;
};

struct printer_args {};

/// Creates a file header from the first row of table slice (that is assumed to
/// have one row).
auto make_file_header(const table_slice& slice) -> std::optional<file_header> {
  if (slice.schema().name() != "pcap.file_header" || slice.rows() == 0) {
    return std::nullopt;
  }
  auto result = file_header{};
  const auto& input_record = caf::get<record_type>(slice.schema());
  auto array = to_record_batch(slice)->ToStructArray().ValueOrDie();
  auto xs = values(input_record, *array);
  auto begin = xs.begin();
  if (begin == xs.end() || *begin == std::nullopt) {
    return std::nullopt;
  }
  for (const auto& [key, value] : **begin) {
    // TODO: Make this more robust, and give a helpful error message if the
    // types are not as expected. This also applies to `to_packet_record`.
    if (key == "magic_number") {
      auto magic_number = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(magic_number);
      result.magic_number = detail::narrow_cast<uint32_t>(*magic_number);
      continue;
    }
    if (key == "major_version") {
      auto major_version = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(major_version);
      result.major_version = detail::narrow_cast<uint16_t>(*major_version);
      continue;
    }
    if (key == "minor_version") {
      auto minor_version = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(minor_version);
      result.minor_version = detail::narrow_cast<uint16_t>(*minor_version);
      continue;
    }
    if (key == "reserved1") {
      auto reserved1 = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(reserved1);
      result.reserved1 = detail::narrow_cast<uint32_t>(*reserved1);
      continue;
    }
    if (key == "reserved2") {
      auto reserved2 = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(reserved2);
      result.reserved2 = detail::narrow_cast<uint32_t>(*reserved2);
      continue;
    }
    if (key == "snaplen") {
      auto snaplen = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(snaplen);
      result.snaplen = detail::narrow_cast<uint32_t>(*snaplen);
      continue;
    }
    if (key == "linktype") {
      auto linktype = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(linktype);
      result.linktype = detail::narrow_cast<uint32_t>(*linktype);
      continue;
    }
    TENZIR_DEBUG("ignoring unknown PCAP file header key '{}' with value {}",
                 key, value);
  }
  return result;
}

/// Constructs a PCAP file header with a given link type.
auto make_file_header(uint32_t linktype) -> file_header {
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

/// Creates a packet record in host-byte order and nanosecond timestamp
/// resolution, i.e., for a fileheader with `magic_number_2`.
auto to_packet_record(auto row) -> std::pair<packet_record, uint32_t> {
  auto pkt = packet_record{};
  auto linktype = uint32_t{0};
  auto timestamp = time{};
  // NB: the API for record_view feels iffy. It should expose a field-based
  // access method, as opposed to just key-value pairs.
  for (const auto& [key, value] : row) {
    if (key == "linktype") {
      auto linktype_ptr = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(linktype_ptr);
      linktype = detail::narrow_cast<uint32_t>(*linktype_ptr);
    } else if (key == "timestamp") {
      auto timestamp_ptr = caf::get_if<time>(&value);
      TENZIR_ASSERT(timestamp_ptr);
      timestamp = *timestamp_ptr;
    } else if (key == "captured_packet_length") {
      auto captured_packet_length = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(captured_packet_length);
      pkt.header.captured_packet_length = *captured_packet_length;
    } else if (key == "original_packet_length") {
      auto original_packet_length = caf::get_if<uint64_t>(&value);
      TENZIR_ASSERT(original_packet_length);
      pkt.header.original_packet_length = *original_packet_length;
    } else if (key == "data") {
      if (auto str_data = caf::get_if<view<std::string>>(&value)) {
        // TODO: Remove this fallback eventually.
        pkt.data = std::span<const std::byte>{
          reinterpret_cast<const std::byte*>(str_data->data()),
          str_data->size()};
      } else {
        auto data = caf::get_if<view<blob>>(&value);
        TENZIR_ASSERT(data);
        pkt.data = *data;
      }
    } else {
      TENZIR_WARN("got invalid PCAP header field ''", key);
    }
  }
  // Split the timestamp in two pieces.
  auto ns = timestamp.time_since_epoch();
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(ns);
  auto fraction = ns - secs;
  auto timestamp_fraction = detail::narrow_cast<uint32_t>(fraction.count());
  pkt.header.timestamp = detail::narrow_cast<uint32_t>(secs.count());
  pkt.header.timestamp_fraction = timestamp_fraction;
  // Translate the string to raw packet data.
  return {pkt, linktype};
}

class pcap_printer final : public plugin_printer {
public:
  pcap_printer() = default;

  explicit pcap_printer(printer_args args) : args_{std::move(args)} {
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
       current_file_header = std::optional<file_header>{},
       file_header_printed = false, buffer = std::vector<std::byte>{},
       meta
       = std::move(meta)](table_slice slice) mutable -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        // We may receive multiple file headers. If we receive any, we take
        // it into consideration for timestamp resolution.
        if (slice.schema().name() == "pcap.file_header") {
          TENZIR_DEBUG("got new PCAP file header");
          if (auto header = make_file_header(slice)) {
            current_file_header = *header;
          } else {
            diagnostic::warning("failed to parse PCAP file header")
              .emit(ctrl.diagnostics());
          }
          co_yield {};
          co_return;
        }
        // Helper function to process a row in a table slice of packets.
        auto process_packet_row = [&](auto row) -> std::optional<diagnostic> {
          auto [pkt, linktype] = to_packet_record(row);
          // Generate file header based on first packet or fail if the packet
          // is incompatible with the known file header.
          if (not current_file_header) {
            TENZIR_DEBUG("generating PCAP file header");
            current_file_header = make_file_header(linktype);
          } else if (linktype != current_file_header->linktype) {
            return diagnostic::error(
                     "packet linktype doesn't match file header")
              .done();
          } else if (current_file_header->magic_number == magic_number_1) {
            pkt.header.timestamp_fraction /= 1'000;
          }
          auto bytes = as_bytes(pkt.header);
          buffer.reserve(sizeof(packet_header) + pkt.data.size());
          buffer.insert(buffer.end(), bytes.begin(), bytes.end());
          buffer.insert(buffer.end(), pkt.data.begin(), pkt.data.end());
          return {};
        };
        // Extract PCAP data from input.
        const auto& input_record = caf::get<record_type>(slice.schema());
        if (slice.schema().name() == "pcap.packet") {
          auto resolved_slice = resolve_enumerations(slice);
          auto array
            = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
          for (const auto& row : values(input_record, *array)) {
            TENZIR_ASSERT(row);
            if (auto diag = process_packet_row(*row)) {
              ctrl.diagnostics().emit(std::move(*diag));
              co_return;
            }
          }
        } else if (slice.schema().name() == "tenzir.packet") {
          const auto pcap_index = input_record.resolve_key("pcap");
          if (not pcap_index) {
            TENZIR_VERBOSE("ignoring tenzir.packet events without pcap field");
            co_yield {};
            co_return;
          }
          auto [pcap_type, pcap_array] = pcap_index->get(slice);
          const auto* pcap_record_type = caf::get_if<record_type>(&pcap_type);
          const auto* pcap_values
            = caf::get_if<arrow::StructArray>(&*pcap_array);
          if (not(pcap_record_type or pcap_values)) {
            diagnostic::warning("got a malformed 'tenzir.packet' event")
              .note("field 'pcap' not a record")
              .emit(ctrl.diagnostics());
            co_yield {};
            co_return;
          }
          for (const auto& row : values(*pcap_record_type, *pcap_values)) {
            TENZIR_ASSERT(row);
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
          co_yield chunk::copy(as_bytes(*current_file_header), meta);
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
  auto initialize(const record& config,
                  const record& /* global_config */) -> caf::error override {
    config_ = config;
    return caf::none;
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto args = parser_args{};
    parser.add("-e,--emit-file-headers", args.emit_file_headers);
    parser.parse(p);
    return std::make_unique<pcap_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
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

class write_plugin final
  : public virtual operator_plugin2<writer_adapter<pcap_printer>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<writer_adapter<pcap_printer>>(pcap_printer{});
  }
};

} // namespace

} // namespace tenzir::plugins::pcap

TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::pcap::write_plugin)
