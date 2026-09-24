//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/community_id.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ether_type.hpp>
#include <tenzir/flow.hpp>
#include <tenzir/frame_type.hpp>
#include <tenzir/location.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/mac.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <netinet/in.h>

#include <string_view>

namespace tenzir::plugins::decapsulate {

namespace {

auto to_uint16(std::span<const std::byte, 2> bytes) {
  return uint16_t((uint16_t(bytes[0]) << 8) | uint16_t(bytes[1]));
}

/// An 802.3 Ethernet frame.
struct ethernet_frame {
  // 2 MAC addresses and the 2-byte EtherType.
  static constexpr size_t header_size = 6 + 6 + 2;

  static auto make(std::span<const std::byte> bytes) -> Option<ethernet_frame> {
    if (bytes.size() < header_size) {
      return None{};
    }
    auto result = ethernet_frame{};
    result.dst = mac{bytes.subspan<0, 6>()};
    result.src = mac{bytes.subspan<6, 6>()};
    auto type = as_ether_type(bytes.subspan<12, 2>());
    switch (type) {
      default:
        result.type = type;
        result.payload = bytes.subspan<header_size>();
        break;
      case ether_type::ieee_802_1aq: {
        size_t min_frame_size = 6 + 6 + 4 + 2;
        if (bytes.size() < min_frame_size) {
          return None{};
        }
        result.outer_vid = to_uint16(bytes.subspan<14, 2>());
        *result.outer_vid &= 0x0FFF; // lower 12 bits only
        result.type = as_ether_type(bytes.subspan<16, 2>());
        result.payload = bytes.subspan(min_frame_size);
        // Keep going for QinQ frames (TPID = 0x8100).
        if (result.type == ether_type::ieee_802_1aq) {
          min_frame_size += 4;
          if (bytes.size() < min_frame_size) {
            return None{};
          }
          result.inner_vid = to_uint16(bytes.subspan<18, 2>());
          *result.inner_vid &= 0x0FFF; // lower 12 bits only
          result.type = as_ether_type(bytes.subspan<20, 2>());
          result.payload = bytes.subspan(min_frame_size);
        }
        break;
      }
      case ether_type::ieee_802_1q_db: {
        constexpr size_t min_frame_size = 6 + 6 + 4 + 4 + 2;
        if (bytes.size() < min_frame_size) {
          return None{};
        }
        result.outer_vid = to_uint16(bytes.subspan<14, 2>());
        *result.outer_vid &= 0x0FFF; // lower 12 bits only
        result.inner_vid = to_uint16(bytes.subspan<18, 2>());
        *result.inner_vid &= 0x0FFF; // lower 12 bits only
        result.type = as_ether_type(bytes.subspan<20, 2>());
        result.payload = bytes.subspan<min_frame_size>();
        break;
      }
    }
    return result;
  }

  mac dst;                      ///< Destination MAC address
  mac src;                      ///< Source MAC address
  Option<uint16_t> outer_vid{}; ///< Outer 802.1Q tag control information
  Option<uint16_t> inner_vid{}; ///< Outer 802.1Q tag control information
  ether_type type{ether_type::invalid}; ///< EtherType
  std::span<const std::byte> payload{}; ///< Payload
};

/// An IP packet.
struct packet {
  static auto make(std::span<const std::byte> bytes, ether_type type)
    -> Option<packet> {
    packet result;
    switch (type) {
      default:
        break;
      case ether_type::ipv4: {
        constexpr size_t ipv4_header_size = 20;
        if (bytes.size() < ipv4_header_size) {
          return None{};
        }
        size_t header_length = (std::to_integer<uint8_t>(bytes[0]) & 0x0f) * 4;
        if (bytes.size() < header_length) {
          return None{};
        }
        result.src = ip::v4(bytes.subspan<12, 4>());
        result.dst = ip::v4(bytes.subspan<16, 4>());
        result.type = std::to_integer<uint8_t>(bytes[9]);
        result.payload = bytes.subspan(header_length);
        return result;
      }
      case ether_type::ipv6: {
        constexpr size_t ipv6_header_size = 40;
        if (bytes.size() < ipv6_header_size) {
          return None{};
        }
        result.src = ip::v6(bytes.subspan<8, 16>());
        result.dst = ip::v6(bytes.subspan<24, 16>());
        result.type = std::to_integer<uint8_t>(bytes[6]);
        result.payload = bytes.subspan(40);
        return result;
      }
    }
    return None{};
  }

  ip src{};
  ip dst{};
  uint8_t type{0};
  std::span<const std::byte> payload{};
};

/// A layer 4 segment.
struct segment {
  static auto make(std::span<const std::byte> bytes, uint8_t type)
    -> Option<segment> {
    segment result;
    switch (type) {
      default:
        break;
      case IPPROTO_TCP: {
        constexpr size_t min_tcp_header_size = 20;
        if (bytes.size() < min_tcp_header_size) {
          return None{};
        }
        result.src = to_uint16(bytes.subspan<0, 2>());
        result.dst = to_uint16(bytes.subspan<2, 2>());
        result.type = port_type::tcp;
        size_t data_offset = (std::to_integer<uint8_t>(bytes[12]) >> 4) * 4;
        if (bytes.size() < data_offset) {
          return None{};
        }
        result.payload = bytes.subspan(data_offset);
        return result;
      }
      case IPPROTO_UDP: {
        constexpr size_t udp_header_size = 8;
        if (bytes.size() < udp_header_size) {
          return None{};
        }
        result.src = to_uint16(bytes.subspan<0, 2>());
        result.dst = to_uint16(bytes.subspan<2, 2>());
        result.type = port_type::udp;
        result.payload = bytes.subspan<8>();
        return result;
      }
      case IPPROTO_ICMP: {
        constexpr size_t icmp_header_size = 8;
        if (bytes.size() < icmp_header_size) {
          return None{};
        }
        auto message_type = std::to_integer<uint8_t>(bytes[0]);
        auto message_code = std::to_integer<uint8_t>(bytes[1]);
        result.src = message_type;
        result.dst = message_code;
        result.type = port_type::icmp;
        result.payload = bytes.subspan<8>();
        return result;
      }
    }
    return None{};
  }

  uint16_t src{0};
  uint16_t dst{0};
  port_type type{port_type::unknown};
  std::span<const std::byte> payload{};
};

/// Parses a packet in a sequence where each step is split into two parts:
/// 1. Reconstruct the header structure into a dedicated structure.
/// 2. Append the structure to the builder.
///
/// Works with both the legacy `record_ref` and nova's record builder.
template <class RecordBuilder>
auto parse(RecordBuilder builder, std::span<const std::byte> bytes,
           frame_type type) -> Option<diagnostic> {
  // Parse layer 2.
  auto frame_payload = std::span<const std::byte>{};
  auto frame_type = ether_type::invalid;
  switch (type) {
    default:
      TENZIR_TRACE("failed to parse layer-2 frame");
      return None{};
    case frame_type::ethernet: {
      // Parse Ethernet frame.
      auto frame = ethernet_frame::make(bytes);
      if (not frame) {
        TENZIR_TRACE("failed to parse layer-2 frame");
        return None{};
      }
      auto ether = builder.field("ether").record();
      auto src_str = fmt::to_string(frame->src);
      auto dst_str = fmt::to_string(frame->dst);
      ether.field("src").data(std::string_view{src_str});
      ether.field("dst").data(std::string_view{dst_str});
      // Finish `ether` before adding a sibling field.
      ether.field("type").data(static_cast<uint64_t>(frame->type));
      if (frame->outer_vid) {
        auto vlan = builder.field("vlan").record();
        vlan.field("outer").data(static_cast<uint64_t>(*frame->outer_vid));
        if (frame->inner_vid) {
          vlan.field("inner").data(static_cast<uint64_t>(*frame->inner_vid));
        }
      }
      frame_payload = frame->payload;
      frame_type = frame->type;
      break;
    }
    case frame_type::sll2: {
      constexpr size_t sll2_header_size = 20;
      if (bytes.size() < sll2_header_size) {
        TENZIR_TRACE("skipping invalid SLL2 frame");
        return None{};
      }
      frame_payload = bytes.subspan(sll2_header_size);
      frame_type = static_cast<ether_type>(to_uint16(bytes.subspan<0, 2>()));
      break;
    }
  }
  // Parse layer 3.
  auto packet = packet::make(frame_payload, frame_type);
  if (not packet) {
    TENZIR_TRACE("failed to parse layer-3 packet");
    return None{};
  }
  auto ip = builder.field("ip").record();
  ip.field("src").data(packet->src);
  ip.field("dst").data(packet->dst);
  ip.field("type").data(static_cast<uint64_t>(packet->type));
  // Parse layer 4.
  auto segment = segment::make(packet->payload, packet->type);
  if (not segment) {
    TENZIR_TRACE("failed to parse layer-4 segment");
    return None{};
  }
  switch (segment->type) {
    case port_type::icmp: {
      auto icmp = builder.field("icmp").record();
      icmp.field("type").data(uint64_t{segment->src});
      icmp.field("code").data(uint64_t{segment->dst});
      break;
    }
    case port_type::tcp: {
      auto tcp = builder.field("tcp").record();
      tcp.field("src_port").data(uint64_t{segment->src});
      tcp.field("dst_port").data(uint64_t{segment->dst});
      break;
    }
    case port_type::udp: {
      auto udp = builder.field("udp").record();
      udp.field("src_port").data(uint64_t{segment->src});
      udp.field("dst_port").data(uint64_t{segment->dst});
      break;
    }
    case port_type::icmp6:
    case port_type::sctp:
    case port_type::unknown:
      break;
  }
  // Compute Community ID.
  auto conn = make_flow(packet->src, packet->dst, segment->src, segment->dst,
                        segment->type);
  auto cid = community_id::make(conn);
  builder.field("community_id").data(std::string_view{cid});
  return None{};
}

auto decapsulate(const series& s, diagnostic_handler& dh) -> Option<series> {
  // Get the packet payload.
  if (s.type.kind().is_not<record_type>()) {
    if (s.type.kind().is_not<null_type>()) {
      diagnostic::warning("expected `record`, got `{}`", s.type.kind()).emit(dh);
    }
    return None{};
  }
  const auto& layout = as<record_type>(s.type);
  const auto linktype_index = layout.resolve_key("linktype");
  if (not linktype_index) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("schema 'pcap.packet' must have a 'linktype' field")
      .emit(dh);
    return None{};
  }
  const auto linktype_array
    = linktype_index->get(as<arrow::StructArray>(*s.array));
  const auto linktype_values = try_as<arrow::UInt64Array>(&*linktype_array);
  if (not linktype_values) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("field 'linktype' not of type uint64")
      .emit(dh);
    return None{};
  }
  const auto data_index = layout.resolve_key("data");
  if (not data_index) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("schema 'pcap.packet' must have a 'data' field")
      .emit(dh);
    return None{};
  }
  const auto data_array = data_index->get(as<arrow::StructArray>(*s.array));
  const auto data_values = try_as<arrow::BinaryArray>(&*data_array);
  if (not data_values) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("field 'data' not of type blob")
      .emit(dh);
    return None{};
  }
  auto builder = series_builder{};
  for (auto i = 0u; i < s.length(); ++i) {
    const auto linktype = (*linktype_values)[i];
    const auto data = (*data_values)[i];
    if (not data) {
      continue;
    }
    auto inferred_type = static_cast<frame_type>(linktype ? *linktype : 0);
    if (auto diag = parse(builder.record(), as_bytes(*data), inferred_type)) {
      dh.emit(std::move(*diag));
    }
  }
  auto new_s = builder.finish_assert_one_array();
  new_s.type = type{s.type.name(), new_s.type};
  return new_s;
}

auto kind(nova::RowView<nova::Data> const& value) -> std::string_view {
  return match(value, []<class T>(nova::RowView<T> const&) -> std::string_view {
    return nova::Type<T>::static_name;
  });
}

auto find_field(nova::RowView<nova::Record> const& row, std::string_view name)
  -> Option<nova::RowView<nova::Data>> {
  for (auto const& [key, value] : row) {
    if (key == name) {
      return value;
    }
  }
  return None{};
}

struct DecapsulateArgs {
  nova::ValueArgument packet;
};

struct DecapsulateFunction {
  auto eval(DecapsulateArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto wrong_type = nova::WarnOnce{};
    auto malformed = nova::WarnOnce{};
    auto warn_malformed = [&](std::string_view note) {
      malformed(frame,
                diagnostic::warning("got a malformed 'pcap.packet' event")
                  .primary(args.packet.source)
                  .note("{}", note));
    };
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto append = [&](nova::storage::Index row) {
      auto value = args.packet.data.get(row);
      auto const* packet = try_as<nova::RowView<nova::Record>>(value);
      if (not packet) {
        if (not is<nova::RowView<nova::Null>>(value)) {
          wrong_type(frame, diagnostic::warning("expected `record`, got `{}`",
                                                kind(value))
                              .primary(args.packet.source));
        }
        builder.null();
        return;
      }
      auto linktype = find_field(*packet, "linktype");
      if (not linktype) {
        warn_malformed("schema 'pcap.packet' must have a 'linktype' field");
        builder.null();
        return;
      }
      auto frame_kind = uint64_t{0};
      if (auto const* x = try_as<nova::RowView<nova::UInt>>(*linktype)) {
        frame_kind = **x;
      } else if (not is<nova::RowView<nova::Null>>(*linktype)) {
        warn_malformed("field 'linktype' not of type uint64");
        builder.null();
        return;
      }
      auto data = find_field(*packet, "data");
      if (not data) {
        warn_malformed("schema 'pcap.packet' must have a 'data' field");
        builder.null();
        return;
      }
      auto const* bytes = try_as<nova::RowView<nova::Blob>>(*data);
      if (not bytes) {
        if (not is<nova::RowView<nova::Null>>(*data)) {
          warn_malformed("field 'data' not of type blob");
        }
        builder.null();
        return;
      }
      if (auto diag = parse(builder.record(), **bytes,
                            static_cast<frame_type>(frame_kind))) {
        static_cast<diagnostic_handler&>(frame).emit(std::move(*diag));
      }
    };
    nova::storage::for_each_true(frame.mask(), [&](auto row) {
      builder.skip_n(row - builder.length());
      append(row);
    });
    builder.skip_n(frame.length() - builder.length());
    return builder.finish();
  }
};

class plugin final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "decapsulate";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<DecapsulateArgs, DecapsulateFunction>{};
    d.positional("packet", &DecapsulateArgs::packet, "record");
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("packet", expr, "record")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series series) {
          if (auto op = decapsulate(series, ctx.dh())) {
            return op.value();
          }
          return series::null(null_type{}, series.length());
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::decapsulate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::decapsulate::plugin)
