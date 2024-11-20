//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/community_id.hpp>
#include <tenzir/error.hpp>
#include <tenzir/ether_type.hpp>
#include <tenzir/flow.hpp>
#include <tenzir/frame_type.hpp>
#include <tenzir/location.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/mac.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/record_batch.h>
#include <netinet/in.h>

namespace tenzir::plugins::decapsulate {

namespace {

auto to_uint16(std::span<const std::byte, 2> bytes) {
  const auto* data = bytes.data();
  const auto* ptr = reinterpret_cast<const uint16_t*>(std::launder(data));
  return detail::to_host_order(*ptr);
}

/// An 802.3 Ethernet frame.
struct ethernet_frame {
  // 2 MAC addresses and the 2-byte EtherType.
  static constexpr size_t header_size = 6 + 6 + 2;

  static auto make(std::span<const std::byte> bytes)
    -> std::optional<ethernet_frame> {
    if (bytes.size() < header_size) {
      return std::nullopt;
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
          return std::nullopt;
        }
        result.outer_vid = to_uint16(bytes.subspan<14, 2>());
        *result.outer_vid &= 0x0FFF; // lower 12 bits only
        result.type = as_ether_type(bytes.subspan<16, 2>());
        result.payload = bytes.subspan(min_frame_size);
        // Keep going for QinQ frames (TPID = 0x8100).
        if (result.type == ether_type::ieee_802_1aq) {
          min_frame_size += 4;
          if (bytes.size() < min_frame_size) {
            return std::nullopt;
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
          return std::nullopt;
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

  mac dst;                             ///< Destination MAC address
  mac src;                             ///< Source MAC address
  std::optional<uint16_t> outer_vid{}; ///< Outer 802.1Q tag control information
  std::optional<uint16_t> inner_vid{}; ///< Outer 802.1Q tag control information
  ether_type type{ether_type::invalid}; ///< EtherType
  std::span<const std::byte> payload{}; ///< Payload
};

/// An IP packet.
struct packet {
  static auto make(std::span<const std::byte> bytes, ether_type type)
    -> std::optional<packet> {
    packet result;
    switch (type) {
      default:
        break;
      case ether_type::ipv4: {
        constexpr size_t ipv4_header_size = 20;
        if (bytes.size() < ipv4_header_size) {
          return std::nullopt;
        }
        size_t header_length = (std::to_integer<uint8_t>(bytes[0]) & 0x0f) * 4;
        if (bytes.size() < header_length) {
          return std::nullopt;
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
          return std::nullopt;
        }
        result.src = ip::v6(bytes.subspan<8, 16>());
        result.dst = ip::v6(bytes.subspan<24, 16>());
        result.type = std::to_integer<uint8_t>(bytes[6]);
        result.payload = bytes.subspan(40);
        return result;
      }
    }
    return std::nullopt;
  }

  ip src{};
  ip dst{};
  uint8_t type{0};
  std::span<const std::byte> payload{};
};

/// A layer 4 segment.
struct segment {
  static auto make(std::span<const std::byte> bytes, uint8_t type)
    -> std::optional<segment> {
    segment result;
    switch (type) {
      default:
        break;
      case IPPROTO_TCP: {
        constexpr size_t min_tcp_header_size = 20;
        if (bytes.size() < min_tcp_header_size) {
          return std::nullopt;
        }
        result.src = to_uint16(bytes.subspan<0, 2>());
        result.dst = to_uint16(bytes.subspan<2, 2>());
        result.type = port_type::tcp;
        size_t data_offset = (std::to_integer<uint8_t>(bytes[12]) >> 4) * 4;
        if (bytes.size() < data_offset) {
          return std::nullopt;
        }
        result.payload = bytes.subspan(data_offset);
        return result;
      }
      case IPPROTO_UDP: {
        constexpr size_t udp_header_size = 8;
        if (bytes.size() < udp_header_size) {
          return std::nullopt;
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
          return std::nullopt;
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
    return std::nullopt;
  }

  uint16_t src{0};
  uint16_t dst{0};
  port_type type{port_type::unknown};
  std::span<const std::byte> payload{};
};

/// Parses a packet in a sequence where each step is split into two parts:
/// 1. Reconstruct the header structure into a dedicated structure.
/// 2. Append the structure to the builder.
auto parse(record_ref builder, std::span<const std::byte> bytes,
           frame_type type) -> std::optional<diagnostic> {
  // Parse layer 2.
  auto frame_payload = std::span<const std::byte>{};
  auto frame_type = ether_type::invalid;
  switch (type) {
    default:
      TENZIR_TRACE("failed to parse layer-2 frame");
      return std::nullopt;
    case frame_type::ethernet: {
      // Parse Ethernet frame.
      auto frame = ethernet_frame::make(bytes);
      if (!frame) {
        TENZIR_TRACE("failed to parse layer-2 frame");
        return std::nullopt;
      }
      auto ether = builder.field("ether").record();
      auto src_str = fmt::to_string(frame->src);
      auto dst_str = fmt::to_string(frame->dst);
      ether.field("src").data(std::string_view{src_str});
      ether.field("dst").data(std::string_view{dst_str});
      if (frame->outer_vid) {
        auto vlan = builder.field("vlan").record();
        vlan.field("outer").data(static_cast<uint64_t>(*frame->outer_vid));
        if (frame->inner_vid) {
          vlan.field("inner").data(static_cast<uint64_t>(*frame->inner_vid));
        }
      }
      ether.field("type").data(static_cast<uint64_t>(frame->type));
      frame_payload = frame->payload;
      frame_type = frame->type;
      break;
    }
    case frame_type::sll2: {
      constexpr size_t sll2_header_size = 20;
      if (bytes.size() < sll2_header_size) {
        TENZIR_TRACE("skipping invalid SLL2 frame");
        return std::nullopt;
      }
      frame_payload = bytes.subspan(sll2_header_size);
      frame_type = static_cast<ether_type>(to_uint16(bytes.subspan<0, 2>()));
      break;
    }
  }
  // Parse layer 3.
  auto packet = packet::make(frame_payload, frame_type);
  if (!packet) {
    TENZIR_TRACE("failed to parse layer-3 packet");
    return std::nullopt;
  }
  auto ip = builder.field("ip").record();
  ip.field("src").data(packet->src);
  ip.field("dst").data(packet->dst);
  ip.field("type").data(static_cast<uint64_t>(packet->type));
  // Parse layer 4.
  auto segment = segment::make(packet->payload, packet->type);
  if (!segment) {
    TENZIR_TRACE("failed to parse layer-4 segment");
    return std::nullopt;
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
  builder.field("community_id").data(cid);
  return std::nullopt;
}

auto decapsulate(const series& s, diagnostic_handler& dh, bool include_old)
  -> std::optional<series> {
  // Get the packet payload.
  if (s.type.kind().is_not<record_type>()) {
    if (s.type.kind().is_not<null_type>()) {
      diagnostic::warning("expected `record`, got `{}`", s.type.kind()).emit(dh);
    }
    return std::nullopt;
  }
  const auto& layout = as<record_type>(s.type);
  const auto linktype_index = layout.resolve_key("linktype");
  if (!linktype_index) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("schema 'pcap.packet' must have a 'linktype' field")
      .emit(dh);
    return std::nullopt;
  }
  const auto linktype_array
    = linktype_index->get(as<arrow::StructArray>(*s.array));
  const auto linktype_values = try_as<arrow::UInt64Array>(&*linktype_array);
  if (!linktype_values) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("field 'linktype' not of type uint64")
      .emit(dh);
    return std::nullopt;
  }
  const auto data_index = layout.resolve_key("data");
  if (!data_index) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("schema 'pcap.packet' must have a 'data' field")
      .emit(dh);
    return std::nullopt;
  }
  const auto data_array = data_index->get(as<arrow::StructArray>(*s.array));
  const auto data_values = try_as<arrow::BinaryArray>(&*data_array);
  if (!data_values) {
    diagnostic::warning("got a malformed 'pcap.packet' event")
      .note("field 'data' not of type blob")
      .emit(dh);
    return std::nullopt;
  }
  auto builder = series_builder{};
  for (auto i = 0u; i < s.length(); ++i) {
    const auto linktype = (*linktype_values)[i];
    const auto data = (*data_values)[i];
    if (!data) {
      continue;
    }
    auto inferred_type = static_cast<frame_type>(linktype ? *linktype : 0);
    if (auto diag = parse(builder.record(), as_bytes(*data), inferred_type)) {
      dh.emit(std::move(*diag));
    }
  }
  auto new_s = builder.finish_assert_one_array();
  new_s.type = type{s.type.name(), new_s.type};
  if (include_old) {
    // Add back the untouched data column at the end.
    auto transformation = indexed_transformation{
      .index = {as<record_type>(new_s.type).num_fields() - 1},
      .fun = [&](struct record_type::field in_field,
                 std::shared_ptr<arrow::Array> in_array)
        -> indexed_transformation::result_type {
        return {
          {std::move(in_field), std::move(in_array)},
          {{"pcap", s.type}, s.array},
        };
      },
    };
    const auto ptr = std::dynamic_pointer_cast<arrow::StructArray>(new_s.array);
    TENZIR_ASSERT(ptr);
    auto [ty, transformed]
      = transform_columns(new_s.type, ptr, {std::move(transformation)});
    return series{ty, transformed};
  }
  return new_s;
}

class decapsulate_operator final : public crtp_operator<decapsulate_operator> {
public:
  decapsulate_operator() = default;

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto s = decapsulate(series{slice}, ctrl.diagnostics(), true);
      if (not s) {
        co_yield {};
        continue;
      }
      auto* ptr = try_as<arrow::StructArray>(&*s->array);
      TENZIR_ASSERT(ptr);
      auto batch = arrow::RecordBatch::Make(s->type.to_arrow_schema(),
                                            s->length(), ptr->fields());
      co_yield table_slice{batch, s->type};
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  auto name() const -> std::string override {
    return "decapsulate";
  }

  friend auto inspect(auto& f, decapsulate_operator& x) -> bool {
    return f.object(x).pretty_name("decapsulate_operator").fields();
  }
};

class plugin final : public virtual operator_plugin<decapsulate_operator>,
                     public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "decapsulate";
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("tql2.decapsulate")
          .add(expr, "<expr>")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto series = eval(expr);
        if (auto op = decapsulate(series, ctx.dh(), false)) {
          return op.value();
        }
        return series::null(null_type{}, series.length());
      });
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{name(), fmt::format("https://docs.tenzir.com/"
                                                      "operators/{}",
                                                      name())};
    parser.parse(p);
    return std::make_unique<decapsulate_operator>();
  }

private:
  record config_;
};

} // namespace

} // namespace tenzir::plugins::decapsulate

TENZIR_REGISTER_PLUGIN(tenzir::plugins::decapsulate::plugin)
