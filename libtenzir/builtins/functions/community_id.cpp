//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/as_bytes.hpp>
#include <tenzir/community_id.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/flow.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/fused_downstream_manager.hpp>

namespace tenzir::plugins::community_id {

namespace {

struct arguments {
  ast::expression src_ip;
  ast::expression dst_ip;
  ast::expression dst_port;
  ast::expression src_port;
  ast::expression proto;
};

class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.community_id";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto args = arguments{};
    TRY(argument_parser2::function("community_id")
          .add(args.src_ip, "<source ip>")
          .add(args.dst_ip, "<destination ip>")
          .add(args.src_port, "<source port>")
          .add(args.dst_port, "<destination port>")
          .add(args.proto, "<transport protocol>")
          .parse(inv, ctx));
    return function_use::make([args = std::move(args)](evaluator eval,
                                                       session ctx) -> series {
      auto null_series = [&] {
        return series::null(string_type{}, eval.length());
      };
      auto src_ip_series = eval(args.src_ip);
      if (not caf::holds_alternative<ip_type>(src_ip_series.type)) {
        diagnostic::warning("`community_id` expected `ip` as 1st argument")
          .primary(args.src_ip)
          .emit(ctx);
        return null_series();
      }
      auto dst_ip_series = eval(args.dst_ip);
      if (not caf::holds_alternative<ip_type>(dst_ip_series.type)) {
        diagnostic::warning("`community_id` expected `ip` as 3rd argument")
          .primary(args.dst_ip)
          .emit(ctx);
        return null_series();
      }
      auto src_port_series = eval(args.src_port);
      if (not caf::holds_alternative<int64_type>(src_port_series.type)) {
        diagnostic::warning("`community_id` expected `integer` as 2nd argument")
          .primary(args.src_port)
          .emit(ctx);
        return null_series();
      }
      auto dst_port_series = eval(args.dst_port);
      if (not caf::holds_alternative<int64_type>(dst_port_series.type)) {
        diagnostic::warning("`community_id` expected `integer` as 4th argument")
          .primary(args.dst_port)
          .emit(ctx);
        return null_series();
      }
      auto proto_series = eval(args.proto);
      if (not caf::holds_alternative<string_type>(proto_series.type)) {
        diagnostic::warning("`community_id` expected `string` as 5th argument")
          .primary(args.dst_port)
          .emit(ctx);
        return null_series();
      }
      auto b = arrow::StringBuilder{};
      check(b.Reserve(eval.length()));
      auto src_ips = src_ip_series.as<ip_type>();
      auto dst_ips = dst_ip_series.as<ip_type>();
      auto src_ports = src_port_series.as<int64_type>();
      auto dst_ports = dst_port_series.as<int64_type>();
      auto protos = proto_series.as<string_type>();
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        if (src_ips->array->IsNull(i) or src_ports->array->IsNull(i)
            or dst_ips->array->IsNull(i) or dst_ports->array->IsNull(i)
            or protos->array->IsNull(i)) {
          check(b.AppendNull());
          continue;
        }
        const auto* src_ip_ptr = src_ips->array->storage()->GetValue(i);
        const auto* dst_ip_ptr = dst_ips->array->storage()->GetValue(i);
        auto src_ip = ip::v6(as_bytes<16>(src_ip_ptr));
        auto dst_ip = ip::v6(as_bytes<16>(dst_ip_ptr));
        auto src_port = src_ports->array->GetView(i);
        auto dst_port = dst_ports->array->GetView(i);
        auto proto = protos->array->GetView(i);
        auto type = port_type::unknown;
        if (proto == "tcp") {
          type = port_type::tcp;
        } else if (proto == "udp") {
          type = port_type::udp;
        } else if (proto == "icmp") {
          type = port_type::icmp;
        } else if (proto == "icmp6") {
          type = port_type::icmp6;
        } else {
          diagnostic::warning("`community_id` expected `tcp`, `udp`, `icmp`, "
                              "or `icmp6` as protocol")
            .primary(args.proto)
            .emit(ctx);
          return null_series();
        }
        auto flow
          = make_flow(src_ip, dst_ip, detail::narrow<uint16_t>(src_port),
                      detail::narrow<uint16_t>(dst_port), type);
        auto seed = uint16_t{0}; // TODO: make an optional parameter.
        check(
          b.Append(tenzir::community_id::compute<policy::base64>(flow, seed)));
      }
      return series{string_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::community_id

TENZIR_REGISTER_PLUGIN(tenzir::plugins::community_id::plugin)
