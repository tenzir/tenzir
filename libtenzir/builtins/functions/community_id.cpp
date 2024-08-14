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
  ast::expression proto;
  std::optional<ast::expression> dst_port;
  std::optional<ast::expression> src_port;
  std::optional<ast::expression> seed;
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
          .add(args.proto, "<transport protocol>")
          .add("src_port", args.src_port)
          .add("dst_port", args.dst_port)
          .add("seed", args.seed)
          .parse(inv, ctx));
    return function_use::make([args = std::move(args)](evaluator eval,
                                                       session ctx) -> series {
      auto null_series = [&] {
        return series::null(string_type{}, eval.length());
      };
      auto src_ip_series = eval(args.src_ip);
      if (caf::holds_alternative<null_type>(src_ip_series.type)) {
        return null_series();
      }
      auto dst_ip_series = eval(args.dst_ip);
      if (caf::holds_alternative<null_type>(dst_ip_series.type)) {
        return null_series();
      }
      auto proto_series = eval(args.proto);
      if (caf::holds_alternative<null_type>(proto_series.type)) {
        return null_series();
      }
      auto src_port_series = std::optional<series>{};
      if (args.src_port) {
        src_port_series = eval(*args.src_port);
        if (caf::holds_alternative<null_type>(src_port_series->type)) {
          src_port_series = std::nullopt;
        }
      }
      auto dst_port_series = std::optional<series>{};
      if (args.dst_port) {
        dst_port_series = eval(*args.dst_port);
        if (caf::holds_alternative<null_type>(dst_port_series->type)) {
          dst_port_series = std::nullopt;
        }
      }
      if (dst_port_series.has_value() != dst_port_series.has_value()) {
        diagnostic::warning("`community_id` requires either two ports or none")
          .hint("set `src_port` and `dst_port` together or omit both")
          .emit(ctx);
        return null_series();
      }
      auto seed_series = std::optional<series>{};
      if (args.seed) {
        seed_series = eval(*args.seed);
        if (caf::holds_alternative<null_type>(seed_series->type)) {
          seed_series = std::nullopt;
        }
      }
      auto src_ips = src_ip_series.as<ip_type>();
      if (not src_ips) {
        diagnostic::warning("`community_id` expected `ip` as 1st argument")
          .primary(args.src_ip)
          .emit(ctx);
        return null_series();
      }
      auto dst_ips = dst_ip_series.as<ip_type>();
      if (not dst_ips) {
        diagnostic::warning("`community_id` expected `ip` as 2nd argument")
          .primary(args.dst_ip)
          .emit(ctx);
        return null_series();
      }
      auto protos = proto_series.as<string_type>();
      if (not protos) {
        diagnostic::warning("`community_id` expected `string` as 3rd argument")
          .primary(args.proto)
          .emit(ctx);
        return null_series();
      }
      auto src_ports = std::optional<basic_series<int64_type>>{};
      auto dst_ports = std::optional<basic_series<int64_type>>{};
      if (src_port_series) {
        TENZIR_ASSERT(dst_port_series.has_value());
        src_ports = src_port_series->as<int64_type>();
        if (not src_ports) {
          diagnostic::warning("`community_id` got an argument type mismatch")
            .primary(*args.src_port)
            .note("expected argument `src_port` to be of type `int64`")
            .emit(ctx);
          return null_series();
        }
        dst_ports = dst_port_series->as<int64_type>();
        if (not dst_ports) {
          diagnostic::warning("`community_id` got an argument type mismatch")
            .primary(*args.dst_port)
            .note("expected argument `dst_port` to be of type `int64`")
            .emit(ctx);
          return null_series();
        }
      }
      auto seeds = std::optional<basic_series<int64_type>>{};
      if (seed_series) {
        seeds = seed_series->as<int64_type>();
        if (not seeds) {
          diagnostic::warning("`community_id` got an argument type mismatch")
            .primary(*args.seed)
            .note("expected argument `seed` to be of type `int64`")
            .emit(ctx);
        }
      }
      auto b = arrow::StringBuilder{};
      check(b.Reserve(eval.length()));
      for (auto i = int64_t{0}; i < eval.length(); ++i) {
        if (src_ips->array->IsNull(i) or dst_ips->array->IsNull(i)
            or protos->array->IsNull(i)) {
          check(b.AppendNull());
          continue;
        }
        const auto* src_ip_ptr = src_ips->array->storage()->GetValue(i);
        const auto* dst_ip_ptr = dst_ips->array->storage()->GetValue(i);
        auto src_ip = ip::v6(as_bytes<16>(src_ip_ptr));
        auto dst_ip = ip::v6(as_bytes<16>(dst_ip_ptr));
        auto proto = protos->array->GetView(i);
        auto proto_type = port_type::unknown;
        if (proto == "tcp") {
          proto_type = port_type::tcp;
        } else if (proto == "udp") {
          proto_type = port_type::udp;
        } else if (proto == "icmp") {
          proto_type = port_type::icmp;
        } else if (proto == "icmp6") {
          proto_type = port_type::icmp6;
        } else {
          diagnostic::warning("`community_id` expected `tcp`, `udp`, `icmp`, "
                              "or `icmp6` as protocol")
            .primary(args.proto)
            .emit(ctx);
          check(b.AppendNull());
          continue;
        }
        auto seed = uint16_t{0};
        if (seeds and not seeds->array->IsNull(i)) {
          // TODO: Perform a bounds check. There are probably already utilities
          // for this available.
          seed = detail::narrow_cast<uint16_t>(seeds->array->GetView(i));
        }
        if (src_ports) {
          if (src_ports->array->IsNull(i) or dst_ports->array->IsNull(i)) {
            check(b.AppendNull());
            continue;
          }
          auto src_port = src_ports->array->GetView(i);
          auto dst_port = dst_ports->array->GetView(i);
          auto flow
            = make_flow(src_ip, dst_ip, detail::narrow<uint16_t>(src_port),
                        detail::narrow<uint16_t>(dst_port), proto_type);
          check(b.Append(tenzir::community_id::make(flow, seed)));
        } else {
          check(b.Append(
            tenzir::community_id::make(src_ip, dst_ip, proto_type, seed)));
        }
      }
      return series{string_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::community_id

TENZIR_REGISTER_PLUGIN(tenzir::plugins::community_id::plugin)
