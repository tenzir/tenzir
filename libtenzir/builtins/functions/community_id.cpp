//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/community_id.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/flow.hpp>
#include <tenzir/tql2/plugin.hpp>

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

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto args = arguments{};
    TRY(argument_parser2::function("community_id")
          .named("src_ip", args.src_ip, "ip")
          .named("dst_ip", args.dst_ip, "ip")
          .named("src_port", args.src_port, "int")
          .named("dst_port", args.dst_port, "int")
          .named("proto", args.proto, "string")
          .named("seed", args.seed, "int")
          .parse(inv, ctx));
    return function_use::make([args
                               = std::move(args)](evaluator eval, session ctx) {
      auto string_nulls = series::null(string_type{}, eval.length());
      auto null_nulls = series::null(null_type{}, eval.length());
      auto emit_proto_warning = false;
      auto emit_port_conflict_warning = false;
      auto emit_src_port_range_warning = false;
      auto emit_dst_port_range_warning = false;
      auto emit_seed_warning = false;
      auto b = arrow::StringBuilder{};
      check(b.Reserve(eval.length()));
      for (auto [src_ip_series, dst_ip_series, proto_series, src_port_series,
                 dst_port_series, seed_series] :
           split_multi_series(eval(args.src_ip), eval(args.dst_ip),
                              eval(args.proto),
                              args.src_port ? eval(*args.src_port) : null_nulls,
                              args.dst_port ? eval(*args.dst_port) : null_nulls,
                              args.seed ? eval(*args.seed) : null_nulls)) {
        auto length = src_ip_series.length();
        auto append_nulls = [&] {
          check(b.AppendNulls(length));
        };
        if (is<null_type>(src_ip_series.type)) {
          append_nulls();
          continue;
        }
        if (is<null_type>(dst_ip_series.type)) {
          append_nulls();
          continue;
        }
        if (is<null_type>(proto_series.type)) {
          append_nulls();
          continue;
        }
        auto src_ips = src_ip_series.as<ip_type>();
        if (not src_ips) {
          diagnostic::warning("expected argument of type `ip`, but got "
                              "`{}`",
                              src_ip_series.type.kind())
            .primary(args.src_ip)
            .emit(ctx);
          append_nulls();
          continue;
        }
        auto dst_ips = dst_ip_series.as<ip_type>();
        if (not dst_ips) {
          diagnostic::warning("expected argument of type `ip`, but got "
                              "`{}`",
                              dst_ip_series.type.kind())
            .primary(args.dst_ip)
            .emit(ctx);
          append_nulls();
          continue;
        }
        auto protos = proto_series.as<string_type>();
        if (not protos) {
          diagnostic::warning("expected argument of type `string`, but "
                              "got `{}`",
                              proto_series.type.kind())
            .primary(args.proto)
            .emit(ctx);
          append_nulls();
          continue;
        }
        auto src_ports = std::optional<basic_series<int64_type>>{};
        auto dst_ports = std::optional<basic_series<int64_type>>{};
        if (src_port_series.type.kind().is_not<null_type>()) {
          src_ports = src_port_series.as<int64_type>();
          if (not src_ports) {
            diagnostic::warning("expected argument of type `int64`, but "
                                "got `{}`",
                                src_port_series.type.kind())
              .primary(*args.src_port)
              .emit(ctx);
            append_nulls();
            continue;
          }
        }
        if (dst_port_series.type.kind().is_not<null_type>()) {
          dst_ports = dst_port_series.as<int64_type>();
          if (not dst_ports) {
            diagnostic::warning("expected argument of type `int64`, but "
                                "got `{}`",
                                dst_port_series.type.kind())
              .primary(*args.dst_port)
              .emit(ctx);
            append_nulls();
            continue;
          }
        }
        auto seeds = std::optional<basic_series<int64_type>>{};
        if (seed_series.type.kind().is_not<null_type>()) {
          seeds = seed_series.as<int64_type>();
          if (not seeds) {
            diagnostic::warning("expected argument of type `int64`, but "
                                "got `{}`",
                                seed_series.type.kind())
              .primary(*args.seed)
              .emit(ctx);
            append_nulls();
            continue;
          }
        }
        for (auto i = int64_t{0}; i < length; ++i) {
          if (src_ips->array->IsNull(i) or dst_ips->array->IsNull(i)
              or protos->array->IsNull(i)) {
            check(b.AppendNull());
            continue;
          }
          const auto* src_ip_ptr = src_ips->array->storage()->GetValue(i);
          const auto* dst_ip_ptr = dst_ips->array->storage()->GetValue(i);
          auto src_ip = ip::v6(as_bytes<16>(src_ip_ptr, 16));
          auto dst_ip = ip::v6(as_bytes<16>(dst_ip_ptr, 16));
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
            emit_proto_warning = true;
            check(b.AppendNull());
            continue;
          }
          auto seed = uint16_t{0};
          if (seeds and not seeds->array->IsNull(i)) {
            auto value = seeds->array->GetView(i);
            if (value < 0 or value > 65'535) {
              emit_seed_warning = true;
              check(b.AppendNull());
              continue;
            }
            seed = detail::narrow_cast<uint16_t>(value);
          }
          auto have_src_port = src_ports and not src_ports->array->IsNull(i);
          auto have_dst_port = dst_ports and not dst_ports->array->IsNull(i);
          if (have_src_port and have_dst_port) {
            auto src_port = src_ports->array->GetView(i);
            // TODO: create an abstraction that bakes this check into the
            // narrowing operation below.
            if (src_port < 0 or src_port > 65'535) {
              emit_src_port_range_warning = true;
              check(b.AppendNull());
              continue;
            }
            auto dst_port = dst_ports->array->GetView(i);
            if (dst_port < 0 or dst_port > 65'535) {
              emit_dst_port_range_warning = true;
              check(b.AppendNull());
              continue;
            }
            auto flow
              = make_flow(src_ip, dst_ip,
                          detail::narrow_cast<uint16_t>(src_port),
                          detail::narrow_cast<uint16_t>(dst_port), proto_type);
            check(b.Append(tenzir::community_id::make(flow, seed)));
          } else if (have_src_port != have_dst_port) {
            emit_port_conflict_warning = true;
            check(b.AppendNull());
            continue;
          } else {
            check(b.Append(
              tenzir::community_id::make(src_ip, dst_ip, proto_type, seed)));
          }
        }
      }
      if (emit_seed_warning) {
        diagnostic::warning("`seed` must be between 0 and 65535")
          .primary(*args.seed)
          .emit(ctx);
      }
      if (emit_port_conflict_warning) {
        auto d = diagnostic::warning(
          "encountered only `src_port` or `dst_port` but not both");
        if (args.src_port) {
          d = std::move(d).primary(*args.src_port);
        }
        if (args.dst_port) {
          d = std::move(d).primary(*args.dst_port);
        }
        std::move(d).emit(ctx);
      }
      if (emit_src_port_range_warning) {
        diagnostic::warning("`src_port` must be between 0 and 65535")
          .primary(*args.src_port)
          .emit(ctx);
      }
      if (emit_dst_port_range_warning) {
        diagnostic::warning("`dst_port` must be between 0 and 65535")
          .primary(*args.dst_port)
          .emit(ctx);
      }
      if (emit_proto_warning) {
        diagnostic::warning("`proto` must be `tcp`, `udp`, `icmp`, or `icmp6`")
          .primary(args.proto)
          .emit(ctx);
      }
      return series{string_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::community_id

TENZIR_REGISTER_PLUGIN(tenzir::plugins::community_id::plugin)
