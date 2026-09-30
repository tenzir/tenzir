//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/community_id.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/flow.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::community_id {

namespace {

struct arguments {
  ast::expression src_ip;
  ast::expression dst_ip;
  ast::expression proto;
  Option<ast::expression> dst_port;
  Option<ast::expression> src_port;
  Option<ast::expression> seed;
};

struct CommunityIdArgs {
  nova::ValueArgument src_ip;
  nova::ValueArgument dst_ip;
  nova::ValueArgument proto;
  Option<nova::ValueArgument> src_port;
  Option<nova::ValueArgument> dst_port;
  Option<nova::ValueArgument> seed;
};

template <nova::data_type Tag>
auto prepare_argument(nova::ValueArgument const& arg,
                      nova::storage::BitMap& rows, nova::EvalFrame frame,
                      bool nullable = false)
  -> Option<nova::MaskedArray<nova::Array<Tag>>> {
  auto values = arg.data.get_alternative<Tag>();
  auto valid
    = values ? values->present : nova::storage::BitMap{rows.length(), false};
  auto nulls = arg.data.get_alternative<nova::Null>();
  auto accepted = nulls ? valid | nulls->present : valid;
  auto invalid = rows.and_not(accepted);
  if (invalid.any()) {
    auto row = *nova::storage::true_bits(invalid).begin();
    match(arg.data.get(row), [&]<class T>(nova::RowView<T>) {
      diagnostic::warning("expected argument of type `{}`, but got `{}`",
                          nova::Type<Tag>::static_name,
                          nova::Type<T>::static_name)
        .primary(arg.source)
        .emit(frame);
    });
  }
  rows = rows & (nullable ? accepted : valid);
  return values;
}

struct CommunityIdFunction {
  static auto eval(CommunityIdArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto rows = frame.mask();
    auto absent = nova::ValueArgument{frame.null(), location::unknown};
    auto src_ips = prepare_argument<nova::Ip>(args.src_ip, rows, frame);
    auto dst_ips = prepare_argument<nova::Ip>(args.dst_ip, rows, frame);
    auto protos = prepare_argument<nova::String>(args.proto, rows, frame);
    auto src_ports = prepare_argument<nova::Int>(
      args.src_port ? *args.src_port : absent, rows, frame, true);
    auto dst_ports = prepare_argument<nova::Int>(
      args.dst_port ? *args.dst_port : absent, rows, frame, true);
    auto seeds = prepare_argument<nova::Int>(args.seed ? *args.seed : absent,
                                             rows, frame, true);
    auto proto_warning = nova::WarnOnce{};
    auto seed_warning = nova::WarnOnce{};
    auto port_conflict_warning = nova::WarnOnce{};
    auto src_port_warning = nova::WarnOnce{};
    auto dst_port_warning = nova::WarnOnce{};
    auto builder = nova::ArrayBuilder<nova::Data>{};
    for (auto row = nova::storage::Index{0}; row < rows.length(); ++row) {
      auto make = [&]() -> Option<std::string> {
        if (not rows.get(row)) {
          return None{};
        }
        auto proto = *protos->data.get(row);
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
          proto_warning(frame, diagnostic::warning("`proto` must be `tcp`, "
                                                   "`udp`, `icmp`, or `icmp6`")
                                 .primary(args.proto.source));
          return None{};
        }
        auto seed = int64_t{0};
        if (seeds and seeds->present.get(row)) {
          seed = *seeds->data.get(row);
          if (seed < 0 or seed > 65'535) {
            seed_warning(frame, diagnostic::warning("`seed` must be between 0 "
                                                    "and 65535")
                                  .primary(args.seed->source));
            return None{};
          }
        }
        auto have_src = src_ports and src_ports->present.get(row);
        auto have_dst = dst_ports and dst_ports->present.get(row);
        if (have_src != have_dst) {
          auto warning = diagnostic::warning(
            "encountered only `src_port` or `dst_port` but not both");
          if (args.src_port) {
            warning = std::move(warning).primary(args.src_port->source);
          }
          if (args.dst_port) {
            warning = std::move(warning).primary(args.dst_port->source);
          }
          port_conflict_warning(frame, std::move(warning));
          return None{};
        }
        auto src_ip = *src_ips->data.get(row);
        auto dst_ip = *dst_ips->data.get(row);
        auto seed_value = detail::narrow_cast<uint16_t>(seed);
        if (not have_src) {
          return tenzir::community_id::make(src_ip, dst_ip, proto_type,
                                            seed_value);
        }
        auto src_port = *src_ports->data.get(row);
        auto dst_port = *dst_ports->data.get(row);
        if (src_port < 0 or src_port > 65'535) {
          src_port_warning(frame, diagnostic::warning("`src_port` must be "
                                                      "between 0 and 65535")
                                    .primary(args.src_port->source));
          return None{};
        }
        if (dst_port < 0 or dst_port > 65'535) {
          dst_port_warning(frame, diagnostic::warning("`dst_port` must be "
                                                      "between 0 and 65535")
                                    .primary(args.dst_port->source));
          return None{};
        }
        return tenzir::community_id::make(
          make_flow(src_ip, dst_ip, detail::narrow_cast<uint16_t>(src_port),
                    detail::narrow_cast<uint16_t>(dst_port), proto_type),
          seed_value);
      };
      if (auto value = make()) {
        builder.data(*value);
      } else {
        builder.null();
      }
    }
    return builder.finish();
  }
};

class plugin final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<CommunityIdArgs, CommunityIdFunction>{};
    d.named("src_ip", &CommunityIdArgs::src_ip, "ip");
    d.named("dst_ip", &CommunityIdArgs::dst_ip, "ip");
    d.named("proto", &CommunityIdArgs::proto, "string");
    d.named_optional("src_port", &CommunityIdArgs::src_port, "int");
    d.named_optional("dst_port", &CommunityIdArgs::dst_port, "int");
    d.named_optional("seed", &CommunityIdArgs::seed, "int");
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "community_id";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
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
        auto src_ports = Option<basic_series<int64_type>>{};
        auto dst_ports = Option<basic_series<int64_type>>{};
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
        auto seeds = Option<basic_series<int64_type>>{};
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
