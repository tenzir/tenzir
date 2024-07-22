//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/ip.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/ip.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::ip {

namespace {

class ip final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.ip";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("ip").add(expr, "<string>").parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            return series::null(ip_type{}, arg.length());
          },
          [](const arrow::StringArray& arg) {
            auto b = ip_type::make_arrow_builder(arrow::default_memory_pool());
            check(b->Reserve(arg.length()));
            for (auto i = 0; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b->AppendNull());
                continue;
              }
              auto result = tenzir::ip{};
              if (parsers::ip(arg.GetView(i), result)) {
                check(append_builder(ip_type{}, *b, result));
              } else {
                // TODO: ?
                check(b->AppendNull());
              }
            }
            return series{ip_type{}, check(b->Finish())};
          },
          [&](const ip_type::array_type&) {
            return arg;
          },
          [&](const auto&) {
            diagnostic::warning("`ip` expected `string`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(ip_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::ip

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ip::ip)
