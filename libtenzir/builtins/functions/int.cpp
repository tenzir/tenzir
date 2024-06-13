//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/tql2/arrow_utils.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::int_ {

namespace {

class int_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.int";
  }

  auto make_function(invocation inv, session ctx) const
    -> std::unique_ptr<function_use> override {
    // int(<number>)
    // int(<string>)
    auto expr = ast::expression{};
    argument_parser2::fn("int").add(expr, "<string>").parse(inv, ctx);
    return function_use::make(
      [expr = std::move(expr)](auto eval, session ctx) -> series {
        auto value = eval(expr);
        auto f = detail::overload{
          [](const arrow::NullArray& arg) {
            auto b = arrow::Int64Builder{};
            check(b.AppendNulls(arg.length()));
            return finish(b);
          },
          [](const arrow::Int64Array& arg) {
            return std::make_shared<arrow::Int64Array>(arg.data());
          },
          [&](const arrow::StringArray& arg) {
            auto report = false;
            auto b = arrow::Int64Builder{};
            check(b.Reserve(value.length()));
            for (auto row = int64_t{0}; row < value.length(); ++row) {
              if (arg.IsNull(row)) {
                // TODO: Do we want to report this? Probably not.
                check(b.AppendNull());
              } else {
                auto result = int64_t{};
                auto p = ignore(*parsers::space) >> parsers::integer
                         >> ignore(*parsers::space);
                if (p(arg.GetView(row), result)) {
                  check(b.Append(result));
                } else {
                  check(b.AppendNull());
                  report = true;
                }
              }
            }
            if (report) {
              // TODO: It would be helpful to know what string, but then
              // deduplication doesn't work? Perhaps some unique identifier.
              diagnostic::warning("`int` failed to convert some string")
                .primary(expr)
                .emit(ctx);
            }
            return finish(b);
          },
          [&](const auto&) -> std::shared_ptr<arrow::Int64Array> {
            diagnostic::warning("`int` currently expects `int64` or `string`, "
                                "got `{}`",
                                value.type.kind())
              .primary(expr)
              .emit(ctx);
            auto b = arrow::Int64Builder{};
            check(b.AppendNulls(value.length()));
            return finish(b);
          },
        };
        return series{int64_type{}, caf::visit(f, *value.array)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::int_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_)
