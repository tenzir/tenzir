//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto docs = "https://docs.tenzir.com/functions/int";
    auto arg = located<series>{};
    auto success
      = function_argument_parser{docs}.add(arg, "<array>").parse(inv, dh);
    if (not success) {
      return series::null(int64_type{}, inv.length);
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::StringArray& arg) {
        auto report = false;
        auto b = arrow::Int64Builder{};
        check(b.Reserve(inv.length));
        for (auto row = int64_t{0}; row < inv.length; ++row) {
          if (arg.IsNull(row)) {
            // TODO: Do we want to report this? Probably not.
            check(b.AppendNull());
          } else {
            auto result = int64_t{};
            if (parsers::integer(arg.GetView(row), result)) {
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
            .primary(inv.self.get_location())
            .emit(dh);
        }
        return finish(b);
      },
      [&](const auto&) -> std::shared_ptr<arrow::Int64Array> {
        diagnostic::warning("`int` currently expects `int64` or `string`, got "
                            "`{}`",
                            arg.inner.type.kind())
          .primary(arg.source)
          .emit(dh);
        auto b = arrow::Int64Builder{};
        check(b.AppendNulls(detail::narrow<int64_t>(inv.length)));
        return finish(b);
      },
    };
    return series{int64_type{}, caf::visit(f, *arg.inner.array)};
  }
};

} // namespace

} // namespace tenzir::plugins::int_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_)
