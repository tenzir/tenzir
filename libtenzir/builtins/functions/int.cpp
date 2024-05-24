//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::int_ {

namespace {

using namespace tql2;

class int_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.int";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    if (inv.args.size() != 1) {
      diagnostic::error("`int` expects exactly one argument")
        .primary(inv.self.get_location())
        .emit(dh);
      return series::null(int64_type{}, inv.length);
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::StringArray& arg) {
        auto report = false;
        auto b = arrow::Int64Builder{};
        (void)b.Reserve(arg.length());
        for (auto row = int64_t{0}; row < detail::narrow<int64_t>(inv.length);
             ++row) {
          if (arg.IsNull(row)) {
            // TODO: Do we want to report this?
            (void)b.UnsafeAppendNull();
          } else {
            auto result = int64_t{};
            if (parsers::integer(arg.GetView(row), result)) {
              (void)b.UnsafeAppend(result);
            } else {
              (void)b.UnsafeAppendNull();
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
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
      [&](const auto& arg) -> std::shared_ptr<arrow::Int64Array> {
        TENZIR_UNUSED(arg);
        diagnostic::warning("`int` currently expects `int64` or `string`, got "
                            "`{}`",
                            inv.args[0].type.kind())
          // TODO: Wrong location.
          .primary(inv.self.get_location())
          .emit(dh);
        auto b = arrow::Int64Builder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(inv.length));
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{int64_type{}, caf::visit(f, *inv.args[0].array)};
  }
};

} // namespace

} // namespace tenzir::plugins::int_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::int_::int_)
