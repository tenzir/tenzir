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

namespace tenzir::plugins::round {

namespace {

using namespace tql2;

class plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.round";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    if (args.size() != 1) {
      diagnostic::error("`round` expects exactly one argument")
        .primary(self.get_location())
        .emit(dh);
    }
    if (args.empty()) {
      auto b = arrow::Int64Builder{};
      (void)b.AppendNulls(detail::narrow<int64_t>(length));
      return series{int64_type{}, b.Finish().ValueOrDie()};
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::DoubleArray& arg) {
        auto b = arrow::Int64Builder{};
        for (auto row = int64_t{0}; row < detail::narrow<int64_t>(length);
             ++row) {
          if (arg.IsNull(row)) {
            (void)b.AppendNull();
          } else {
            // TODO: NaN, inf, ...
            auto result = std::llround(arg.Value(0));
            (void)b.Append(result);
          }
        }
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
      [&](const auto& arg) -> std::shared_ptr<arrow::Int64Array> {
        TENZIR_UNUSED(arg);
        diagnostic::warning("`round` expects `int64` or `double`, got `{}`",
                            args[0].type.kind())
          // TODO: Wrong location.
          .primary(self.get_location())
          .emit(dh);
        auto b = arrow::Int64Builder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(length));
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{int64_type{}, caf::visit(f, *args[0].array)};
  }
};

} // namespace

} // namespace tenzir::plugins::round

TENZIR_REGISTER_PLUGIN(tenzir::plugins::round::plugin)
