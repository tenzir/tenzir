//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::otherwise {

namespace {

class otherwise final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "otherwise";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto primary = ast::expression{};
    auto fallback = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("primary", primary, "any")
          .positional("fallback", fallback, "any")
          .parse(inv, ctx));
    return function_use::make(
      [primary = std::move(primary), fallback = std::move(fallback),
       loc = inv.call.get_location()](evaluator eval, session ctx) -> series {
        const auto ps = eval(primary);
        const auto fs = eval(fallback);
        if (ps.type.kind().is<null_type>()) {
          return fs;
        }
        if (fs.type.kind().is<null_type>()) {
          return ps;
        }
        if (ps.type != fs.type) {
          diagnostic::warning("cannot fallback to a different type")
            .primary(fallback,
                     fmt::format("expected `{}`, got `{}`", ps.type, fs.type))
            .secondary(loc, "evaluating to primary expression")
            .emit(ctx);
          return ps;
        }
        auto b = ps.type.make_arrow_builder(arrow::default_memory_pool());
        check(b->Reserve(ps.array->length()));
        for (auto offset = int64_t{}; offset != ps.array->length();) {
          auto count = int64_t{1};
          const auto valid = ps.array->IsValid(offset);
          while (offset + count != ps.array->length()
                 and ps.array->IsValid(offset + count) == valid) {
            ++count;
          }
          check(append_array_slice(*b, ps.type, valid ? *ps.array : *fs.array,
                                   offset, count));
          offset += count;
        }
        return series{ps.type, finish(*b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::otherwise

TENZIR_REGISTER_PLUGIN(tenzir::plugins::otherwise::otherwise)
