//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline.hpp"

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/flatten.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/record_util.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::flatten {

namespace {

constexpr auto default_flatten_separator = ".";

struct FlattenArgs {
  nova::ValueArgument x;
  std::string separator = default_flatten_separator;
};

class FlattenFunction {
public:
  static auto eval(FlattenArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto records = nova::resolve_record(args.x, frame);
    if (not records) {
      return frame.null();
    }
    auto flattened = nova::flatten(std::move(records->data), records->present,
                                   args.separator);
    if (not flattened.renamed_fields.empty()) {
      diagnostic::warning("renamed fields with conflicting names after "
                          "flattening: {}",
                          fmt::join(flattened.renamed_fields, ", "))
        .primary(args.x.source)
        .emit(frame);
    }
    return nova::Array<nova::Data>{std::move(flattened.data)}.null_where(
      frame.mask().and_not(records->present));
  }
};

class plugin final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "flatten";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<FlattenArgs, FlattenFunction>{};
    d.positional("x", &FlattenArgs::x, "record");
    d.optional_positional("separator", &FlattenArgs::separator);
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto sep = Option<std::string>{default_flatten_separator};
    TRY(argument_parser2::function("flatten")
          .positional("x", expr, "record")
          .positional("separator", sep)
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), sep = std::move(sep.value())](
        evaluator eval, session ctx) -> multi_series {
        return map_series(eval(expr), [&](series s) {
          auto ptr = std::dynamic_pointer_cast<arrow::StructArray>(s.array);
          if (not ptr) {
            diagnostic::warning("expected `record`, got `{}`", s.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, s.length());
          }
          auto flattened = tenzir::flatten(s.type, ptr, sep);
          if (not flattened.renamed_fields.empty()) {
            diagnostic::warning("renamed fields with conflicting names after "
                                "flattening: {}",
                                fmt::join(flattened.renamed_fields, ", "))
              .primary(expr)
              .emit(ctx);
          }
          return series{flattened.schema, flattened.array};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::flatten

TENZIR_REGISTER_PLUGIN(tenzir::plugins::flatten::plugin)
