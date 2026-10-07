//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/flatten.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unflatten {

namespace {

struct UnflattenArgs {
  nova::ValueArgument x;
  Option<located<std::string>> separator;
};

class UnflattenFunction {
public:
  static auto eval(UnflattenArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto const separator = args.separator
                             ? std::string_view{args.separator->inner}
                             : std::string_view{"."};
    return nova::unflatten(args.x.data, frame.mask(), separator);
  }
};

class plugin final : public virtual nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "unflatten";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<UnflattenArgs, UnflattenFunction>{};
    d.positional("x", &UnflattenArgs::x, "any");
    d.positional("separator", &UnflattenArgs::separator);
    d.validate(
      [](UnflattenArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        if (args.separator and args.separator->inner.empty()) {
          diagnostic::error("`separator` must not be empty")
            .primary(*args.separator)
            .emit(dh);
          return failure::promise();
        }
        return {};
      });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto sep = Option<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .positional("separator", sep)
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), sep = std::move(sep)](evaluator eval, session) {
        return map_series(eval(expr), [&](series s) {
          auto unflattened = tenzir::unflatten(s.array, sep.value_or("."));
          auto schema = type::from_arrow(*unflattened->type());
          return series{type{s.type.name(), schema}, unflattened};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::unflatten

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unflatten::plugin)
