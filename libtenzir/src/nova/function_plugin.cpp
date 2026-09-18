//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/function_plugin.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/secret.hpp"
#include "tenzir/try.hpp"
#include "tenzir/type.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <ranges>
#include <span>
#include <utility>

#include "diagnostics.hpp"

namespace tenzir::nova {

namespace {

auto is_hidden(std::string_view name) -> bool {
  return name.starts_with('_');
}

/// The names of a named argument that appear in usage strings, joined with
/// `|`. Empty if all names are hidden.
auto display_names(std::span<const std::string> names) -> std::string {
  auto visible = names | std::views::filter([](const auto& name) {
                   return not is_hidden(name);
                 });
  return fmt::format("{}", fmt::join(visible, "|"));
}

/// Builds the "named argument does not exist" diagnostic, suggesting the
/// closest visible name if there is a reasonably similar one.
auto unknown_named_argument(std::string_view name, const ast::expression& at,
                            std::span<const std::string_view> candidates)
  -> diagnostic_builder {
  auto result
    = diagnostic::error("named argument `{}` does not exist", name).primary(at);
  if (not candidates.empty()) {
    const auto best = std::ranges::max(
      candidates | std::views::transform([&](auto x) {
        return std::make_pair(detail::calculate_similarity(name, x), x);
      }),
      [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
      });
    if (best.first > -10) {
      result = std::move(result).hint("did you mean `{}`?", best.second);
    }
  }
  return result;
}

} // namespace

auto _::default_type_name(type_kind kind) -> std::string {
  return match(
    kind,
    [](tag<string_type>) -> std::string {
      return "string";
    },
    [](tag<secret_type>) -> std::string {
      return "string";
    },
    [](tag<int64_type>) -> std::string {
      return "int";
    },
    [](tag<uint64_type>) -> std::string {
      return "int";
    },
    [](tag<double_type>) -> std::string {
      return "number";
    },
    [](tag<bool_type>) -> std::string {
      return "bool";
    },
    [&](auto) -> std::string {
      return fmt::format("{}", kind);
    });
}

auto _::convert_constant(located<data> constant, type_kind expected,
                         InstantiateCtx ctx) -> failure_or<located<data>> {
  auto& value = constant.inner;
  if (expected.is<uint64_type>()) {
    if (auto* signed_value = try_as<int64_t>(&value)) {
      if (*signed_value < 0) {
        diagnostic::error("expected positive integer, got `{}`", *signed_value)
          .primary(constant.source)
          .emit(ctx);
        return failure::promise();
      }
      value = static_cast<uint64_t>(*signed_value);
    }
  }
  if (expected.is<secret_type>()) {
    if (auto* str = try_as<std::string>(&value)) {
      value = secret::make_literal(*str);
    }
  }
  const auto actual = type_kind_of_data(value);
  if (actual != expected) {
    diagnostic::error("expected argument of type `{}`, but got `{}`", expected,
                      actual)
      .primary(constant.source)
      .emit(ctx);
    return failure::promise();
  }
  return constant;
}

auto FunctionDescription::usage(std::string_view name) const -> std::string {
  auto result = std::string{};
  auto has_previous = false;
  auto in_brackets = false;
  const auto separate = [&] {
    if (std::exchange(has_previous, true)) {
      result += ", ";
    }
  };
  for (auto [idx, arg] : detail::enumerate(positional_)) {
    if (is_hidden(arg.name)) {
      continue;
    }
    separate();
    if (first_optional_ and idx >= *first_optional_ and not in_brackets) {
      result += '[';
      in_brackets = true;
    }
    result += fmt::format("{}:{}", arg.name, arg.type);
  }
  // Required named arguments come first, optional ones share the brackets.
  for (auto required : {true, false}) {
    for (const auto& arg : named_) {
      if (arg.required != required) {
        continue;
      }
      auto names = display_names(arg.names);
      if (names.empty()) {
        continue;
      }
      if (required and in_brackets) {
        result += ']';
        in_brackets = false;
      }
      separate();
      if (not required and not in_brackets) {
        result += '[';
        in_brackets = true;
      }
      result += fmt::format("{}={}", names, arg.type);
    }
  }
  if (in_brackets) {
    result += ']';
  }
  return fmt::format("{}({})", name, result);
}

auto FunctionDescription::instantiate(std::string_view name,
                                      ast::function_call& call,
                                      InstantiateCtx ctx) const
  -> failure_or<Instantiation> {
  // All diagnostics below carry the usage and documentation of this function,
  // including those of nested constant evaluation.
  auto docs
    = fmt::format("https://tenzir.com/docs/reference/functions/{}", name);
  auto diagnostics = _::DiagnosticScope{ctx, usage(name), std::move(docs)};
  ctx = InstantiateCtx{diagnostics.handler(), ctx};
  // Sort the call's arguments into positional and named ones, checking the
  // shape of the call but not yet looking at the argument values.
  auto positional = std::vector<ast::expression*>{};
  auto named = std::vector<std::pair<size_t, ast::expression*>>{};
  auto named_found = std::vector<Option<location>>(named_.size());
  auto positional_idx = size_t{0};
  const auto min_positional = first_optional_.value_or(positional_.size());
  const auto max_positional = positional_.size();
  for (auto& arg : call.args) {
    if (auto* assignment = try_as<ast::assignment>(&arg)) {
      auto selector = ast::selector::try_from(assignment->left);
      auto const* sel
        = selector ? try_as<ast::field_path>(&*selector) : nullptr;
      if (not sel or sel->has_this() or sel->path().size() != 1
          or sel->path()[0].has_question_mark) {
        diagnostic::error("invalid argument name")
          .primary(assignment->left)
          .emit(ctx);
        continue;
      }
      const auto& arg_name = sel->path()[0].id.name;
      auto it = std::ranges::find_if(named_, [&](const Named& candidate) {
        return std::ranges::contains(candidate.names, arg_name);
      });
      if (it == named_.end()) {
        auto candidates = std::vector<std::string_view>{};
        for (const auto& candidate : named_) {
          std::ranges::copy_if(candidate.names, std::back_inserter(candidates),
                               [](const auto& x) {
                                 return not is_hidden(x);
                               });
        }
        unknown_named_argument(arg_name, assignment->left, candidates).emit(ctx);
        continue;
      }
      const auto idx = static_cast<size_t>(it - named_.begin());
      if (named_found[idx]) {
        diagnostic::error("duplicate named argument `{}`", arg_name)
          .primary(*named_found[idx])
          .primary(assignment->left)
          .emit(ctx);
        continue;
      }
      named_found[idx] = assignment->left.get_location();
      named.emplace_back(idx, &assignment->right);
      continue;
    }
    if (is<ast::pipeline_expr>(arg)) {
      diagnostic::error("functions do not take pipeline arguments")
        .primary(arg)
        .emit(ctx);
      continue;
    }
    const auto is_variadic
      = variadic_index_ and positional_idx == *variadic_index_;
    if (positional_idx >= max_positional and not is_variadic) {
      diagnostic::error("too many positional arguments").primary(arg).emit(ctx);
      continue;
    }
    positional.push_back(&arg);
    // Everything from the variadic position onwards belongs to it.
    if (not is_variadic) {
      ++positional_idx;
    }
  }
  const auto variadic_required
    = variadic_index_ and *variadic_index_ < min_positional;
  const auto too_few = variadic_required ? positional.size() < min_positional
                                         : positional_idx < min_positional;
  if (too_few) {
    const auto* specifier
      = min_positional == max_positional and not variadic_index_ ? "exactly"
                                                                 : "at least";
    diagnostic::error("expected {} {} positional argument{}", specifier,
                      min_positional, min_positional == 1 ? "" : "s")
      .primary(call)
      .emit(ctx);
  }
  for (auto [idx, arg] : detail::enumerate(named_)) {
    if (arg.required and not named_found[idx]) {
      diagnostic::error("required argument `{}` was not provided",
                        arg.names.front())
        .primary(call)
        .emit(ctx);
    }
  }
  if (diagnostics.failed()) {
    return failure::promise();
  }
  // Now look at the values. Each argument knows how to prepare itself. Like
  // the shape check above, this reports every bad argument instead of
  // stopping at the first one.
  auto args = make_args_();
  auto slots = std::vector<_::ValueSlot>{};
  auto deferred = std::vector<ast::expression*>{};
  auto sink = PrepareSink{args, slots, deferred};
  auto prepared = true;
  for (auto [idx, expr] : detail::enumerate(positional)) {
    const auto pos_idx
      = variadic_index_ and idx >= *variadic_index_ ? *variadic_index_ : idx;
    TENZIR_ASSERT(pos_idx < positional_.size());
    prepared &= positional_[pos_idx].prepare(sink, *expr, ctx).is_success();
  }
  for (const auto& [idx, expr] : named) {
    TENZIR_ASSERT(idx < named_.size());
    prepared &= named_[idx].prepare(sink, *expr, ctx).is_success();
  }
  // A failed preparation leaves its member default constructed, and a failed
  // variadic element leaves a gap in the vector, so neither the validator nor
  // the function may observe these arguments.
  if (not prepared or diagnostics.failed()) {
    return failure::promise();
  }
  if (set_call_location_) {
    (*set_call_location_)(args, call.get_location());
  }
  if (validator_) {
    TRY((*validator_)(args, ctx));
  }
  if (diagnostics.failed()) {
    return failure::promise();
  }
  return Instantiation{
    _::CallSite{std::move(args), std::move(slots), kernel_},
    std::move(deferred),
  };
}

auto FunctionPlugin::instantiate(ast::function_call& call,
                                 InstantiateCtx ctx) const
  -> failure_or<FunctionDescription::Instantiation> {
  return describe().instantiate(function_name(), call, ctx);
}

} // namespace tenzir::nova
