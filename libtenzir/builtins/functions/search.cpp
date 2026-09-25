//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/enumerate.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view.hpp"
#include "tenzir/view3.hpp"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include <functional>
#include <utility>

namespace tenzir::plugins::search {

namespace {

/// Returns a copy of `array` with each value replaced by its full Unicode case
/// folding, preserving nulls. Used for case-insensitive comparison.
auto fold_case(const arrow::StringArray& array)
  -> std::shared_ptr<arrow::StringArray> {
  auto b = arrow::StringBuilder{tenzir::arrow_memory_pool()};
  check(b.Reserve(array.length()));
  for (auto i = int64_t{0}; i < array.length(); ++i) {
    if (array.IsNull(i)) {
      check(b.AppendNull());
      continue;
    }
    check(b.Append(detail::utf8_fold_case(array.Value(i))));
  }
  return std::static_pointer_cast<arrow::StringArray>(finish(b));
}

auto comparable(const type& x, const type& y) -> bool {
  return match(std::tie(x, y), []<typename X, typename Y>(const X&, const Y&) {
    return std::same_as<X, Y> or
           // null compares to all
           concepts::one_of<null_type, X, Y> or
           // double with int or uint
           (concepts::number<X> and concepts::number<Y>) or
           // ip with subnet
           (concepts::one_of<ip_type, X, Y>
            and concepts::one_of<subnet_type, X, Y>);
  });
}

auto equals(data_view3 l, const data& r, bool exact) -> bool {
  return match(
    std::tie(l, r),
    [](const concepts::integer auto& x, const concepts::integer auto& y) {
      return std::cmp_equal(x, y);
    },
    [](const concepts::number auto& x, const concepts::number auto& y) {
      return x == y;
    },
    [&](const std::string_view& x, const std::string& y) {
      return exact ? x == y : x.contains(y);
    },
    [&](const view<subnet>& x, const subnet& y) {
      return exact ? x == y : x.contains(y);
    },
    [&](const view<subnet>& x, const ip& y) {
      return not exact and x.contains(y);
    },
    [&](const auto&, const auto&) {
      return l == r;
    });
}

auto contains(const series& input, const type& what_type, const data& what,
              bool exact, bool ignore_case, std::vector<bool>& b) -> void {
  TENZIR_ASSERT(std::cmp_equal(input.length(), b.size()));
  if (comparable(input.type, what_type)) {
    // For case-insensitive string comparison we lower the column once and
    // compare against the already-lowered needle. `what` is lowered by the
    // caller when `ignore_case` is set.
    if (ignore_case) {
      if (auto ss = input.as<string_type>(); ss and is<std::string>(what)) {
        const auto& needle = as<std::string>(what);
        auto lowered = fold_case(*ss->array);
        for (auto i = int64_t{0}; i < lowered->length(); ++i) {
          if (lowered->IsNull(i)) {
            continue;
          }
          auto v = lowered->Value(i);
          b[i] = b[i] or (exact ? v == needle : v.contains(needle));
        }
        return;
      }
    }
    for (const auto& [i, val] : detail::enumerate(input.values())) {
      b[i] = b[i] or equals(val, what, exact);
    }
    return;
  }
  if (const auto rs = input.as<record_type>()) {
    for (const auto& field : rs->fields()) {
      contains(field.data, what_type, what, exact, ignore_case, b);
    }
    return;
  }
  if (const auto ls = input.as<list_type>()) {
    auto b_ = std::vector<bool>{};
    b_.resize(ls->array->values()->length());
    contains({ls->type.value_type(), ls->array->values()}, what_type, what,
             exact, ignore_case, b_);
    auto curr = b_.begin();
    for (auto i = int64_t{}; i < ls->length(); ++i) {
      auto begin = curr;
      std::advance(curr, ls->array->value_length(i));
      b[i] = std::ranges::any_of(begin, curr, std::identity{});
    }
  }
}

auto contains(nova::RowView<nova::Data> input, const data& target, bool exact,
              bool ignore_case) -> bool {
  return match(
    input, [&]<nova::data_type T>(nova::RowView<T> const& value) -> bool {
      if constexpr (std::same_as<T, nova::Record>) {
        if (is<caf::none_t>(target)) {
          return false;
        }
        for (auto const& field : value) {
          if (contains(field.second, target, exact, ignore_case)) {
            return true;
          }
        }
        return false;
      } else if constexpr (std::same_as<T, nova::List>) {
        if (is<caf::none_t>(target)) {
          return false;
        }
        for (auto const& element : value) {
          if (contains(element, target, exact, ignore_case)) {
            return true;
          }
        }
        return false;
      } else if constexpr (std::same_as<T, nova::Null>) {
        return equals(data_view3{caf::none}, target, exact);
      } else {
        if constexpr (std::same_as<T, nova::String>) {
          if (ignore_case and is<std::string>(target)) {
            const auto& needle = as<std::string>(target);
            const auto folded = detail::utf8_fold_case(*value);
            return exact ? folded == needle : folded.contains(needle);
          }
        }
        if constexpr (std::same_as<T, nova::Secret>) {
          // Secrets never expose their value, so they never match.
          return false;
        } else {
          return equals(data_view3{*value}, target, exact);
        }
      }
    });
}

struct SearchArgs {
  nova::ValueArgument input;
  nova::ConstantArgument target_argument;
  /// The target as legacy data, derived from `target_argument` in `validate`.
  data target;
  bool exact = false;
  bool ignore_case = false;
  location call;
};

template <bool Deprecated>
class SearchFunction final {
public:
  static auto eval(SearchArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto result = nova::Results{frame.mask().length()};
    nova::storage::for_each_true(frame.mask(), [&](nova::storage::Index row) {
      result.set<nova::Bool>(row,
                             contains(args.input.data.get(row), args.target,
                                      args.exact, args.ignore_case));
    });
    return std::move(result).finish(frame.mask());
  }
};

template <bool Deprecated = false>
class Plugin final : public nova::FunctionPlugin {
  auto is_deterministic() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return Deprecated ? "contains" : "search";
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<SearchArgs, SearchFunction<Deprecated>>{};
    d.positional("input", &SearchArgs::input, "any");
    d.positional("target", &SearchArgs::target_argument);
    d.named_optional("exact", &SearchArgs::exact);
    d.named_optional("ignore_case", &SearchArgs::ignore_case);
    d.call_location(&SearchArgs::call);
    d.validate(
      [](SearchArgs& args, diagnostic_handler& dh) -> failure_or<void> {
        if constexpr (Deprecated) {
          diagnostic::warning("`contains` is deprecated")
            .primary(args.call)
            .hint("use `search` instead")
            .emit(dh);
        }
        auto const& target = args.target_argument;
        if (is<nova::Record>(target.inner) or is<nova::List>(target.inner)) {
          diagnostic::error("`target` cannot be a list or a record")
            .primary(target)
            .emit(dh);
          return failure::promise();
        }
        if (is<nova::Secret>(target.inner)) {
          diagnostic::error("`target` cannot be a secret")
            .primary(target)
            .emit(dh);
          return failure::promise();
        }
        args.target = nova::materialize_legacy(target.inner);
        if (args.ignore_case and is<std::string>(args.target)) {
          args.target = detail::utf8_fold_case(as<std::string>(args.target));
        }
        return {};
      });
    return std::move(d).finish();
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    if constexpr (Deprecated) {
      diagnostic::warning("`contains` is deprecated")
        .primary(inv.call.get_location())
        .hint("use `search` instead")
        .emit(ctx);
    }
    auto input = ast::expression{};
    auto target = located<data>{};
    auto exact = false;
    auto ignore_case = false;
    TRY(argument_parser2::function(name())
          .positional("input", input, "any")
          .positional("target", target)
          .named_optional("exact", exact)
          .named_optional("ignore_case", ignore_case)
          .parse(inv, ctx));
    if (is<record>(target.inner) or is<list>(target.inner)) {
      diagnostic::error("`target` cannot be a list or a record")
        .primary(target)
        .emit(ctx);
      return failure::promise();
    }
    if (ignore_case and is<std::string>(target.inner)) {
      target.inner = detail::utf8_fold_case(as<std::string>(target.inner));
    }
    return function_use::make(
      [in = std::move(input), what = std::move(target.inner), exact,
       ignore_case](evaluator eval, session) -> multi_series {
        const auto what_type = type::infer(what).value();
        auto b = arrow::BooleanBuilder{tenzir::arrow_memory_pool()};
        check(b.Reserve(eval.length()));
        auto result = std::vector<bool>{};
        for (const auto& s : eval(in)) {
          result.resize(detail::narrow<size_t>(s.length()));
          contains(s, what_type, what, exact, ignore_case, result);
          check(b.AppendValues(result));
          result.clear();
        }
        return series{bool_type{}, finish(b)};
      });
  }
};

} // namespace

} // namespace tenzir::plugins::search

TENZIR_REGISTER_PLUGIN(tenzir::plugins::search::Plugin<>)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::search::Plugin<true>)
