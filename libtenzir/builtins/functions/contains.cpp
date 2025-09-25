//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view.hpp"

#include <arrow/array/builder_primitive.h>
#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

#include <functional>
#include <utility>

namespace tenzir::plugins::contains {

namespace {

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

auto equals(const data_view& l, const data& r, bool exact) -> bool {
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
              bool exact, std::vector<bool>& b) -> void {
  TENZIR_ASSERT(std::cmp_equal(input.length(), b.size()));
  if (comparable(input.type, what_type)) {
    for (const auto& [i, val] : detail::enumerate(input.values())) {
      b[i] = b[i] or equals(val, what, exact);
    }
    return;
  }
  if (const auto rs = input.as<record_type>()) {
    for (const auto& field : rs->fields()) {
      contains(field.data, what_type, what, exact, b);
    }
    return;
  }
  if (const auto ls = input.as<list_type>()) {
    auto b_ = std::vector<bool>{};
    b_.resize(ls->array->values()->length());
    contains({ls->type.value_type(), ls->array->values()}, what_type, what,
             exact, b_);
    auto curr = b_.begin();
    for (auto i = int64_t{}; i < ls->length(); ++i) {
      auto begin = curr;
      std::advance(curr, ls->array->value_length(i));
      b[i] = std::ranges::any_of(begin, curr, std::identity{});
    }
  }
}

class plugin final : public function_plugin {
  auto is_deterministic() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "contains";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto target = located<data>{};
    auto exact = false;
    TRY(argument_parser2::function(name())
          .positional("input", input, "any")
          .positional("target", target)
          .named_optional("exact", exact)
          .parse(inv, ctx));
    if (is<record>(target.inner) or is<list>(target.inner)) {
      diagnostic::error("`target` cannot be a list or a record")
        .primary(target)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([in = std::move(input),
                               what = std::move(target.inner),
                               exact](evaluator eval, session) -> multi_series {
      const auto what_type = type::infer(what).value();
      auto b = arrow::BooleanBuilder{};
      check(b.Reserve(eval.length()));
      auto result = std::vector<bool>{};
      for (const auto& s : eval(in)) {
        result.resize(detail::narrow<size_t>(s.length()));
        contains(s, what_type, what, exact, result);
        check(b.AppendValues(result));
        result.clear();
      }
      return series{bool_type{}, finish(b)};
    });
  }
};

} // namespace

} // namespace tenzir::plugins::contains

TENZIR_REGISTER_PLUGIN(tenzir::plugins::contains::plugin)
