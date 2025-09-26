//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view.hpp"

#include <arrow/array/data.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/type_fwd.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/bitmap_ops.h>

namespace tenzir::plugins::search {

namespace {

auto comparable(const type& x, const type& y) -> bool {
  return match(std::tie(x, y), []<typename X, typename Y>(const X&, const Y&) {
    return std::same_as<X, Y> or concepts::one_of<null_type, X, Y>
           or (concepts::number<type_to_data_t<X>>
               and concepts::number<type_to_data_t<Y>>);
  });
}

auto equals(const data_view& l, const data& r) -> bool {
  return match(
    std::tie(l, r),
    [](const concepts::integer auto& x, const concepts::integer auto& y) {
      return std::cmp_equal(x, y);
    },
    [](const concepts::number auto& x, const concepts::number auto& y) {
      return x == y;
    },
    [&](const auto&, const auto&) {
      return l == r;
    });
}

auto search_in_data(const data_view& val, const type& val_type,
                    const type& what_type, const data& what) -> bool {
  if (comparable(val_type, what_type) and equals(val, what)) {
    return true;
  }
  return match(
    std::tie(val, val_type),
    [&](const view<record>& rec, const record_type& rec_type) -> bool {
      for (const auto& [key, value] : rec) {
        if (auto field_type = rec_type.field(key)) {
          if (search_in_data(value, *field_type, what_type, what)) {
            return true;
          }
        }
      }
      return false;
    },
    [&](const view<list>& lst, const list_type& lst_type) -> bool {
      for (const auto& value : lst) {
        if (search_in_data(value, lst_type.value_type(), what_type, what)) {
          return true;
        }
      }
      return false;
    },
    [&](const auto&, const auto&) -> bool {
      return false;
    });
}

auto search_in_series(const series& input, const type& what_type,
                      const data& what) -> series {
  auto b = bool_type::make_arrow_builder(arrow::default_memory_pool());
  for (const auto& val : input.values()) {
    auto found = search_in_data(val, input.type, what_type, what);
    check(b->Append(found));
  }
  return series{bool_type{}, finish(*b)};
}

class search final : public function_plugin {
  auto is_deterministic() const -> bool override {
    return true;
  }

  auto name() const -> std::string override {
    return "search";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto in = ast::expression{};
    auto what = data{};
    TRY(argument_parser2::function(name())
          .positional("in", in, "any")
          .positional("what", what)
          .parse(inv, ctx));
    return function_use::make([in = std::move(in), what = std::move(what)](
                                evaluator eval, session ctx) -> multi_series {
      TENZIR_UNUSED(ctx);
      const auto what_type = type::infer(what).value();
      return map_series(eval(in), [what_type, what](const series& s) -> series {
        return search_in_series(s, what_type, what);
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::search

TENZIR_REGISTER_PLUGIN(tenzir::plugins::search::search)
