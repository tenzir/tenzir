//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/eval_impl2.hpp"

#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/similarity.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir2/printer.hpp"
#include "tenzir2/type_system/array/access.hpp"
#include "tenzir2/type_system/array/builder.hpp"
#include "tenzir2/type_system/array/fundamental.hpp"
#include "tenzir2/type_system/array/list.hpp"
#include "tenzir2/type_system/array/record.hpp"
#include "tenzir2/type_system/array/string.hpp"
#include "tenzir2/type_system/array/subnet.hpp"

#include <algorithm>
#include <limits>
#include <optional>
#include <ranges>

namespace tenzir2 {

auto evaluator::eval(tenzir::ast::record const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  auto names = std::vector<std::string>{};
  auto arrays = std::vector<array_<data>>{};

  struct SpreadInfo {
    std::size_t col_offset;
    array_<::tenzir2::record> arr;
  };
  auto non_uniform_spreads = std::vector<SpreadInfo>{};
  auto needs_keys = false;

  for (auto const& item : x.items) {
    item.match(
      [&](tenzir::ast::record::field const& f) {
        names.push_back(f.name.name);
        arrays.push_back(eval(f.expr, active));
      },
      [&](tenzir::ast::spread const& s) {
        auto spread_val = eval(s.expr, active);
        access::transform(
          std::move(spread_val),
          [&]<typename T>(array_<T>&& arr) -> array_<data> {
            const auto len = arr.length();
            if constexpr (std::same_as<T, ::tenzir2::record>) {
              if (arr.is_uniform()
                  and len == static_cast<std::ptrdiff_t>(length_)) {
                for (auto k = std::size_t{0}; k < arr.num_fields(); ++k) {
                  names.push_back(std::string{arr.name(k)});
                  arrays.push_back(arr.value_array(k));
                }
              } else {
                needs_keys = true;
                const auto col_offset = names.size();
                for (auto k = std::size_t{0}; k < arr.num_fields(); ++k) {
                  names.push_back(std::string{arr.name(k)});
                  arrays.push_back(arr.value_array(k));
                }
                non_uniform_spreads.push_back({col_offset, std::move(arr)});
              }
            } else {
              tenzir::diagnostic::warning(
                "spread expression must evaluate to `record`")
                .primary(s.expr)
                .emit(ctx_);
            }
            auto nb
              = array_builder_<::tenzir2::null>{memory::default_resource()};
            nb.null(len);
            return nb.finish();
          });
      });
  }

  if (not needs_keys) {
    return array_<::tenzir2::record>{std::move(names), std::move(arrays)};
  }

  // Build per-row key mapping. Uniform columns (field exprs and uniform
  // spreads) always have element index == row index. Non-uniform spread
  // columns use occurrence counting to reconstruct the original key indices.
  using key_t = array_<::tenzir2::record>::key_t;
  using keys_builder_t
    = memory::list_array_builder<memory::array_builder<key_t>,
                                 memory::array<key_t>>;
  auto kb = keys_builder_t{memory::default_resource()};

  struct SpreadHelper {
    const SpreadInfo* sp;
    std::vector<int64_t> occurrence_counts;
  };
  auto helpers = std::vector<SpreadHelper>{};
  helpers.reserve(non_uniform_spreads.size());
  for (auto& sp : non_uniform_spreads) {
    helpers.push_back({&sp, std::vector<int64_t>(sp.arr.num_fields(), 0)});
  }

  auto find_spread = [&](std::size_t col) -> SpreadHelper* {
    for (auto& h : helpers) {
      if (col >= h.sp->col_offset
          && col < h.sp->col_offset + h.sp->arr.num_fields()) {
        return &h;
      }
    }
    return nullptr;
  };

  for (auto i = std::ptrdiff_t{0}; i < length_; ++i) {
    auto list = kb.list();
    auto col = std::size_t{0};
    while (col < names.size()) {
      if (auto* h = find_spread(col)) {
        auto row_view = h->sp->arr.get(i);
        if (row_view.valid()) {
          for (auto const& field_kv : *row_view) {
            const auto fname = field_kv.first;
            auto k = std::size_t{0};
            while (k < h->sp->arr.num_fields()
                   and h->sp->arr.name(k) != fname) {
              ++k;
            }
            list.emplace_back(
              key_t{h->sp->col_offset + k, h->occurrence_counts[k]++});
          }
        }
        col = h->sp->col_offset + h->sp->arr.num_fields();
      } else {
        list.emplace_back(key_t{col, static_cast<int64_t>(i)});
        ++col;
      }
    }
  }

  return array_<::tenzir2::record>{
    std::move(names), std::move(arrays), {}, kb.finish()};
}

auto evaluator::eval(tenzir::ast::list const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_TODO();
}

auto evaluator::eval(tenzir::ast::field_access const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  auto left = eval(x.left, active);
  return access::transform(
    std::move(left), [&]<typename T>(array_<T>&& arr) -> array_<data> {
      if constexpr (std::same_as<T, ::tenzir2::record>) {
        return std::move(arr).field(x.name.name);
      } else {
        auto nb = array_builder_<::tenzir2::null>{memory::default_resource()};
        nb.null(arr.length());
        return nb.finish();
      }
    });
}

auto evaluator::eval(tenzir::ast::function_call const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return not_implemented(x);
}

auto evaluator::eval(tenzir::ast::this_ const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return array_<data>{input_or_throw(x).data_};
}

auto evaluator::eval(tenzir::ast::root_field const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return input_or_throw(x).data_.field(x.id.name);
}

auto evaluator::eval(tenzir::ast::index_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  auto left = eval(x.expr, active);

  // Classify the index: string constant, integer constant, or dynamic.
  const std::string* const_str = nullptr;
  std::optional<int64_t> const_int = std::nullopt;
  std::optional<array_<data>> index_arr = std::nullopt;
  x.index.match(
    [&](const tenzir::ast::constant& c) {
      if (auto* s = tenzir::try_as<std::string>(c.value)) {
        const_str = s;
      } else if (auto* i = tenzir::try_as<int64_t>(c.value)) {
        const_int = *i;
      } else if (auto* u = tenzir::try_as<uint64_t>(c.value)) {
        const_int = static_cast<int64_t>(*u);
      }
    },
    [](const auto&) {});
  if (not const_str and not const_int) {
    index_arr = eval(x.index, active);
  }

  // Helper: add a single data row-view into a builder.
  auto add_to_builder = [](array_builder_<::tenzir2::data>& b,
                           array_row_view_<::tenzir2::data> view) {
    match(view, [&](const auto& v) {
      using V = std::remove_cvref_t<decltype(v)>;
      if constexpr (std::same_as<V, array_row_view_<::tenzir2::null>>) {
        b.null();
      } else if (not v.valid()) {
        b.null();
      } else if constexpr (std::same_as<V, array_row_view_<::tenzir2::record>>
                           or std::same_as<V, array_row_view_<::tenzir2::list>>) {
        TENZIR_TODO(); // structured elements inside list not yet supported
      } else {
        // *v for string rows returns string_view; builder expects std::string.
        using Inner = std::remove_cvref_t<decltype(*v)>;
        if constexpr (std::same_as<Inner, std::string_view>) {
          b.data<std::string>(std::string{*v});
        } else {
          b.data(*v);
        }
      }
    });
  };

  // Helper: extract an integer from a data row-view (for dynamic indexing).
  auto get_int
    = [](array_row_view_<::tenzir2::data> v) -> std::optional<int64_t> {
    return match(v, [](const auto& x) -> std::optional<int64_t> {
      using V = std::remove_cvref_t<decltype(x)>;
      if constexpr (std::same_as<V, array_row_view_<int64_t>>) {
        return x.valid() ? std::optional{*x} : std::nullopt;
      } else if constexpr (std::same_as<V, array_row_view_<uint64_t>>) {
        return x.valid() ? std::optional{static_cast<int64_t>(*x)}
                         : std::nullopt;
      } else {
        return std::nullopt;
      }
    });
  };

  return access::transform(
    std::move(left), [&]<typename T>(array_<T>&& arr) -> array_<data> {
      auto make_null = [&]() -> array_<data> {
        auto nb = array_builder_<::tenzir2::null>{memory::default_resource()};
        nb.null(arr.length());
        return nb.finish();
      };

      if constexpr (std::same_as<T, ::tenzir2::record>) {
        if (const_str) {
          // String key → fast column extraction.
          return std::move(arr).field(*const_str);
        }
        if (const_int) {
          auto N = *const_int;
          if (arr.is_uniform()) {
            // Fast path: all rows share the same schema.
            if (N < 0 or static_cast<std::size_t>(N) >= arr.num_fields()) {
              return make_null();
            }
            return arr.field(arr.name(static_cast<std::size_t>(N)));
          }
          // Non-uniform: iterate row by row.
          auto builder
            = array_builder_<::tenzir2::data>{memory::default_resource()};
          for (auto i = std::ptrdiff_t{0}; i < arr.length(); ++i) {
            auto row = arr.get(i);
            if (not row.valid()) {
              builder.null();
              continue;
            }
            auto found = false;
            auto field_idx = std::ptrdiff_t{0};
            for (auto [name, val] : *row) {
              if (field_idx == N) {
                add_to_builder(builder, val);
                found = true;
                break;
              }
              ++field_idx;
            }
            if (not found) {
              builder.null();
            }
          }
          return builder.finish();
        }
        // Dynamic string-keyed index.
        return std::move(arr).field(*index_arr);
      } else if constexpr (std::same_as<T, ::tenzir2::list>) {
        if (const_str) {
          tenzir::diagnostic::warning("cannot index into `list` using `string`")
            .primary(x.expr)
            .emit(ctx_);
          return make_null();
        }
        auto builder
          = array_builder_<::tenzir2::data>{memory::default_resource()};
        for (auto i = std::ptrdiff_t{0}; i < arr.length(); ++i) {
          auto row = arr.get(i);
          if (not row.valid()) {
            builder.null();
            continue;
          }
          auto len = row.length();
          int64_t idx = 0;
          if (const_int) {
            idx = *const_int;
          } else {
            auto maybe = get_int(index_arr->get(i));
            if (not maybe) {
              builder.null();
              continue;
            }
            idx = *maybe;
          }
          if (idx < 0) {
            idx += len;
          }
          if (idx < 0 or idx >= len) {
            builder.null();
            continue;
          }
          add_to_builder(builder, row.get(idx));
        }
        return builder.finish();
      } else {
        tenzir::diagnostic::warning("expected `record` or `list`")
          .primary(x.expr)
          .emit(ctx_);
        return make_null();
      }
    });
}

auto evaluator::eval(tenzir::ast::meta const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_TODO();
}

auto evaluator::eval(tenzir::ast::assignment const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
}

auto data_to_data2(const ::tenzir::data& d) -> ::tenzir2::data;

auto evaluator::eval(tenzir::ast::constant const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  TENZIR_UNUSED(active);
  return to_array(data_to_data2(x.as_data()));
}

auto evaluator::eval(tenzir::ast::format_expr const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  auto cols = std::vector<variant<std::string, array_<data>>>{};
  cols.reserve(x.segments.size());
  for (auto const& segment : x.segments) {
    segment.match(
      [&](std::string const& text) {
        cols.emplace_back(text);
      },
      [&](tenzir::ast::format_expr::replacement const& replacement) {
        cols.emplace_back(eval(replacement.expr, active));
      });
  }
  auto builder = array_builder_<std::string>{memory::default_resource()};
  for (auto row = std::ptrdiff_t{0}; row < length_; ++row) {
    auto result = std::string{};
    for (auto const& col : cols) {
      match(
        col,
        [&](std::string const& text) {
          result += text;
        },
        [&](array_<data> const& values) {
          result += format_tql(values.get(row), {.compact = true});
        });
    }
    builder.data(result);
  }
  return builder.finish();
}

auto evaluator::eval(tenzir::ast::lambda_expr const& x,
                     array_<list> const& input) -> array_<data> {
  TENZIR_UNUSED(input);
  return not_implemented(x);
}

auto evaluator::eval(tenzir::ast::expression const& x,
                     tenzir::ActiveRows const& active) -> array_<data> {
  return tenzir::trace_panic(x, [&] {
    auto result = x.match([&](auto const& y) {
      return eval(y, active);
    });
    TENZIR_ASSERT_EQ(result.length(), static_cast<std::ptrdiff_t>(length_),
                     "got length {} instead of {} while evaluating {:?}",
                     result.length(), length_, x);
    return result;
  });
}

auto evaluator::to_array(data const& x) const -> array_<data> {
  TENZIR_UNUSED(x);
  TENZIR_TODO();
}

} // namespace tenzir2
