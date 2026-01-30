//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/view3.hpp"

#include "tenzir/arrow_utils.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/generator.hpp"
#include "tenzir/series_builder.hpp"

namespace tenzir {

auto values3(const table_slice& x) -> generator<record_view3> {
  auto array = check(to_record_batch(x)->ToStructArray());
  for (auto row : values3(*array)) {
    TENZIR_ASSERT(row);
    co_yield *row;
  }
}

auto make_view_wrapper(data_view2 x) -> view_wrapper {
  auto b = series_builder{};
  b.data(std::move(x));
  return view_wrapper{b.finish_assert_one_array().array};
}

template <typename Ordering>
auto badly_strengthen_partial(std::partial_ordering o) -> Ordering {
  if constexpr (std::same_as<Ordering, std::partial_ordering>) {
    return o;
  } else {
    if (o == std::partial_ordering::less) {
      return Ordering::less;
    }
    if (o == std::partial_ordering::greater) {
      return Ordering::greater;
    }
    if (o == std::partial_ordering::equivalent) {
      return Ordering::equivalent;
    }
    return Ordering::equivalent;
  }
}

template <typename Ordering>
auto order_impl(const record_view3 l, const record_view3 r) -> Ordering;
template <typename Ordering>
auto order_impl(const list_view3 l, const list_view3 r) -> Ordering;

template <typename Ordering>
auto order_impl(const data_view3 l, const data_view3 r) -> Ordering {
  constexpr static auto is_partial
    = std::same_as<Ordering, std::partial_ordering>;
  constexpr static auto f = detail::overload{
    [](const concepts::integer auto& l,
       const concepts::integer auto& r) -> Ordering {
      if (std::cmp_less(l, r)) {
        return Ordering::less;
      }
      if (std::cmp_greater(l, r)) {
        return Ordering::greater;
      }
      return Ordering::equivalent;
    },
    []<concepts::number L, concepts::number R>(const L& l,
                                               const R& r) -> Ordering {
      if constexpr (std::same_as<L, double>) {
        if (std::isnan(l)) {
          return Ordering::greater;
        }
      }
      if constexpr (std::same_as<R, double>) {
        if (std::isnan(r)) {
          return Ordering::less;
        }
      }
      return badly_strengthen_partial<Ordering>(l <=> r);
    },
    [](const record_view3 l, const record_view3 r) -> Ordering {
      return order_impl<Ordering>(l, r);
    },
    [](const list_view3 l, const list_view3 r) -> Ordering {
      return order_impl<Ordering>(l, r);
    },
    []<typename L, typename R>(const L& l, const R& r) -> Ordering {
      constexpr auto l_is_null = std::same_as<L, caf::none_t>;
      constexpr auto r_is_null = std::same_as<R, caf::none_t>;
      if constexpr (l_is_null and r_is_null) {
        return Ordering::equivalent;
      } else if constexpr (l_is_null) {
        return Ordering::greater;
      } else if constexpr (r_is_null) {
        return Ordering::less;
      } else if constexpr (std::same_as<L, R>) {
        if constexpr (requires { l <=> r; }) {
          return badly_strengthen_partial<Ordering>(l <=> r);
        }
        /// In partial ordering, types that cant be ordered are unordered.
        /// If we require a weak ordering, we say that are equivalent.
        if constexpr (is_partial) {
          return std::partial_ordering::unordered;
        } else {
          return std::weak_ordering::equivalent;
        }
      } else if constexpr (is_partial) {
        /// In partial ordering, objects that are of non-relatable types are
        /// unordered.
        return std::partial_ordering::unordered;
      } else {
        /// If we require weak ordering, we order by type index.
        constexpr auto li = detail::tl_index_of<data_view_types, L>::value;
        constexpr auto ri = detail::tl_index_of<data_view_types, R>::value;
        return li <=> ri;
      }
    },
  };
  return match(std::tie(l, r), f);
}

auto partial_order(const data_view3 l, const data_view3 r)
  -> std::partial_ordering {
  return order_impl<std::partial_ordering>(l, r);
}

auto weak_order(const data_view3 l, const data_view3 r) -> std::weak_ordering {
  return order_impl<std::weak_ordering>(l, r);
}

template <typename Ordering>
auto order_impl(const record_view3 l, const record_view3 r) -> Ordering {
  const auto& l_fields = l.array_.type()->fields();
  const auto& r_fields = r.array_.type()->fields();
  for (auto i = 0uz; i < l_fields.size() and i < r_fields.size(); ++i) {
    const auto& l_name = l_fields[i]->name();
    const auto& r_name = r_fields[i]->name();
    const auto name_order = l_name <=> r_name;
    if (name_order != Ordering::equivalent) {
      return name_order;
    }
    const auto l_value
      = view_at(*l.array_.field(detail::narrow<int>(i)), l.index_);
    const auto r_value
      = view_at(*r.array_.field(detail::narrow<int>(i)), r.index_);
    const auto value_order = order_impl<Ordering>(l_value, r_value);
    if (value_order != Ordering::equivalent) {
      return value_order;
    }
  }
  return l_fields.size() <=> r_fields.size();
}

template <typename Ordering>
auto order_impl(const list_view3 l, const list_view3 r) -> Ordering {
  auto lit = l.begin();
  auto rit = r.begin();
  for (; lit != l.end() and rit != r.end(); ++lit, ++rit) {
    const auto comp = order_impl<Ordering>(*lit, *rit);
    if (comp != Ordering::equivalent) {
      return comp;
    }
  }
  return l.size() <=> r.size();
}

auto partial_order(const list_view3 l, const list_view3 r)
  -> std::partial_ordering {
  return order_impl<std::partial_ordering>(l, r);
}

auto weak_order(const list_view3 l, const list_view3 r) -> std::weak_ordering {
  return order_impl<std::weak_ordering>(l, r);
}

} // namespace tenzir

namespace fmt {

#define X(view)                                                                \
  auto formatter<tenzir::view>::format(const tenzir::view& value,              \
                                       format_context& ctx) const              \
    -> format_context::iterator {                                              \
    auto printer = tenzir::json_printer{{                                      \
      .tql = true,                                                             \
      .oneline = true,                                                         \
    }};                                                                        \
    auto it = ctx.out();                                                       \
    const auto ok = printer.print(it, value);                                  \
    TENZIR_ASSERT(ok);                                                         \
    return it;                                                                 \
  }                                                                            \
  static_assert(true)

X(data_view3);
X(record_view3);
X(list_view3);

#undef X

} // namespace fmt
