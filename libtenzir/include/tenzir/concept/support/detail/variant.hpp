//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/type_list.hpp"
#include "tenzir/variant.hpp"

#include <caf/detail/type_list.hpp>

namespace tenzir::detail {

template <class... Ts>
struct lazy_type_list {
  using type = type_list<Ts...>;
};

template <class T, class U>
struct lazy_variant_concat {
  using type = tl_concat_t<
    tl_make_t<typename T::types>,
    tl_make_t<typename U::types>
  >;
};

template <class T, class U>
struct lazy_variant_push_back {
  using type = tl_push_back_t<
    tl_make_t<typename T::types>,
    U
  >;
};

template <class T, class U>
using variant_type_concat =
  tl_distinct_t<
    typename std::conditional_t<
      caf::is_variant<T>{} && caf::is_variant<U>{},
      lazy_variant_concat<T, U>,
      std::conditional_t<
        caf::is_variant<T>{},
        lazy_variant_push_back<T, U>,
        std::conditional_t<
          caf::is_variant<U>{},
          lazy_variant_push_back<U, T>,
          lazy_type_list<T, U>
        >
      >
    >::type
  >;

template <class T, class U>
using flattened_variant
  = caf::detail::tl_apply_t<variant_type_concat<T, U>, tenzir::variant>;

} // namespace tenzir::detail
