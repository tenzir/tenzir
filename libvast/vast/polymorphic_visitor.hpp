/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - URL:        https://gist.github.com/foonathan/daad3fffaf5dd7cd7a5bbabd6ccd8c1b
// - Author:     Jonathan Müller
// - Details:    https://foonathan.net/blog/2017/12/21/visitors.html

#pragma once

#include <typeinfo>
#include <type_traits>

#include <caf/optional.hpp>
#include <caf/detail/type_traits.hpp>

#include "vast/detail/overload.hpp"

namespace vast {

/// The base class for polymorphic visitors.
template <class Result, class CommonBase>
class polymorphic_visitor {
public:
  template <class T>
  caf::optional<Result> operator()(const T& x) {
    static_assert(std::is_base_of_v<CommonBase, T>);
    return do_visit(&x);
  }

protected:
  virtual caf::optional<Result> do_visit(const CommonBase* ptr) = 0;

  ~polymorphic_visitor() {
    // nop
  }
};

namespace detail {

template <class F>
using fun_res_t =
  typename caf::detail::get_callable_trait<F>::result_type;

template <class F>
struct fun_arg {
  using arg_types = typename caf::detail::get_callable_trait<F>::arg_types;
  static_assert(caf::detail::tl_size<arg_types>::value == 1);
  using type = std::decay_t<caf::detail::tl_head_t<arg_types>>;
};

template <class F>
using fun_arg_t = typename fun_arg<F>::type;

template <class Result, class CommonBase, class F, class... Ts>
class fun final : public polymorphic_visitor<Result, CommonBase> {
public:
  using result_t = std::conditional_t<std::is_same_v<void, Result>,
                                      caf::unit_t, Result>;

  explicit fun(F f) : f_(std::move(f)) {
    // nop
  }

protected:
  template <class T>
  bool try_visit(caf::optional<result_t>& result, const CommonBase* ptr) {
    auto invoke = [&](const T* dptr) {
      if constexpr (std::is_same_v<void, Result>)
        f_(*dptr);
      else
        result = f_(*dptr);
    };
    if constexpr (std::is_final_v<T>) {
      if (typeid(*ptr) == typeid(T)) {
        invoke(static_cast<const T*>(ptr));
        return true;
      }
    } else {
      auto dptr = dynamic_cast<const T*>(ptr);
      if (dptr != nullptr) {
        invoke(dptr);
        return true;
      }
    }
    return false;
  }

  caf::optional<Result> do_visit(const CommonBase* ptr) override {
    caf::optional<result_t> result;
    (try_visit<Ts>(result, ptr) || ...);
    return result;
  }

  F f_;
};

} // namespace detail

/// Constructs a visitor from a list of unary functions. All functions must
/// take an argument that is-a `CommonBase`.
/// @relates polymorphic_visitor
template <class CommonBase, class F, class... Fs>
static auto make_polymorphic_visitor(F f, Fs... fs) {
  using namespace detail;
  using result_type = fun_res_t<F>;
  // All lambdas must return the same type.
  static_assert((std::is_same_v<result_type, fun_res_t<Fs>> && ...));
  // All lambdas must take one argument that's a subtype of CommonBase.
  static_assert(std::is_base_of_v<CommonBase, fun_arg_t<F>>);
  static_assert((std::is_base_of_v<CommonBase, fun_arg_t<Fs>> && ...));
  auto lambda = overload(std::move(f), std::move(fs)...);
  using visitor_type = fun<result_type, CommonBase, decltype(lambda),
                                      fun_arg_t<F>, fun_arg_t<Fs>...>;
  return visitor_type{std::move(lambda)};
}

} // namespace vast
