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

#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"

namespace vast {

/// The base class for polymorphic visitors.
template <class Result, class... Bases>
class polymorphic_visitor {
public:
  Result operator()(const Bases&... xs) {
    return do_visit(&xs...);
  }

protected:
  virtual Result do_visit(const Bases*...) = 0;

  ~polymorphic_visitor() {
    // nop
  }
};

namespace detail {

template <class F>
using fun_res_t =
  typename caf::detail::get_callable_trait<F>::result_type;

template <class Result, class Bases, class F, class... Ts>
class polymorphic_visitor_impl;

template <class Result, class... Bases, class F, class... Ts>
class polymorphic_visitor_impl<
  Result,
  caf::detail::type_list<Bases...>,
  F,
  Ts...
> final : public polymorphic_visitor<Result, Bases...> {
public:
  using result_type = std::conditional_t<
    std::is_void_v<Result>,
    caf::unit_t,
    Result
  >;

  explicit polymorphic_visitor_impl(F f) : f_(std::move(f)) {
    // nop
  }

protected:
  template <class T, class Ptr>
  const T* downcast(Ptr ptr) {
    if constexpr (!std::is_final_v<T>)
      return dynamic_cast<const T*>(ptr);
    if (typeid(*ptr) == typeid(T))
      return static_cast<const T*>(ptr);
    return nullptr;
  }

  template <class... Args>
  bool try_visit(caf::detail::type_list<Args...>,
                 caf::optional<result_type>& result, const Bases*... xs) {
    static_assert(sizeof...(Args) == sizeof...(Bases));
    auto invoke = [&](auto... args) {
      if ((!args || ...))
        return false;
      if constexpr (std::is_void_v<Result>) {
        f_(*args...);
        result = caf::unit;
      } else {
        result = f_(*args...);
      }
      return true;
    };
    return invoke(downcast<std::decay_t<Args>>(xs)...);
  }

  Result do_visit(const Bases*... xs) override {
    caf::optional<result_type> result;
    (try_visit(Ts{}, result, xs...) || ...);
    VAST_ASSERT(result);
    if constexpr (std::is_void_v<Result>)
      return;
    else
      return std::move(*result);
  }

  F f_;
};

} // namespace detail

/// Constructs a visitor from a list of functions.
/// @relates polymorphic_visitor
template <class... Bases, class F, class... Fs>
static auto make_polymorphic_visitor(F f, Fs... fs) {
  using namespace detail;
  // All lambdas must return the same type.
  using result_type = fun_res_t<F>;
  static_assert((std::is_same_v<result_type, fun_res_t<Fs>> && ...));
  // Construct the lambda and the polymorphic visitor.
  auto lambda = overload(std::move(f), std::move(fs)...);
  using visitor_type = polymorphic_visitor_impl<
    result_type,
    caf::detail::type_list<Bases...>,
    decltype(lambda),
    typename caf::detail::get_callable_trait<F>::arg_types,
    typename caf::detail::get_callable_trait<Fs>::arg_types...
  >;
  return visitor_type{std::move(lambda)};
}

} // namespace vast
