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

namespace vast::detail {

template <class T>
static const void* get_most_derived(const T& x) {
  if constexpr (!std::is_polymorphic_v<T> || std::is_final_v<T>)
    return &x;
  return dynamic_cast<const void*>(&x);
}

} // namespace vast::detail

namespace vast {

/// The base class for polymorphic visitors.
/// @relates make_visitor
template <class Result>
class polymorphic_visitor {
public:
  template <class T>
  caf::optional<Result> operator()(const T& x) {
    return do_visit(detail::get_most_derived(x), typeid(x));
  }

protected:
  ~polymorphic_visitor() {
    // nop
  }

private:
  virtual caf::optional<Result> do_visit(const void* ptr,
                                         const std::type_info& info) = 0;
};

namespace detail {

template <class Result, class F, class... Ts>
class lambda_visitor : public polymorphic_visitor<Result> {
public:
  explicit lambda_visitor(F f) : f_(std::move(f)) {
    // nop
  }

private:
  using result_t = std::conditional_t<std::is_same_v<void, Result>,
                                      caf::unit_t, Result>;

  template <class T>
  bool try_visit(result_t& result, const void* ptr,
                 const std::type_info& info) {
    if (info != typeid(T))
      return false;
    if constexpr (std::is_same_v<void, Result>)
      f_(*static_cast<const T*>(ptr));
    else
      result = f_(*static_cast<const T*>(ptr));
    return true;
  }

  caf::optional<Result> do_visit(const void* ptr,
                                 const std::type_info& info) override {
    result_t result;
    if ((try_visit<Ts>(result, ptr, info) || ...))
      return result;
    return caf::none;
  }

  F f_;
};

template <class F>
using lambda_visitor_res =
  typename caf::detail::get_callable_trait<F>::result_type;

} // namespace detail

/// Constructs a visitor for a selected number of types in a polymorphic
/// hiearchy.
/// @relates visitor
template <class... Ts, class F, class... Fs>
static auto make_visitor(F f, Fs... fs) {
  using namespace detail;
  using result_type = lambda_visitor_res<F>;
  // All lambdas must return the same type.
  static_assert((std::is_same_v<result_type, lambda_visitor_res<Fs>> && ...));
  auto lambda = overload(std::move(f), std::move(fs)...);
  using visitor_type = lambda_visitor<result_type, decltype(lambda), Ts...>;
  return visitor_type{std::move(lambda)};
}

} // namespace vast
