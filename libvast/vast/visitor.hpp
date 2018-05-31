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
class visitor {
public:
  template <class T>
  void operator()(const T& x) {
    do_visit(detail::get_most_derived(x), typeid(x));
  }

protected:
  ~visitor() {
    // nop
  }

private:
  virtual void do_visit(const void* ptr, const std::type_info& info) = 0;
};

namespace detail {

template <class F, class... Ts>
class lambda_visitor : public visitor {
public:
  explicit lambda_visitor(F f) : f_(std::move(f)) {
    // nop
  }

private:
  template <class T>
  bool try_visit(const void* ptr, const std::type_info& info) {
    if (info != typeid(T))
      return false;
    f_(*static_cast<const T*>(ptr));
    return true;
  }

  void do_visit(const void* ptr, const std::type_info& info) override {
    (try_visit<Ts>(ptr, info) || ...);
  }

  F f_;
};

} // namespace detail

/// Constructs a visitor for a selected number of types in a polymorphic
/// hiearchy.
/// @relates visitor
template <class... Ts, class... Fs>
static auto make_visitor(Fs... fs) {
  auto lambda = detail::overload(std::move(fs)...);
  return detail::lambda_visitor<decltype(lambda), Ts...>(std::move(lambda));
}

} // namespace vast
