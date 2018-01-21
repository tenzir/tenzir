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

#ifndef VAST_DETAIL_VARIADIC_SERIALIZATION_HPP
#define VAST_DETAIL_VARIADIC_SERIALIZATION_HPP

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

namespace vast::detail {

// Variadic helpers to interface with CAF's serialization framework.

template <class Processor, class T>
void process(Processor& proc, T&& x) {
  proc & const_cast<T&>(x); // CAF promises not to touch it.
}

template <class Processor, class T, class... Ts>
void process(Processor& proc, T&& x, Ts&&... xs) {
  process(proc, std::forward<T>(x));
  process(proc, std::forward<Ts>(xs)...);
}

template <class T>
void write(caf::serializer& sink, T const& x) {
  sink << x;
}

template <class T, class... Ts>
void write(caf::serializer& sink, T const& x, Ts const&... xs) {
  write(sink, x);
  write(sink, xs...);
}

template <class T>
void read(caf::deserializer& source, T& x) {
  source >> x;
}

template <class T, class... Ts>
void read(caf::deserializer& source, T& x, Ts&... xs) {
  read(source, x);
  read(source, xs...);
}

} // namespace vast::detail

#endif
