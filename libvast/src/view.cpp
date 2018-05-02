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

#include "vast/view.hpp"

namespace vast {

pattern_view::pattern_view(const pattern& x) : pattern_{x.string()} {
  // nop
}

bool operator==(pattern_view x, pattern_view y) noexcept {
  return x.pattern_ == y.pattern_;
}


address_view::address_view(const address& x) : data_{&x.data()} {
  // nop
}

bool operator==(address_view x, address_view y) noexcept {
  return x.data_ == y.data_;
}


subnet_view::subnet_view(const subnet& x)
  : network_{x.network()},
    length_{x.length()} {
  // nop
}

bool operator==(subnet_view x, subnet_view y) noexcept {
  return x.network_ == y.network_ && x.length_ == y.length_;
}


default_vector_view::default_vector_view(const vector& xs) : xs_{xs} {
  // nop
}

default_vector_view::value_type default_vector_view::at(size_type i) const {
  return make_data_view(xs_[i]);
}

default_vector_view::size_type default_vector_view::size() const noexcept {
  return xs_.size();
}


default_set_view::default_set_view(const set& xs) : xs_{xs} {
  // nop
}

default_set_view::value_type default_set_view::at(size_type i) const {
  auto it = xs_.begin();
  std::advance(it, i);
  return make_data_view(*it);
}

default_set_view::size_type default_set_view::size() const noexcept {
  return xs_.size();
}


default_table_view::default_table_view(const table& xs) : xs_{xs} {
  // nop
}

default_table_view::value_type default_table_view::at(size_type i) const {
  auto it = xs_.begin();
  std::advance(it, i);
  auto& [key, value] = *it;
  return {make_data_view(key), make_data_view(value)};
}

default_table_view::size_type default_table_view::size() const noexcept {
  return xs_.size();
}


data_view make_view(const data& x) {
  return visit([](const auto& z) { return make_data_view(z); }, x);
}


} // namespace vast
