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

default_vector_view::default_vector_view(const vector& xs) : xs_{xs} {
  // nop
}

vector_view::value_type default_vector_view::at(size_type i) const {
  return make_view(xs_[i]);
}

vector_view::size_type default_vector_view::size() const {
  return xs_.size();
}

view_t<data> make_view(const data& x) {
  return visit([](const auto& z) { return make_view(z); }, x);
}

} // namespace vast
