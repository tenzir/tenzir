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

#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_CHAR_HELPERS_HPP
#define VAST_CONCEPT_PARSEABLE_DETAIL_CHAR_HELPERS_HPP

#include <string>
#include <vector>

namespace vast {
namespace detail {

template <typename Attribute, typename T>
void absorb(Attribute& a, T&& x) {
  a = std::move(x);
}

inline void absorb(std::string& str, char c) {
  str += c;
}

inline void absorb(std::vector<char> v, char c) {
  v.push_back(c);
}

} // namespace detail
} // namespace vast

#endif
