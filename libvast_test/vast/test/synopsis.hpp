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

#pragma once

#include <caf/optional.hpp>

#include "vast/synopsis.hpp"

#include "vast/test/test.hpp"

namespace vast::test {

namespace {

namespace nft {

inline constexpr auto N = caf::none;
inline const auto T = caf::optional<bool>{true};
inline const auto F = caf::optional<bool>{false};

}

struct verifier {
  synopsis_ptr syn;
  inline
  void operator()(data_view rhs, std::array<caf::optional<bool>, 12> ref) {
    CHECK_EQUAL(syn->lookup(vast::match, rhs), ref[0]);
    CHECK_EQUAL(syn->lookup(vast::not_match, rhs), ref[1]);
    CHECK_EQUAL(syn->lookup(vast::in, rhs), ref[2]);
    CHECK_EQUAL(syn->lookup(vast::not_in, rhs), ref[3]);
    CHECK_EQUAL(syn->lookup(vast::ni, rhs), ref[4]);
    CHECK_EQUAL(syn->lookup(vast::not_ni, rhs), ref[5]);
    CHECK_EQUAL(syn->lookup(vast::equal, rhs), ref[6]);
    CHECK_EQUAL(syn->lookup(vast::not_equal, rhs), ref[7]);
    CHECK_EQUAL(syn->lookup(vast::less, rhs), ref[8]);
    CHECK_EQUAL(syn->lookup(vast::less_equal, rhs), ref[9]);
    CHECK_EQUAL(syn->lookup(vast::greater, rhs), ref[10]);
    CHECK_EQUAL(syn->lookup(vast::greater_equal, rhs), ref[11]);
  }
};

} // namespace <anonymous>

} // namespace vast::test

