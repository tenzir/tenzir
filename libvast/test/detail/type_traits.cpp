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

#define SUITE type_traits

#include "vast/test/test.hpp"

#include "vast/detail/type_traits.hpp"

TEST(sum) {
  static_assert(vast::detail::sum<> == 0);
  static_assert(vast::detail::sum<1, 2, 3> == 6);
  static_assert(vast::detail::sum<42, 58> == 100);
}

