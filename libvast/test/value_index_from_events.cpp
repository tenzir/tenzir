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

#include "vast/value_index.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"

#define SUITE value_index
#include "test.hpp"
#include "fixtures/events.hpp"

FIXTURE_SCOPE(bro_conn_log_value_index_tests, fixtures::events)

// This test checks a regression that we encountered in combination with the
// bro conn.log
TEST(address from events) {
  using namespace vast;
  auto orig_h = [](const event& x) {
    auto& log_entry = get<vector>(x.data());
    auto& conn_id = get<vector>(log_entry[2]);
    return get<address>(conn_id[0]);
  };
  address_index idx;
  auto addr = *to<data>("169.254.225.22");
  for (auto i = 6400; i < 6500; ++i) { // The bogus range we identified.
    auto& x = bro_conn_log[i];
    auto before = idx.lookup(equal, addr);
    idx.push_back(orig_h(x), x.id());
    auto after = idx.lookup(equal, addr);
    // In [6400,6500), there is no address 169.254.225.22 present.
    CHECK_EQUAL(rank(*before), rank(*after));
  }
}

FIXTURE_SCOPE_END()
