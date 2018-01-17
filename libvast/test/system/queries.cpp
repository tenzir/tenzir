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

#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/node.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(query_tests, fixtures::node)

TEST(node queries) {
  ingest("bro");
  auto xs = query("proto == \"tcp\"");
  CHECK_EQUAL(xs.size(), 3135u);
}

FIXTURE_SCOPE_END()
