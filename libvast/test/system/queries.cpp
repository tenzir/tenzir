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

#define SUITE system
#include "vast/test/test.hpp"
#include "vast/test/fixtures/node.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(query_tests, fixtures::node)

TEST_DISABLED(node queries) {
  ingest("zeek");
  CHECK_EQUAL(rows(query("proto == \"udp\"")), 20u);
  CHECK_EQUAL(rows(query("proto == \"tcp\"")), 0u);
}

FIXTURE_SCOPE_END()
