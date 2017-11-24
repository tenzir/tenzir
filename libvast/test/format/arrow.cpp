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

#define SUITE format

#include "vast/format/arrow.hpp"

#include "vast/test/test.hpp"
#include "vast/test/fixtures/events.hpp"

//#include <plasma/store.h>

using namespace vast;

struct fixture : fixtures::events {
  fixture() {
// FIXME: Cannot use a plasma store fixture because the <plasma/store.h>
// transitively includes flatbuffers, which we don't have necessarily .
//    rm(directory);
//    constexpr int64_t system_memory = 1'000'000;
//    constexpr bool hugetlbfs_enabled = false;
//    plasma_store = std::make_unique<plasma::PlasmaStore>(nullptr,
//                                                         system_memory,
//                                                         directory,
//                                                         hugetlbfs_enabled);
  }

  std::string directory = "/tmp/plasma";
////  std::unique_ptr<plasma::PlasmaStore> plasma_store;
};

FIXTURE_SCOPE(plasma_tests, fixture)

TEST(arrow writer) {
  format::arrow::writer writer{directory};
  REQUIRE(writer.connected());
  for (auto& x : bro_conn_log)
    CHECK(writer.write(x));
}

FIXTURE_SCOPE_END()
