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

#include "vast/format/mrt.hpp"

#include "vast/test/data.hpp"
#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"

// TODO: you're likely gonna need these, too
// #include "vast/concept/parseable/to.hpp"
// #include "vast/defaults.hpp"
// #include "vast/detail/make_io_stream.hpp"
// #include "vast/filesystem.hpp"
// #include "vast/to_events.hpp"

using namespace vast;

// Technically, we don't need the actor system. However, we do need to
// initialize the table slice builder factories which happens automatically in
// the actor system setup. Further, including this fixture gives us access to
// log files to hunt down bugs faster.
FIXTURE_SCOPE(syslog_tests, fixtures::deterministic_actor_system)

TEST(Syslog) {
  // TODO: implement tests here for Syslog parser and reader.
}

FIXTURE_SCOPE_END()
