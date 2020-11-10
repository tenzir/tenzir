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

#define SUITE msgpack_table_slice

#include "vast/msgpack_table_slice.hpp"

#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include "vast/msgpack_table_slice_builder.hpp"

using namespace vast;

FIXTURE_SCOPE(msgpack_table_slice_tests, fixtures::table_slices)

TEST_TABLE_SLICE(msgpack_table_slice_builder, "msgpack")

FIXTURE_SCOPE_END()
