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

#define SUITE table_indexer

#include "vast/system/table_indexer.hpp"

#include "vast/test/test.hpp"

#include "vast/test/fixtures/events.hpp"

#include "vast/table_slice.hpp"

using vast::system::table_indexer;
using namespace vast;

FIXTURE_SCOPE(table_indexer_tests, fixtures::events)

TEST(zeek conn log) {
  // Initializing table_indexer with `nullptr` as parent means we must not call
  // any of these functions:
  // - init()
  // - flush_to_disk()
  // - state()
  // - indexer_at()
  // - row_ids_file()
  // - spawn_indexers()
  // - partition_dir()
  // - base_dir()
  // - data_dir()
  // These functions are tested as part of the `partition` test suite.
  table_indexer tbl{nullptr, zeek_conn_log_slices[0]->layout()};
  CHECK_EQUAL(tbl.dirty(), false);
  CHECK_EQUAL(tbl.columns(), zeek_conn_log_slices[0]->columns());
  CHECK_EQUAL(tbl.row_ids(), ids());
  CHECK_EQUAL(tbl.indexers().size(), zeek_conn_log_slices[0]->columns());
  CHECK_EQUAL(tbl.layout(), zeek_conn_log_slices[0]->layout());
  for (auto& slice : zeek_conn_log_slices)
    tbl.add(slice);
  CHECK_EQUAL(tbl.dirty(), true);
  CHECK_EQUAL(rank(tbl.row_ids()), zeek_conn_log.size());
  // Make sure the destructor does not try to flush to disk.
  tbl.set_clean();
  CHECK_EQUAL(tbl.dirty(), false);
}

FIXTURE_SCOPE_END()
