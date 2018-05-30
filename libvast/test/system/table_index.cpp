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

#define SUITE table_index
#include "test.hpp"

#include "fixtures/events.hpp"
#include "fixtures/filesystem.hpp"

#include "vast/bitmap.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/error.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/system/table_index.hpp"

using namespace vast;
using namespace vast::system;

namespace {

struct fixture : fixtures::events, fixtures::filesystem {
  fixture() {
    directory /= "column-layout";
  }

  template <class T>
  T unbox(expected<T> x) {
    REQUIRE(x);
    return std::move(*x);
  }
};

} // namespace <anonymous>

FIXTURE_SCOPE(table_index_tests, fixture)

TEST(bro conn logs) {
  MESSAGE("generate column layout for bro conn logs");
  const auto conn_log_type = bro_conn_log[0].type();
  auto cols = unbox(make_table_index(directory, conn_log_type));
  CHECK_EQUAL(cols.num_meta_columns(), 2u);
  MESSAGE("ingesting events");
  for (auto& entry : bro_conn_log) {
    auto err = cols.add(entry);
    if (err) {
      FAIL("error during ingestion: " << caf::to_string(err));
    }
  }
  MESSAGE("querying");
  auto pred = to<predicate>("id.resp_p == 995/?");
  REQUIRE(pred);
  auto result = unbox(cols.lookup(*pred));
  CHECK_EQUAL(rank(result), 53u);
  auto check_uid = [](const event& e, const std::string& uid) {
    auto& v = get<vector>(e.data());
    return v[1] == uid;
  };
  for (auto i : select(result))
    if (i == 819)
      CHECK(check_uid(bro_conn_log[819], "KKSlmtmkkxf")); // first
    else if (i == 3594)
      CHECK(check_uid(bro_conn_log[3594], "GDzpFiROJQi")); // intermediate
    else if (i == 6338)
      CHECK(check_uid(bro_conn_log[6338], "zwCckCCgXDb")); // last
}

FIXTURE_SCOPE_END()
