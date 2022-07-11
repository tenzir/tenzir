//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE archive

#include "vast/system/archive.hpp"

#include "vast/concept/printable/stream.hpp"
#include "vast/detail/spawn_container_source.hpp"
#include "vast/ids.hpp"
#include "vast/table_slice.hpp"

#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

#include <filesystem>

using namespace caf;
using namespace vast;

namespace {

struct fixture : fixtures::deterministic_actor_system_and_events {
  system::archive_actor a;

  fixture()
    : fixtures::deterministic_actor_system_and_events(
      VAST_PP_STRINGIFY(SUITE)) {
    a = self->spawn(system::archive, directory, 10, 1024 * 1024);
  }

  void push_to_archive(std::vector<table_slice> xs) {
    vast::detail::spawn_container_source(sys, std::move(xs), a);
    run();
  }

  std::vector<table_slice> make_query_context(const ids& ids) {
    bool done = false;
    uint64_t tally = 0;
    uint64_t rows = 0;
    std::vector<table_slice> result;
    auto query_context = query_context::make_extract(self, expression{});
    query_context.ids = ids;
    self->send(a, atom::query_v, query_context);
    run();
    self
      ->do_receive(
        [&](uint64_t x) {
          tally = x;
          done = true;
        },
        [&](table_slice slice) {
          rows += slice.rows();
          result.push_back(std::move(slice));
        })
      .until(done);
    REQUIRE_EQUAL(rows, tally);
    return result;
  }

  std::vector<table_slice>
  make_query_context(std::initializer_list<id_range> ranges) {
    return make_query_context(make_ids(ranges));
  }
};

} // namespace

FIXTURE_SCOPE(archive_tests, fixture)

TEST(zeek conn logs slices) {
  push_to_archive(zeek_conn_log);
  auto result = make_query_context({{10, 15}});
  CHECK_EQUAL(rows(result), 5u);
}

TEST(archiving and querying) {
  MESSAGE("import Zeek conn logs to archive");
  push_to_archive(zeek_conn_log);
  MESSAGE("import Zeek DNS logs to archive");
  push_to_archive(zeek_dns_log);
  MESSAGE("import Zeek HTTP logs to archive");
  push_to_archive(zeek_http_log);
  MESSAGE("query events");
  // conn.log = [0, 20)
  // dns.log  = [20, 52)
  // http.log = [1052, 1092)
  auto result = make_query_context(make_ids({{24, 56}, {1076, 1096}}));
  REQUIRE_EQUAL(rows(result), (52u - 24) + (1092 - 1076));
  // ID 20 is the first entry in the dns.log; then add 4 more.
  auto id_type
    = caf::get<record_type>(zeek_conn_log[0].layout()).field(offset{1}).type;
  CHECK_EQUAL(result[0].at(3, 1, id_type), make_data_view("JoNZFZc3lJb"));
  // ID 1052 is the first entry in the http.log; then add 4 more.
  auto last = result[result.size() - 1];
  CHECK_EQUAL(last.at(last.rows() - 1, 1, id_type), make_data_view("rydI6puScN"
                                                                   "a"));
  self->send_exit(a, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
