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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/importer.hpp"
#include "vast/system/index.hpp"
#include "vast/system/replicated_store.hpp"

#include "vast/detail/spawn_container_source.hpp"

#define SUITE export
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

FIXTURE_SCOPE(exporter_tests, fixtures::actor_system_and_events)

TEST(exporter historical) {
  auto i = self->spawn(system::index, directory / "index", 1000, 5, 5, 1);
  auto a = self->spawn(system::archive, directory / "archive", 1, 1024);
  MESSAGE("ingesting conn.log");
  self->send(i, bro_conn_log);
  self->send(a, bro_conn_log);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing historical query");
  auto e = self->spawn(system::exporter, *expr, historical);
  self->send(e, a);
  self->send(e, system::index_atom::value, i);
  self->send(e, system::sink_atom::value, self);
  self->send(e, system::run_atom::value);
  self->send(e, system::extract_atom::value);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(i, exit_reason::user_shutdown);
  self->send_exit(a, exit_reason::user_shutdown);
}

TEST(exporter continuous -- exporter only) {
  auto i = self->spawn(system::index, directory / "index", 1000, 5, 5, 1);
  auto a = self->spawn(system::archive, directory / "archive", 1, 1024);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing continuous query");
  auto e = self->spawn(system::exporter, *expr, continuous);
  self->send(e, a);
  self->send(e, system::index_atom::value, i);
  self->send(e, system::sink_atom::value, self);
  self->send(e, system::run_atom::value);
  self->send(e, system::extract_atom::value);
  MESSAGE("ingesting conn.log");
  self->send(e, bro_conn_log);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(i, exit_reason::user_shutdown);
  self->send_exit(a, exit_reason::user_shutdown);
}


/*
TEST(foobar) {
  struct pseudo_container {
    struct iterator {
      int value;
      iterator(int x = 0) : value(x) {
        // nop
      }
      iterator(const iterator&) = default;
      iterator& operator=(const iterator&) = default;
      int operator*() const {
        return value;
      }
      iterator operator++(int) {
        return value++;
      }
      iterator& operator++() {
        ++value;
        return *this;
      }
      bool operator!=(const iterator& other) const {
        return value != other.value;
      }
      bool operator==(const iterator& other) const {
        return value == other.value;
      }
    };
    using value_type = int;
    inline iterator begin() {
      return 0;
    }
    inline iterator end() {
      return 2048;
    }
  };
  int result = 0;
  int expected_result = 0;
  pseudo_container tmp;
  for (auto i : tmp)
    expected_result += i;
  auto snk = self->spawn([&result](event_based_actor* ptr) mutable -> behavior {
    return {
      [ptr, &result](stream<int> in) mutable {
        ptr->make_sink(
          in,
          [](unit_t&) {},
          [&result](unit_t&, int x) mutable {
            MESSAGE("receive: " << x);
            result += x;
          },
          [ptr](const unit_t&, const error& err) {
            MESSAGE("sink done: " << ptr->system().render(err));
            ptr->quit();
          }
        );
      }
    };
  });
  MESSAGE("start streaming");
  self->wait_for(spawn_container_source(self->system(), snk, tmp));
  self->wait_for(snk);
  CHECK_EQUAL(result, expected_result);
}
*/

TEST(exporter continuous -- with importer) {
  using namespace system;
  auto ind = self->spawn(system::index, directory / "index", 1000, 5, 5, 1);
  auto arc = self->spawn(archive, directory / "archive", 1, 1024);
  auto imp = self->spawn(importer, directory / "importer");
  auto con = self->spawn(raft::consensus, directory / "consensus");
  self->send(con, run_atom::value);
  meta_store_type ms = self->spawn(replicated_store<std::string, data>, con);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing continuous query");
  auto exp = self->spawn(exporter, *expr, continuous);
  self->send(exp, arc);
  self->send(exp, index_atom::value, ind);
  self->send(exp, sink_atom::value, self);
  self->send(exp, run_atom::value);
  self->send(exp, extract_atom::value);
  self->send(imp, arc);
  self->send(imp, index_atom::value, ind);
  self->send(imp, ms);
  self->send(imp, exporter_atom::value, exp);
  MESSAGE("ingesting conn.log");
  self->wait_for(
    vast::detail::spawn_container_source(self->system(), imp, bro_conn_log));
  //self->send(imp, bro_conn_log);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(ind, exit_reason::user_shutdown);
  self->send_exit(arc, exit_reason::user_shutdown);
  self->send_exit(imp, exit_reason::user_shutdown);
  self->send_exit(con, exit_reason::user_shutdown);
}

TEST(exporter universal) {
  using namespace system;
  auto ind = self->spawn(system::index, directory / "index", 1000, 5, 5, 1);
  auto arc = self->spawn(archive, directory / "archive", 1, 1024);
  auto imp = self->spawn(importer, directory / "importer");
  auto con = self->spawn(raft::consensus, directory / "consensus");
  self->send(con, run_atom::value);
  meta_store_type ms = self->spawn(replicated_store<std::string, data>, con);
  auto expr = to<expression>("service == \"http\" && :addr == 212.227.96.110");
  REQUIRE(expr);
  self->send(imp, arc);
  self->send(imp, index_atom::value, ind);
  self->send(imp, ms);
  MESSAGE("ingesting conn.log for historical query part");
  self->send(ind, bro_conn_log);
  self->send(arc, bro_conn_log);
  MESSAGE("issueing universal query");
  auto exp = self->spawn(exporter, *expr, continuous + historical);
  self->send(exp, arc);
  self->send(exp, index_atom::value, ind);
  self->send(exp, sink_atom::value, self);
  self->send(exp, run_atom::value);
  self->send(exp, extract_atom::value);
  self->send(imp, exporter_atom::value, exp);
  MESSAGE("waiting for results");
  std::vector<event> results;
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  results.clear();
  MESSAGE("ingesting conn.log for continuous query part");
  self->send(imp, bro_conn_log);
  self->do_receive(
    [&](std::vector<event>& xs) {
      std::move(xs.begin(), xs.end(), std::back_inserter(results));
    },
    error_handler()
  ).until([&] { return results.size() == 28; });
  MESSAGE("sanity checking result correctness");
  CHECK_EQUAL(results.front().id(), 105u);
  CHECK_EQUAL(results.front().type().name(), "bro::conn");
  CHECK_EQUAL(results.back().id(), 8354u);
  self->send_exit(ind, exit_reason::user_shutdown);
  self->send_exit(arc, exit_reason::user_shutdown);
  self->send_exit(imp, exit_reason::user_shutdown);
  self->send_exit(con, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
