#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/query_options.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/exporter.hpp"
#include "vast/system/index.hpp"

#define SUITE export
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;
using namespace std::chrono;

FIXTURE_SCOPE(exporter_tests, fixtures::actor_system_and_events)

TEST(exporter) {
  auto i = self->spawn(system::index, directory / "index", 1000, 2);
  auto a = self->spawn(system::archive, directory / "archive", 1, 1024);
  MESSAGE("ingesting conn.log");
  self->send(i, bro_conn_log);
  self->send(a, bro_conn_log);
  auto expr = to<expression>("service == \"http\" && addr == 212.227.96.110");
  REQUIRE(expr);
  MESSAGE("issueing query");
  auto e = self->spawn(system::exporter, *expr, historical);
  self->send(e, a);
  self->send(e, system::put_atom::value, system::index_atom::value, i);
  self->send(e, system::put_atom::value, system::sink_atom::value, self);
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
  self->send(i, system::shutdown_atom::value);
  self->send(a, system::shutdown_atom::value);
}

FIXTURE_SCOPE_END()
