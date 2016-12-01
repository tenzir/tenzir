#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/system/archive.hpp"

#define SUITE archive
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(archive_tests, fixtures::actor_system_and_events)

TEST(archiving and querying) {
  auto a = self->spawn(system::archive, directory, 10, 1024 * 1024);
  MESSAGE("sending events");
  self->send(a, bro_conn_log);
  self->send(a, bro_dns_log);
  self->send(a, bro_http_log);
  self->send(a, bgpdump_txt);
  MESSAGE("querying event set {[100,150), [10150,10200)}");
  bitmap bm;
  bm.append_bits(false, 100);
  bm.append_bits(true, 50);
  bm.append_bits(false, 10000);
  bm.append_bits(true, 50);
  std::vector<event> result;
  self->request(a, infinite, bm).receive(
    [&](std::vector<event>& xs) { result = std::move(xs); },
    error_handler()
  );
  REQUIRE_EQUAL(result.size(), 100u);
  // We sort because the specific compression algorithm used at the archive
  // determines the order of results.
  std::sort(result.begin(), result.end());
  // We processed the segments in reverse order of arrival (to maximize LRU hit
  // rate). Therefore, the result set contains first the events with higher
  // IDs [10150,10200) and then the ones with lower ID [100,150).
  CHECK_EQUAL(result[0].id(), 100u);
  CHECK_EQUAL(result[0].type().name(), "bro::conn");
  CHECK_EQUAL(result[50].id(), 10150u);
  CHECK_EQUAL(result[50].type().name(), "bro::dns");
  CHECK_EQUAL(result[result.size() - 1].id(), 10199u);
  self->send(a, system::shutdown_atom::value);
}

FIXTURE_SCOPE_END()
