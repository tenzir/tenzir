#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/data_store.hpp"
#include "vast/system/importer.hpp"

#define SUITE system
#include "test.hpp"
#include "fixtures/actor_system_and_events.hpp"

using namespace caf;
using namespace vast;

FIXTURE_SCOPE(importer_tests, fixtures::actor_system_and_events)

TEST(importer) {
  directory /= "importer";
  auto store = self->spawn(system::data_store<std::string, data>);
  auto importer = self->spawn(system::importer, directory, 1024);
  self->send(importer, store);
  self->send(importer, actor_cast<system::archive_type>(self));
  self->send(importer, system::index_atom::value, self);
  MESSAGE("sending events");
  self->send(importer, bro_conn_log);
  self->send(importer, bro_dns_log);
  MESSAGE("receiving reflected events");
  for (auto i = 0; i < 4; ++i)
    self->receive(
      [&](const std::vector<event>&) { },
      error_handler()
    );
  self->send_exit(importer, exit_reason::user_shutdown);
}

FIXTURE_SCOPE_END()
