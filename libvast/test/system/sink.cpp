// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/error.hpp"
#include "vast/format/zeek.hpp"
#include "vast/system/sink.hpp"

#define SUITE system
#include "vast/test/test.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"

using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(sink_tests, fixtures::actor_system_and_events)

TEST(zeek sink) {
  MESSAGE("constructing a sink");
  caf::settings options;
  caf::put(options, "vast.export.write", directory.str());
  auto writer = std::make_unique<format::zeek::writer>(options);
  auto snk = self->spawn(sink, std::move(writer), 20u);
  MESSAGE("sending table slices");
  for (auto& slice : zeek_conn_log)
    self->send(snk, slice);
  MESSAGE("shutting down");
  self->send_exit(snk, caf::exit_reason::user_shutdown);
  self->wait_for(snk);
  CHECK(exists(directory / "zeek.conn.log"));
}

FIXTURE_SCOPE_END()
