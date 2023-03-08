//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/sink.hpp"

#include "vast/error.hpp"
#include "vast/format/zeek.hpp"
#include "vast/test/data.hpp"
#include "vast/test/fixtures/actor_system_and_events.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace vast::system;

namespace {

class fixture : public fixtures::actor_system_and_events {
public:
  fixture() : fixtures::actor_system_and_events(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(sink_tests, fixture)

TEST(zeek sink) {
  MESSAGE("constructing a sink");
  caf::settings options;
  caf::put(options, "vast.export.write", directory.string());
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
