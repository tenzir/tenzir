#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/expression.h"
#include "vast/actor/program.h"

#include "framework/unit.h"
#include "test_data.h"

using namespace caf;
using namespace vast;

SUITE("actors")

// This test simulates a scenario with multiple components each in their own
// program instances (e.g., multiple processes or machines). After linking the
// components, an IMPORTER ingests a Bro log and checks that everything went
// well with a simple query.
TEST("distributed")
{
  configuration cfg;
  *cfg["tracker.port"] = 42001;
  *cfg["receiver.name"] = "receiver";
  *cfg["archive.name"] = "archive";
  *cfg["index.name"] = "index";
  *cfg["search.name"] = "search";
  *cfg['v'] = 0;
  *cfg['V'] = 5;

  scoped_actor self;
  bool failed = false;
  message_handler propagate = {
    [](ok_atom) {},
    [&](error const& e)
    {
      failed = true;
      VAST_ERROR("got error: ", e);
    },
    others() >> [&]
    {
      failed = true;
      VAST_ERROR("unexpected message: ", to_string(self->last_dequeued()));
    }};

  // Tracker
  configuration cfg_track{cfg};
  *cfg_track['T'] = true;
  REQUIRE(cfg_track.verify());
  auto t = self->spawn<program>(std::move(cfg_track));
  self->sync_send(t, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);

  // Receiver
  configuration cfg_recv{cfg};
  *cfg_recv['R'] = true;
  REQUIRE(cfg_recv.verify());
  auto r = self->spawn<program, monitored>(std::move(cfg_recv));
  self->sync_send(r, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);

  // Archive
  configuration cfg_arch{cfg};
  *cfg_arch['A'] = true;
  REQUIRE(cfg_arch.verify());
  auto a = self->spawn<program>(std::move(cfg_arch));
  self->sync_send(a, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);

  // Index
  configuration cfg_idx{cfg};
  *cfg_idx['X'] = true;
  REQUIRE(cfg_idx.verify());
  auto x = self->spawn<program>(std::move(cfg_idx));
  self->sync_send(x, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);

  // Search
  configuration cfg_srch{cfg};
  *cfg_srch['S'] = true;
  REQUIRE(cfg_srch.verify());
  auto s = self->spawn<program>(std::move(cfg_srch));
  self->sync_send(s, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);

  VAST_INFO("linking components");
  configuration cfg_link{cfg};
  cfg_link["tracker.link"]->set("receiver", "archive");
  auto l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("receiver", "index");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("search", "archive");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("search", "index");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);

  // Importer
  configuration cfg_imp{cfg};
  *cfg_imp['I'] = "bro";
  *cfg_imp['r'] = m57_day11_18::ftp;
  REQUIRE(cfg.verify());
  REQUIRE(cfg_imp.verify());
  auto i = self->spawn<program, monitored>(std::move(cfg_imp));
  self->sync_send(i, run_atom::value).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});

  // Give the chunks in the pipeline from IMPORTER to RECEIVER some time.
  std::this_thread::sleep_for(std::chrono::milliseconds(800));

  VAST_INFO("checking with a simple query that import went fine");
  self->sync_send(t, tracker_atom::value).await(
    [&](actor const& track)
    {
      self->sync_send(track, get_atom::value, "search").await(
        [&](actor const& srch)
        {
          auto query = ":addr == 192.168.1.105";
          self->sync_send(srch, query_atom::value, self, query).await(
            on_arg_match >> [=](error const& e)
            {
              VAST_ERROR(e);
              REQUIRE(false);
            },
            [&](expression const&, actor const& qry)
            {
              self->send(qry, extract_atom::value, uint64_t{1});
            },
            others() >> [&]
            {
              VAST_ERROR(to_string(self->last_dequeued()));
              REQUIRE(false);
            });
        },
        [=](error const& e)
        {
          VAST_ERROR(e);
          REQUIRE(false);
        });
    },
    others() >> [&]
    {
      VAST_ERROR(to_string(self->last_dequeued()));
      REQUIRE(false);
    });

  VAST_INFO("getting one result");
  bool done = false;
  self->do_receive(
    [&](actor const& /* task */) {},
    [&](progress_atom, double) {},
    [](done_atom) {},
    [&](event const& e)
    {
      VAST_INFO("got event:", e);
      CHECK(e.type().name() == "ftp");
      done = true;
    },
    others() >> [&]
    {
      VAST_ERROR(to_string(self->last_dequeued()));
      REQUIRE(false);
    })
  .until([&] { return done; });

  // We bring down RECEIVER first because it keeps a reference to IDENTIFIER
  // inside TRACKER. If we just killed TRACKER, it would in turn terminate
  // IDENTIFIER and than RECEIVER with an error.
  VAST_INFO("waiting for RECEIVER to terminate");
  self->send_exit(r, exit::done);
  self->receive([&](down_msg const& msg) { REQUIRE(msg.source == r); });

  // Once RECEIVER is down, TRACKER can safely bring DOWN the remaining
  // components.
  VAST_INFO("sending EXIT to TRACKER");
  self->send_exit(t, exit::done);
  self->await_all_other_actors_done();
}
