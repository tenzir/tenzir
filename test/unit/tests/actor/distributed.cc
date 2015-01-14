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
  message_handler propagate = (
      on(atom("ok")) >> [] {},
      [&](error const& e)
      {
        failed = true;
        VAST_ERROR("got error: ", e);
      },
      others() >> [&]
      {
        failed = true;
        VAST_ERROR("unexpected message: ",
                   to_string(self->last_dequeued()));
      });

  // Tracker
  configuration cfg_track{cfg};
  *cfg_track['T'] = true;
  REQUIRE(cfg_track.verify());
  auto t = spawn<program>(std::move(cfg_track));
  self->sync_send(t, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);

  // Receiver
  configuration cfg_recv{cfg};
  *cfg_recv['R'] = true;
  REQUIRE(cfg_recv.verify());
  auto p = spawn<program>(std::move(cfg_recv));
  self->sync_send(p, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);

  // Archive
  configuration cfg_arch{cfg};
  *cfg_arch['A'] = true;
  REQUIRE(cfg_arch.verify());
  auto a = spawn<program>(std::move(cfg_arch));
  self->sync_send(a, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);

  // Index
  configuration cfg_idx{cfg};
  *cfg_idx['X'] = true;
  REQUIRE(cfg_idx.verify());
  auto x = spawn<program>(std::move(cfg_idx));
  self->sync_send(x, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);

  // Search
  configuration cfg_srch{cfg};
  *cfg_srch['S'] = true;
  REQUIRE(cfg_srch.verify());
  auto s = spawn<program>(std::move(cfg_srch));
  self->sync_send(s, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);

  // Link components
  configuration cfg_link{cfg};
  cfg_link["tracker.link"]->set("receiver", "archive");
  auto l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("receiver", "index");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("search", "archive");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});
  if (failed)
    REQUIRE(false);
  cfg_link["tracker.link"]->set("search", "index");
  l = self->spawn<program, monitored>(cfg_link);
  self->sync_send(l, atom("run")).await(propagate);
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
  self->sync_send(i, atom("run")).await(propagate);
  if (failed)
    REQUIRE(false);
  self->receive([](down_msg const&) {});

  // Give messages in pipeline some time.
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // Check that import went fine with a simple query.
  self->sync_send(t, atom("tracker")).await(
    [&](actor const& track)
    {
      self->sync_send(track, atom("get"), "search").await(
        [&](actor const& srch)
        {
          auto query = ":addr == 192.168.1.105";
          self->sync_send(srch, atom("query"), self, query).await(
            on_arg_match >> [=](error const& e)
            {
              VAST_ERROR(e);
              REQUIRE(false);
            },
            [&](expression const&, actor const& qry)
            {
              self->send(qry, atom("extract"), uint64_t{1});
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

  bool done = false;
  self->do_receive(
    on(atom("progress"), arg_match) >> [&](double, uint64_t) {},
    on(atom("done")) >> [] {},
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

  self->send_exit(t, exit::done);
  self->await_all_other_actors_done();
}
