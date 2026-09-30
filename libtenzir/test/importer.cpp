// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/importer.hpp"

#include "tenzir/import_conversion.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/arrow_export.hpp"
#include "tenzir/recent_snapshot.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"

#include <caf/actor_from_state.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/test/fixture/deterministic.hpp>

using namespace tenzir;

TEST("snapshot subscriptions release their terminated receivers") {
  auto f = caf::test::fixture::deterministic{};
  auto resumed = false;
  auto index = f.sys.spawn([&]() -> index_actor::behavior_type {
    return {caf::partial_behavior_init, [](atom::pause, uuid) {},
            [&](atom::resume, uuid) {
              resumed = true;
            },
            [](atom::get, bool) {
              return std::vector<table_slice>{};
            },
            [](atom::get, atom::internal, bool) {
              return std::vector<nova::Events>{};
            }};
  });
  auto importer = f.sys.spawn(caf::actor_from_state<tenzir::importer>, index);
  auto receiver
    = f.sys.spawn([]() -> receiver_actor<nova::Events>::behavior_type {
        return {[](nova::Events) {}};
      });
  auto weak = caf::actor_cast<caf::weak_actor_ptr>(receiver);
  auto ready = false;
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self
      ->mail(atom::get_v, atom::snapshot_v, receiver, false, true, true, false)
      .request(importer, caf::infinite)
      .then(
        [&](NovaRecentSnapshot) {
          ready = true;
        },
        [](caf::error const& error) {
          FAIL("snapshot failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(ready);
  CHECK(not resumed);
  f.inject_exit(receiver);
  receiver = nullptr;
  f.dispatch_messages();
  CHECK(resumed);
  CHECK(not weak.lock());
  f.inject_exit(importer);
  f.inject_exit(index);
  f.inject_exit(client);
}

TEST("snapshot stamps buffered internal slices") {
  auto f = caf::test::fixture::deterministic{};
  auto index = f.sys.spawn([]() -> index_actor::behavior_type {
    return {caf::partial_behavior_init, [](atom::pause, uuid) {},
            [](atom::resume, uuid) {},
            [](atom::get, bool) {
              return std::vector<table_slice>{};
            },
            [](atom::get, atom::internal, bool) {
              return std::vector<nova::Events>{};
            }};
  });
  auto importer = f.sys.spawn(caf::actor_from_state<tenzir::importer>, index);
  auto receiver
    = f.sys.spawn([]() -> receiver_actor<nova::Events>::behavior_type {
        return {[](nova::Events) {}};
      });
  auto builder = series_builder{type{
    "tenzir.metrics.operator",
    record_type{{"value", int64_type{}}},
    {{"internal"}},
  }};
  builder.record().field("value", std::int64_t{42});
  auto slice = builder.finish_assert_one_slice();
  REQUIRE_EQUAL(slice.import_time(), tenzir::time{});
  auto ready = false;
  auto timestamp = tenzir::time{};
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self->mail(slice).send(importer);
    self
      ->mail(atom::get_v, atom::snapshot_v, receiver, true, false, true, false)
      .request(importer, caf::infinite)
      .then(
        [&](NovaRecentSnapshot snapshot) {
          REQUIRE_EQUAL(snapshot.events.size(), size_t{1});
          timestamp = *snapshot.events[0].meta.import_time.get(0);
          ready = true;
        },
        [](caf::error const& error) {
          FAIL("snapshot failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(ready);
  CHECK(timestamp != tenzir::time{});
  f.inject_exit(receiver);
  f.dispatch_messages();
  f.inject_exit(importer);
  f.inject_exit(index);
  f.inject_exit(client);
}

TEST("snapshot preserves buffered Nova import times for table subscribers") {
  auto f = caf::test::fixture::deterministic{};
  auto index = f.sys.spawn([]() -> index_actor::behavior_type {
    return {caf::partial_behavior_init, [](atom::pause, uuid) {},
            [](atom::resume, uuid) {},
            [](atom::get, bool) {
              return std::vector<table_slice>{};
            },
            [](atom::get, atom::internal, bool) {
              return std::vector<nova::Events>{};
            }};
  });
  auto importer = f.sys.spawn(caf::actor_from_state<tenzir::importer>, index);
  auto received = std::vector<table_slice>{};
  auto receiver
    = f.sys.spawn([&]() -> receiver_actor<table_slice>::behavior_type {
        return {[&](table_slice slice) {
          received.push_back(std::move(slice));
        }};
      });
  auto builder = series_builder{type{
    "tenzir.metrics.operator",
    record_type{{"value", int64_type{}}},
    {{"internal"}},
  }};
  builder.record().field("value", std::int64_t{42});
  auto converted = import_table_slice(builder.finish_assert_one_slice());
  REQUIRE(converted);
  auto events = std::move(converted).unwrap();
  auto const import_time = tenzir::time{std::chrono::seconds{1234567890}};
  auto times = nova::ArrayBuilder<nova::Time>{};
  times.data(import_time);
  events.meta.import_time = times.finish();
  auto ready = false;
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self->mail(std::move(events)).send(importer);
    self->mail(atom::get_v, receiver, true, true, false, true)
      .request(importer, caf::infinite)
      .then([](std::vector<table_slice>) {},
            [](caf::error const& error) {
              FAIL("eager subscription failed: {}", error);
            });
    self
      ->mail(atom::get_v, atom::snapshot_v, receiver, true, false, true, false)
      .request(importer, caf::infinite)
      .then(
        [&](recent_snapshot snapshot) {
          REQUIRE_EQUAL(snapshot.events.size(), size_t{1});
          CHECK_EQUAL(snapshot.events[0].import_time(), import_time);
          ready = true;
        },
        [](caf::error const& error) {
          FAIL("snapshot failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(ready);
  REQUIRE_EQUAL(received.size(), size_t{1});
  CHECK_EQUAL(received[0].import_time(), import_time);
  f.inject_exit(receiver);
  f.dispatch_messages();
  f.inject_exit(importer);
  f.inject_exit(index);
  f.inject_exit(client);
}

TEST("eager subscribers receive interleaved shapes in order") {
  auto f = caf::test::fixture::deterministic{};
  auto index = f.sys.spawn([]() -> index_actor::behavior_type {
    return {caf::partial_behavior_init, [](atom::pause, uuid) {},
            [](atom::resume, uuid) {},
            [](atom::get, bool) {
              return std::vector<table_slice>{};
            },
            [](atom::get, atom::internal, bool) {
              return std::vector<nova::Events>{};
            }};
  });
  auto importer = f.sys.spawn(caf::actor_from_state<tenzir::importer>, index);
  auto received = std::vector<table_slice>{};
  auto receiver
    = f.sys.spawn([&]() -> receiver_actor<table_slice>::behavior_type {
        return {[&](table_slice slice) {
          received.push_back(std::move(slice));
        }};
      });
  auto received_nova = std::vector<nova::Events>{};
  auto nova_receiver
    = f.sys.spawn([&]() -> receiver_actor<nova::Events>::behavior_type {
        return {[&](nova::Events events) {
          received_nova.push_back(std::move(events));
        }};
      });
  auto builder = nova::ArrayBuilder<nova::Record>{};
  auto first = builder.record();
  first.field("order").data(int64_t{1});
  first.field("a").data(int64_t{1});
  builder.record();
  auto second = builder.record();
  second.field("order").data(int64_t{2});
  second.field("b").data(int64_t{2});
  auto third = builder.record();
  third.field("order").data(int64_t{3});
  third.field("a").data(int64_t{3});
  auto internal_row = builder.record();
  internal_row.field("order").data(int64_t{4});
  internal_row.field("private").data(int64_t{4});
  auto meta = nova::Events::Meta::make_empty(5, "events");
  auto internal = nova::storage::BitMap::Mutable{5};
  internal.set(4, true);
  meta.internal = nova::Array<nova::Bool>{std::move(internal).finish()};
  auto events = nova::Events{builder.finish(), nova::storage::BitMap{5, true},
                             std::move(meta)};
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self->mail(atom::get_v, receiver, false, true, false, true)
      .request(importer, caf::infinite)
      .then(
        [&, self](std::vector<table_slice>) {
          self
            ->mail(atom::get_v, atom::internal_v, nova_receiver, false, true,
                   false, true)
            .request(importer, caf::infinite)
            .then(
              [&, self](std::vector<nova::Events>) {
                self->mail(std::move(events)).send(importer);
              },
              [](caf::error const& error) {
                FAIL("eager event subscription failed: {}", error);
              });
        },
        [](caf::error const& error) {
          FAIL("eager subscription failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  auto order = std::vector<data>{};
  for (auto const& slice : received) {
    for (auto row = size_t{0}; row < slice.rows(); ++row) {
      order.push_back(materialize(slice.at(row, 0)));
    }
  }
  CHECK_EQUAL(order, (std::vector<data>{int64_t{1}, int64_t{2}, int64_t{3}}));
  auto nova_order = std::vector<data>{};
  for (const auto& events : received_nova) {
    for (const auto& slice : nova::to_table_slices(events)) {
      for (auto row = size_t{0}; row < slice.rows(); ++row) {
        nova_order.push_back(materialize(slice.at(row, 0)));
      }
    }
  }
  CHECK_EQUAL(nova_order,
              (std::vector<data>{int64_t{1}, int64_t{2}, int64_t{3}}));
  f.inject_exit(receiver);
  f.inject_exit(nova_receiver);
  f.dispatch_messages();
  f.inject_exit(importer);
  f.inject_exit(index);
  f.inject_exit(client);
}
