//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/active_partition.hpp"

#include "tenzir/error.hpp"
#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/nova_active_partition.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/resource.hpp"
#include "tenzir/taxonomies.hpp"
#include "tenzir/test/test.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/test/fixture/deterministic.hpp>

using namespace tenzir;

namespace {

struct test_store final : store_actor_plugin {
  store_builder_actor builder;

  auto name() const -> std::string override {
    return "test";
  }

  auto make_store_builder(filesystem_actor, const uuid&, std::string) const
    -> caf::expected<builder_and_header> override {
    return builder_and_header{builder, chunk::copy(std::string{"header"})};
  }

  auto make_store(filesystem_actor, std::span<const std::byte>,
                  caf::message_priority) const
    -> caf::expected<store_actor> override {
    return store_actor{};
  }
};

} // namespace

TEST("persistence reports only successfully written synopsis bytes") {
  for (const auto synopsis_succeeds : {false, true}) {
    auto f = caf::test::fixture::deterministic{};
    auto synopsis_write = caf::typed_response_promise<atom::ok>{};
    auto synopsis_size = size_t{0};
    auto index_size = size_t{0};
    auto result = partition_synopsis_ptr{};
    auto fs = f.sys.spawn(
      [&](filesystem_actor::pointer self) -> filesystem_actor::behavior_type {
        return {caf::partial_behavior_init,
                [&, self](atom::write, const std::filesystem::path& path,
                          const chunk_ptr& chunk) -> caf::result<atom::ok> {
                  if (path.extension() == ".mdx") {
                    synopsis_size = chunk->size();
                    synopsis_write = self->make_response_promise<atom::ok>();
                    return synopsis_write;
                  }
                  index_size = chunk->size();
                  return atom::ok_v;
                }};
      });
    auto plugin = test_store{};
    plugin.builder = f.sys.spawn([]() -> store_builder_actor::behavior_type {
      return {caf::partial_behavior_init, [](atom::persist) -> resource {
                return {.url = "file://test.feather", .size = 512};
              }};
    });
    const auto schema = type{"test", record_type{{"msg", string_type{}}}};
    auto partition = f.sys.spawn(active_partition, schema, uuid::random(), fs,
                                 caf::settings{}, index_config{}, &plugin,
                                 std::make_shared<taxonomies>());
    auto client = f.sys.spawn([&](caf::event_based_actor* self) {
      self
        ->mail(atom::persist_v, std::filesystem::path{"test.index"},
               std::filesystem::path{"test.mdx"})
        .request(partition, caf::infinite)
        .then(
          [&](partition_synopsis_ptr synopsis) {
            result = std::move(synopsis);
          },
          [](const caf::error& error) {
            FAIL("partition persistence failed: {}", error);
          });
      return caf::behavior{};
    });
    f.dispatch_messages();
    REQUIRE(synopsis_write.pending());
    CHECK_GREATER(synopsis_size, size_t{0});
    CHECK_EQUAL(index_size, size_t{0});
    CHECK(not result);
    if (synopsis_succeeds) {
      synopsis_write.deliver(atom::ok_v);
    } else {
      synopsis_write.deliver(caf::make_error(
        ec::filesystem_error, "injected synopsis write failure"));
    }
    f.dispatch_messages();
    REQUIRE(result);
    CHECK_EQUAL(result->sketches_file.size,
                synopsis_succeeds ? synopsis_size : size_t{0});
    CHECK_EQUAL(result->indexes_file.size, index_size);
    CHECK_GREATER(index_size, size_t{0});
    CHECK_EQUAL(result->store_file.size, size_t{512});
    f.inject_exit(partition);
    f.inject_exit(plugin.builder);
    f.inject_exit(fs);
    f.inject_exit(client);
  }
}

TEST("shape-grouped partition persists independent typed outputs") {
  auto f = caf::test::fixture::deterministic{};
  auto written = std::vector<table_slice>{};
  auto fs = f.sys.spawn(
    [](filesystem_actor::pointer) -> filesystem_actor::behavior_type {
      return {caf::partial_behavior_init,
              [](atom::write, std::filesystem::path const&,
                 chunk_ptr const&) -> atom::ok {
                return atom::ok_v;
              }};
    });
  auto plugin = test_store{};
  plugin.builder = f.sys.spawn([&]() -> store_builder_actor::behavior_type {
    return {caf::partial_behavior_init,
            [&](table_slice slice) {
              written.push_back(std::move(slice));
            },
            [](atom::persist) -> resource {
              return {.url = "file://test.feather", .size = 512};
            }};
  });
  auto actor
    = f.sys.spawn(nova_active_partition, std::string{"test"}, false,
                  partition_paths::relative(), fs, caf::settings{},
                  index_config{}, &plugin, std::make_shared<taxonomies>());
  auto builder = nova::ArrayBuilder<nova::Record>{};
  builder.record().field("x").data(int64_t{1});
  builder.record().field("x").data(std::string_view{"two"});
  auto events = nova::Events{builder.finish(), nova::storage::BitMap{2, true},
                             nova::Events::Meta::make_empty(2, "test")};
  auto result = Option<NovaPersistResult>{};
  auto snapshot = Option<std::vector<nova::Events>>{};
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self->mail(events, events.mask).send(actor);
    self->mail(atom::get_v)
      .request(actor, caf::infinite)
      .then(
        [&, self](std::vector<nova::Events> batches) {
          CHECK(written.empty());
          snapshot.emplace(std::move(batches));
          self->mail(atom::persist_v)
            .request(actor, caf::infinite)
            .then(
              [&](NovaPersistResult persisted) {
                result.emplace(std::move(persisted));
              },
              [](caf::error const& error) {
                FAIL("partition persistence failed: {}", error);
              });
        },
        [](caf::error const& error) {
          FAIL("partition snapshot failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(result.is_some());
  REQUIRE(snapshot.is_some());
  REQUIRE_EQUAL(snapshot->size(), 2u);
  auto seen = size_t{0};
  for (auto const& batch : *snapshot) {
    CHECK_EQUAL(batch.length(), events.length());
    for (auto row : nova::storage::true_bits(batch.mask)) {
      CHECK(nova::materialize_legacy(batch.data.get(row))
            == nova::materialize_legacy(events.data.get(row)));
      ++seen;
    }
  }
  CHECK_EQUAL(seen, 2u);
  REQUIRE_EQUAL(result->outputs.size(), 2u);
  CHECK(result->outputs[0].uuid != result->outputs[1].uuid);
  for (auto const& output : result->outputs) {
    CHECK(not output.failure.valid());
    CHECK_EQUAL(output.synopsis->events, 1u);
  }
  REQUIRE_EQUAL(written.size(), 2u);
  CHECK(written[0].schema() != written[1].schema());
  f.inject_exit(actor);
  f.inject_exit(plugin.builder);
  f.inject_exit(fs);
  f.inject_exit(client);
}

TEST("shape-grouped partition reports a failed child without losing siblings") {
  auto f = caf::test::fixture::deterministic{};
  auto fs = f.sys.spawn(
    [](filesystem_actor::pointer) -> filesystem_actor::behavior_type {
      return {caf::partial_behavior_init,
              [](atom::write, std::filesystem::path const&,
                 chunk_ptr const&) -> atom::ok {
                return atom::ok_v;
              }};
    });
  auto persist_count = size_t{0};
  auto plugin = test_store{};
  plugin.builder = f.sys.spawn([&]() -> store_builder_actor::behavior_type {
    return {caf::partial_behavior_init, [](table_slice) {},
            [&](atom::persist) -> caf::result<resource> {
              if (++persist_count == 1) {
                return caf::make_error(ec::filesystem_error,
                                       "injected store failure");
              }
              return resource{.url = "file://test.feather", .size = 512};
            }};
  });
  auto actor
    = f.sys.spawn(nova_active_partition, std::string{"test"}, false,
                  partition_paths::relative(), fs, caf::settings{},
                  index_config{}, &plugin, std::make_shared<taxonomies>());
  auto builder = nova::ArrayBuilder<nova::Record>{};
  builder.record().field("x").data(int64_t{1});
  builder.record().field("x").data(std::string_view{"two"});
  auto events = nova::Events{builder.finish(), nova::storage::BitMap{2, true},
                             nova::Events::Meta::make_empty(2, "test")};
  auto result = Option<NovaPersistResult>{};
  auto client = f.sys.spawn([&](caf::event_based_actor* self) {
    self->mail(events, events.mask).send(actor);
    self->mail(atom::persist_v)
      .request(actor, caf::infinite)
      .then(
        [&](NovaPersistResult persisted) {
          result.emplace(std::move(persisted));
        },
        [](caf::error const& error) {
          FAIL("partition persistence failed: {}", error);
        });
    return caf::behavior{};
  });
  f.dispatch_messages();
  REQUIRE(result.is_some());
  REQUIRE_EQUAL(result->outputs.size(), 2u);
  auto successes = size_t{0};
  auto failures = size_t{0};
  for (auto const& output : result->outputs) {
    if (output.failure.valid()) {
      ++failures;
    } else {
      ++successes;
      CHECK_EQUAL(output.synopsis->events, 1u);
    }
  }
  CHECK_EQUAL(successes, 1u);
  CHECK_EQUAL(failures, 1u);
  CHECK_EQUAL(persist_count, 2u);
  f.inject_exit(actor);
  f.inject_exit(plugin.builder);
  f.inject_exit(fs);
  f.inject_exit(client);
}
