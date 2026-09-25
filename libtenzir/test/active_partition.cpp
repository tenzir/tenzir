//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/active_partition.hpp"

#include "tenzir/error.hpp"
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
