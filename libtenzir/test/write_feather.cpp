//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/test/test.hpp"

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

using namespace tenzir;

TEST("Nova Feather writer rejects saving and restoring checkpoints") {
  auto const* plugin = plugins::find<OperatorPlugin>("write_feather");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_operator_location);
  auto source = location{17, 29};
  for (auto& spawn : desc.spawns) {
    auto* create = try_as<Spawn<nova::Events, chunk_ptr>>(spawn);
    if (not create) {
      continue;
    }
    for (auto loading : {false, true}) {
      auto args = desc.make_args();
      (*desc.set_operator_location)(args, source);
      auto writer = (*create)(std::move(args));
      auto bytes = caf::byte_buffer{};
      auto serializer = caf::binary_serializer{bytes};
      auto deserializer = caf::binary_deserializer{bytes};
      auto serde = loading ? Serde{deserializer} : Serde{serializer};
      auto rejected = false;
      try {
        writer->snapshot(serde);
      } catch (diagnostic const& error) {
        rejected = true;
        CHECK_EQUAL(error.severity, severity::error);
        CHECK_EQUAL(error.message,
                    "write_feather does not support checkpoints yet");
        REQUIRE_EQUAL(error.annotations.size(), size_t{1});
        CHECK(error.annotations.front().primary);
        CHECK_EQUAL(error.annotations.front().source, source);
      }
      CHECK(rejected);
      CHECK(bytes.empty());
    }
    return;
  }
  FAIL("Nova Feather writer is not registered");
}
