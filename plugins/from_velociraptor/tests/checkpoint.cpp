//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/test/test.hpp>

#include <caf/binary_deserializer.hpp>
#include <caf/binary_serializer.hpp>

using namespace tenzir;

TEST("from_velociraptor rejects saving and restoring checkpoints") {
  auto const* plugin = plugins::find<OperatorPlugin>("from_velociraptor");
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_operator_location);
  auto source = location{17, 29};
  for (auto& spawn : desc.spawns) {
    auto* create = try_as<Spawn<void, nova::Events>>(spawn);
    if (not create) {
      continue;
    }
    for (auto loading : {false, true}) {
      auto args = desc.make_args();
      (*desc.set_operator_location)(args, source);
      auto operator_ = (*create)(std::move(args));
      auto bytes = caf::byte_buffer{};
      auto serializer = caf::binary_serializer{bytes};
      auto deserializer = caf::binary_deserializer{bytes};
      auto serde = loading ? Serde{deserializer} : Serde{serializer};
      auto rejected = false;
      try {
        operator_->snapshot(serde);
      } catch (diagnostic const& error) {
        rejected = true;
        CHECK_EQUAL(error.severity, severity::error);
        CHECK_EQUAL(error.message,
                    "from_velociraptor does not support checkpoints yet");
        REQUIRE_EQUAL(error.annotations.size(), size_t{1});
        CHECK(error.annotations.front().primary);
        CHECK_EQUAL(error.annotations.front().source, source);
      }
      CHECK(rejected);
      CHECK(bytes.empty());
    }
    return;
  }
  FAIL("from_velociraptor is not registered");
}
