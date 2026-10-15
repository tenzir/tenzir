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
#include <fmt/format.h>

#include <string_view>

using namespace tenzir;

namespace {

/// Checks that every instance of `name` that the spawn `Output` creates
/// rejects saving and restoring a checkpoint.
template <class Output>
auto check_rejects_checkpoints(std::string_view name) -> void {
  auto const* plugin = plugins::find<OperatorPlugin>(name);
  REQUIRE(plugin);
  auto desc = plugin->describe();
  REQUIRE(desc.set_operator_location);
  auto source = location{17, 29};
  for (auto& spawn : desc.spawns) {
    auto* create = try_as<Spawn<void, Output>>(spawn);
    if (not create) {
      continue;
    }
    for (auto loading : {false, true}) {
      auto args = desc.make_args();
      (*desc.set_operator_location)(args, source);
      auto op = (*create)(std::move(args));
      auto bytes = caf::byte_buffer{};
      auto serializer = caf::binary_serializer{bytes};
      auto deserializer = caf::binary_deserializer{bytes};
      auto serde = loading ? Serde{deserializer} : Serde{serializer};
      auto rejected = false;
      try {
        op->snapshot(serde);
      } catch (diagnostic const& error) {
        rejected = true;
        CHECK_EQUAL(error.severity, severity::error);
        CHECK_EQUAL(error.message,
                    fmt::format("{} does not support checkpoints yet", name));
        REQUIRE_EQUAL(error.annotations.size(), size_t{1});
        CHECK(error.annotations.front().primary);
        CHECK_EQUAL(error.annotations.front().source, source);
      }
      CHECK(rejected);
      CHECK(bytes.empty());
    }
    return;
  }
  FAIL("{} has no spawn for this output", name);
}

} // namespace

TEST("from_clickhouse rejects saving and restoring checkpoints") {
  check_rejects_checkpoints<nova::Events>("from_clickhouse");
  check_rejects_checkpoints<table_slice>("from_clickhouse");
}

TEST("from_hydrolix rejects saving and restoring checkpoints") {
  check_rejects_checkpoints<nova::Events>("from_hydrolix");
  check_rejects_checkpoints<table_slice>("from_hydrolix");
}
