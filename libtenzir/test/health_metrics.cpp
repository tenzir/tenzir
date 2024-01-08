//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/test.hpp"

#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

using namespace tenzir;

void test_health_metrics_plugin(const std::string& plugin_name) {
  MESSAGE(plugin_name);
  auto const* plugin = plugins::find<health_metrics_plugin>(plugin_name);
  REQUIRE(plugin);
  auto collector = plugin->make_collector();
  REQUIRE_NOERROR(collector);
  auto record = (*collector)();
  REQUIRE_NOERROR(record);
  auto layout = plugin->metric_layout();
  for (auto const& field : layout.fields()) {
    CHECK(record->contains(field.name));
  }
}

TEST(health metrics) {
  test_health_metrics_plugin("health-process");
  test_health_metrics_plugin("health-disk");
  test_health_metrics_plugin("health-cpu");
#ifdef _SC_AVPHYS_PAGES
  // The 'memory' health metrics plugin isn't supported
  // on all platforms.
  test_health_metrics_plugin("health-memory");
#endif
}
