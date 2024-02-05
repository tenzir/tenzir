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

void test_metrics_plugin(const std::string& plugin_name) {
  MESSAGE(plugin_name);
  auto const* plugin = plugins::find<metrics_plugin>(plugin_name);
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

TEST(metrics) {
  test_metrics_plugin("process");
  // We don't tes the disk plugin as that sporadically fails in CI.
  // test_metrics_plugin("disk");
  test_metrics_plugin("cpu");
#ifdef _SC_AVPHYS_PAGES
  // The 'memory' health plugin isn't supported
  // on all platforms.
  test_metrics_plugin("memory");
#endif
}
