//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/format_utils.hpp"

#include "tenzir/plugin.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/plugin.hpp"

using namespace tenzir;

TEST("content type normalization") {
  CHECK_EQUAL(normalize_content_type(" Application/JSON ; charset=utf-8 "),
              "application/json");
  CHECK_EQUAL(normalize_content_type(" \t "), "");
}

TEST("read plugin lookup by content type") {
  auto const* json = plugins::find<operator_factory_plugin>("read_json");
  REQUIRE(json);
  CHECK_EQUAL(read_plugin_for_content_type("application/json; charset=utf-8"),
              json);
  auto const* pcap = plugins::find<operator_factory_plugin>("read_pcap");
  REQUIRE(pcap);
  CHECK_EQUAL(read_plugin_for_content_type("application/vnd.tcpdump.pcap"),
              pcap);
  auto const* feather = plugins::find<operator_factory_plugin>("read_feather");
  REQUIRE(feather);
  CHECK_EQUAL(read_plugin_for_content_type("application/vnd.apache.arrow.file"),
              feather);
  CHECK_EQUAL(read_plugin_for_content_type("text/plain"), nullptr);
}

TEST("read plugin lookup by URL path") {
  auto const* csv = plugins::find<operator_factory_plugin>("read_csv");
  REQUIRE(csv);
  CHECK_EQUAL(read_plugin_for_url_path("/tmp/events.csv"), csv);
  auto const* suricata
    = plugins::find<operator_factory_plugin>("read_suricata");
  REQUIRE(suricata);
  CHECK_EQUAL(read_plugin_for_url_path("/tmp/eve.json"), suricata);
  auto const* zeek = plugins::find<operator_factory_plugin>("read_zeek_json");
  REQUIRE(zeek);
  CHECK_EQUAL(read_plugin_for_url_path("/tmp/zeek.json"), zeek);
  auto const* yaml = plugins::find<operator_factory_plugin>("read_yaml");
  REQUIRE(yaml);
  CHECK_EQUAL(read_plugin_for_url_path("/tmp/config.yml"), yaml);
  CHECK_EQUAL(read_plugin_for_url_path("/tmp/events.log"), nullptr);
}
