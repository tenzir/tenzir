
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE cef

#include "cef/parse.hpp"

#include <vast/concept/convertible/to.hpp>
#include <vast/test/test.hpp>

#include <caf/test/dsl.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>

using namespace vast;

namespace {

auto sample
  = R"__(CEF:0|ArcSight|ArcSight|6.0.3.6664.0|agent:030|Agent [test] type [testalertng] started|Low|eventId=1 mrt=1396328238973 categorySignificance=/Normal categoryBehavior=/Execute/Start categoryDeviceGroup=/Application catdt=Security Mangement categoryOutcome=/Success categoryObject=/Host/Application/Service art=1396328241038 cat=/Agent/Started deviceSeverity=Warning rt=1396328238937 fileType=Agent cs2=<Resource ID\="3DxKlG0UBABCAA0cXXAZIwA\=\="/> c6a4=fe80:0:0:0:495d:cc3c:db1a:de71 cs2Label=Configuration Resource c6a4Label=Agent IPv6 Address ahost=SKEELES10 agt=888.99.100.1 agentZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918: 888.99.0.0-888.200.255.255 av=6.0.3.6664.0 atz=Australia/Sydney aid=3DxKlG0UBABCAA0cXXAZIwA\=\= at=testalertng dvchost=SKEELES10 dvc=888.99.100.1 deviceZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918:888.99.0.0-888.200.255.255 dtz=Australia/Sydney _cefVer=0.1)__";

auto to_extension(std::string_view str) {
  return unbox(plugins::cef::parse_extension(str));
}

auto to_message(std::string_view str) {
  return unbox(to<plugins::cef::message>(str));
}

} // namespace

TEST(parse extension with newlines) {
  constexpr auto extension_with_linebreaks = R"__(foo=a\nb\rc bar=a\\\nb)__";
  auto kvps = to_extension(extension_with_linebreaks);
  REQUIRE_EQUAL(kvps.size(), 2u);
  CHECK_EQUAL(kvps[0].second, "a\nb\nc"); // \r unescapes to \n.
  CHECK_EQUAL(kvps[1].second, "a\\\nb");
}

TEST(parse extension equal signs) {
  constexpr auto extension_with_equal_sign = R"__(foo=\=\=\= bar=a \= b)__";
  auto kvps = to_extension(extension_with_equal_sign);
  REQUIRE_EQUAL(kvps.size(), 2u);
  CHECK_EQUAL(kvps[0].second, "==="); // \r unescapes to \n.
  CHECK_EQUAL(kvps[1].second, "a = b");
}

TEST(parse sample) {
  auto msg = to_message(sample);
  CHECK_EQUAL(msg.cef_version, 0);
  CHECK_EQUAL(msg.device_vendor, "ArcSight");
  CHECK_EQUAL(msg.device_product, "ArcSight");
  CHECK_EQUAL(msg.device_version, "6.0.3.6664.0");
  CHECK_EQUAL(msg.signature_id, "agent:030");
  CHECK_EQUAL(msg.name, "Agent [test] type [testalertng] started");
  CHECK_EQUAL(msg.severity, "Low");
  REQUIRE_EQUAL(msg.extension.size(), 29u);
  CHECK_EQUAL(msg.extension[0].first, "eventId");
  CHECK_EQUAL(msg.extension[0].second, "1");
  CHECK_EQUAL(msg.extension[28].first, "_cefVer");
  CHECK_EQUAL(msg.extension[28].second, "0.1");
}
