
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE cef

#include <vast/test/test.hpp>

#include <caf/test/dsl.hpp>
#include <fmt/core.h>
#include <fmt/ranges.h>

using namespace vast;

namespace {

auto sample
  = R"__(CEF:0|ArcSight|ArcSight|6.0.3.6664.0|agent:030|Agent [test] type [testalertng] started|Low|eventId=1 mrt=1396328238973 categorySignificance=/Normal categoryBehavior=/Execute/Start categoryDeviceGroup=/Application catdt=Security Mangement categoryOutcome=/Success categoryObject=/Host/Application/Service art=1396328241038 cat=/Agent/Started deviceSeverity=Warning rt=1396328238937 fileType=Agent cs2=<Resource ID\="3DxKlG0UBABCAA0cXXAZIwA\=\="/> c6a4=fe80:0:0:0:495d:cc3c:db1a:de71 cs2Label=Configuration Resource c6a4Label=Agent IPv6 Address ahost=SKEELES10 agt=888.99.100.1 agentZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918: 888.99.0.0-888.200.255.255 av=6.0.3.6664.0 atz=Australia/Sydney aid=3DxKlG0UBABCAA0cXXAZIwA\=\= at=testalertng dvchost=SKEELES10 dvc=888.99.100.1 deviceZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918:888.99.0.0-888.200.255.255 dtz=Australia/Sydney _cefVer=0.1)__";

} // namespace

TEST(parsing) {
  // TODO
}
