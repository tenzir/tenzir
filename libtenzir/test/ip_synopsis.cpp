//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ip_synopsis.hpp"

#include "tenzir/hash/legacy_hash.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <caf/actor_system.hpp>
#include <caf/actor_system_config.hpp>

using namespace tenzir;

TEST("failed construction") {
  // If there's no type attribute with Bloom filter parameters present,
  // construction fails.
  auto x = make_ip_synopsis<legacy_hash>(type{ip_type{}}, caf::settings{});
  CHECK_EQUAL(x, nullptr);
}
