//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/mac.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("rendering") {
  auto xs = std::array<uint8_t, 6>{0x01, 0x23, 0x45, 0x67, 0x89, 0xAB};
  auto m = mac{as_bytes(xs)};
  CHECK_EQUAL(fmt::to_string(m), "01-23-45-67-89-AB");
}

TEST("universal") {
  auto xs = std::array<uint8_t, 6>{0x01, 0x23, 0b0000010, 0x67, 0x89, 0xAB};
  auto m0 = mac{as_bytes(xs)};
  CHECK(m0.universal());
  auto ys = std::array<uint8_t, 6>{0x01, 0x23, 0b0000101, 0x67, 0x89, 0xAB};
  auto m1 = mac{as_bytes(ys)};
  CHECK(not m1.universal());
}

TEST("unicast") {
  auto xs = std::array<uint8_t, 6>{0x01, 0x23, 0b0000001, 0x67, 0x89, 0xAB};
  auto m0 = mac{as_bytes(xs)};
  CHECK(m0.unicast());
  auto ys = std::array<uint8_t, 6>{0x01, 0x23, 0b0000110, 0x67, 0x89, 0xAB};
  auto m1 = mac{as_bytes(ys)};
  CHECK(not m1.unicast());
}
