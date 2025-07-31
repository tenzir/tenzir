//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/padded_buffer.hpp"

#include "tenzir/test/test.hpp"

#include <string_view>

namespace {
constexpr auto padding_len = 3u;
constexpr auto padding_val = 'S';
using sut_type = tenzir::detail::padded_buffer<padding_len, padding_val>;
} // namespace

TEST("Append a string_view with correct padding") {
  auto sut = sut_type{};
  constexpr auto input = std::string_view{"in poot"};
  sut.append(input);
  REQUIRE(sut);
  const auto view = sut.view();
  CHECK_EQUAL(view, input);
  const auto padding_view
    = std::string_view{view.data() + view.length(), padding_len};
  CHECK_EQUAL(padding_view, std::string_view{"SSS"});
  sut.reset();
  CHECK(sut.view().empty());
}

TEST("Append a string_view twice") {
  auto sut = sut_type{};
  sut.append("one");
  sut.append("two");
  REQUIRE(sut);
  CHECK_EQUAL(sut.view(), "onetwo");
  sut.reset();
  CHECK(sut.view().empty());
}

TEST("truncate") {
  auto sut = sut_type{};
  sut.append("one");
  sut.append("two");
  sut.truncate(3);
  REQUIRE(sut);
  CHECK_EQUAL(sut.view(), "two");
  sut.append("three");
  REQUIRE(sut);
  CHECK_EQUAL(sut.view(), "twothree");
  sut.reset();
  CHECK(sut.view().empty());
}
