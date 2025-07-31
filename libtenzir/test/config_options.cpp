//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/config_options.hpp"

#include "tenzir/detail/settings.hpp"
#include "tenzir/test/test.hpp"

TEST("parse list option with no character after equality sign") {
  tenzir::config_options sut;
  sut.add<std::vector<std::string>>("opt", "desc");
  auto settings = caf::settings{};
  const auto input = std::vector<std::string>{"--opt="};
  const auto result = sut.parse(settings, input);
  REQUIRE_EQUAL(result.first, caf::pec::success);
  REQUIRE_EQUAL(settings.count("opt"), 1u);
  const auto out = tenzir::detail::unpack_config_list_to_vector<std::string>(
    settings["opt"]);
  REQUIRE(out);
  CHECK(out->empty());
}

TEST("parse list option with one arg") {
  tenzir::config_options sut;
  sut.add<std::vector<std::string>>("opt", "desc");
  auto settings = caf::settings{};
  const auto input = std::vector<std::string>{"--opt=opt1"};
  const auto result = sut.parse(settings, input);
  REQUIRE_EQUAL(result.first, caf::pec::success);
  REQUIRE_EQUAL(settings.count("opt"), 1u);
  const auto out = tenzir::detail::unpack_config_list_to_vector<std::string>(
    settings["opt"]);
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), 1u);
  CHECK_EQUAL(out->front(), "opt1");
}

TEST("parse list option with one arg in qoutation marks") {
  tenzir::config_options sut;
  sut.add<std::vector<std::string>>("opt", "desc");
  auto settings = caf::settings{};
  const auto input = std::vector<std::string>{"--opt=\"opt1\""};
  const auto result = sut.parse(settings, input);
  REQUIRE_EQUAL(result.first, caf::pec::success);
  REQUIRE_EQUAL(settings.count("opt"), 1u);
  const auto out = tenzir::detail::unpack_config_list_to_vector<std::string>(
    settings["opt"]);
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), 1u);
  CHECK_EQUAL(out->front(), "opt1");
}

TEST("parse list option with comma separated format") {
  tenzir::config_options sut;
  sut.add<std::vector<std::string>>("opt", "desc");
  auto settings = caf::settings{};
  const auto input = std::vector<std::string>{"--opt=opt1,opt2"};
  const auto result = sut.parse(settings, input);
  REQUIRE_EQUAL(result.first, caf::pec::success);
  REQUIRE_EQUAL(settings.count("opt"), 1u);
  const auto out = tenzir::detail::unpack_config_list_to_vector<std::string>(
    settings["opt"]);
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), 2u);
  CHECK_EQUAL(out->front(), "opt1");
  CHECK_EQUAL(out->back(), "opt2");
}

TEST("parse list option with comma separated format in qoutation marks") {
  tenzir::config_options sut;
  sut.add<std::vector<std::string>>("opt", "desc");
  auto settings = caf::settings{};
  const auto input = std::vector<std::string>{"--opt=\"opt1,opt2\""};
  const auto result = sut.parse(settings, input);
  REQUIRE_EQUAL(result.first, caf::pec::success);
  REQUIRE_EQUAL(settings.count("opt"), 1u);
  const auto out = tenzir::detail::unpack_config_list_to_vector<std::string>(
    settings["opt"]);
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), 2u);
  CHECK_EQUAL(out->front(), "opt1");
  CHECK_EQUAL(out->back(), "opt2");
}
