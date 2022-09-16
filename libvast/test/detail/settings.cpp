//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE settings

#include "vast/detail/settings.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

TEST(return error when passed config value is not a list type) {
  caf::config_value in{caf::config_value::integer{5}};
  std::string_view error_msg{"some_err"};
  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in, error_msg);
  REQUIRE(!out);
  auto context = out.error().context();
  std::string context_value;
  auto f = caf::message_handler{[&](std::string& str) {
    context_value = str;
  }};
  REQUIRE(f(context));
  CHECK_EQUAL(context_value, error_msg);
}

TEST(return error when passed config value list has different type than
       passed template param) {
  std::vector list_values{caf::config_value{caf::config_value::integer{5}},
                          caf::config_value{caf::config_value::string{"strr"}}};
  caf::config_value in{list_values};

  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in, "err");
  CHECK(!out);
}

TEST(unpack list properly) {
  std::vector list_values{caf::config_value{caf::config_value::integer{5}},
                          caf::config_value{caf::config_value::integer{15}}};
  caf::config_value in{list_values};

  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in, "err");
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), list_values.size());
  CHECK_EQUAL(out->front(), caf::config_value::integer{5});
  CHECK_EQUAL(out->back(), caf::config_value::integer{15});
}
