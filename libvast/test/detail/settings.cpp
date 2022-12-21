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
  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in);
  CHECK(!out);
}

TEST(return error when passed config value list has different type than
       passed template param) {
  std::vector list_values{caf::config_value{caf::config_value::integer{5}},
                          caf::config_value{caf::config_value::string{"strr"}}};
  caf::config_value in{list_values};

  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in);
  CHECK(!out);
}

TEST(unpack list properly) {
  std::vector list_values{caf::config_value{caf::config_value::integer{5}},
                          caf::config_value{caf::config_value::integer{15}}};
  caf::config_value in{list_values};

  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in);
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), list_values.size());
  CHECK_EQUAL(out->front(), caf::config_value::integer{5});
  CHECK_EQUAL(out->back(), caf::config_value::integer{15});
}

TEST(unpack nested settings properly) {
  caf::settings settings;
  caf::config_value::list list{caf::config_value{20}};
  caf::put(settings, "outer.inner", list);
  caf::actor_system_config in;
  in.content = settings;
  const auto out
    = vast::detail::unpack_config_list_to_vector<caf::config_value::integer>(
      in, "outer.inner");
  REQUIRE(out);
  REQUIRE_EQUAL(out->size(), std::size_t{1});
  CHECK_EQUAL(out->front(), 20);
}

TEST(convert_to_caf_compatible_list_arg returns empty string when no equality
       sign is in input string) {
  CHECK_EQUAL("", vast::detail::convert_to_caf_compatible_list_arg("--temp"));
}

TEST(convert_to_caf_compatible_list_arg returns input string when no value is
       listed after the equality sign) {
  const auto in = std::string{"--temp="};
  CHECK_EQUAL(in, vast::detail::convert_to_caf_compatible_list_arg(in));
}

TEST(convert_to_caf_compatible_list_arg one value) {
  CHECK_EQUAL("--opt=[\"val\"]",
              vast::detail::convert_to_caf_compatible_list_arg("--opt=val"));
}

TEST(convert_to_caf_compatible_list_arg three values) {
  const auto in = std::string{"--opt=val1,val2,val3"};
  CHECK_EQUAL("--opt=[\"val1\",\"val2\",\"val3\"]",
              vast::detail::convert_to_caf_compatible_list_arg(in));
}