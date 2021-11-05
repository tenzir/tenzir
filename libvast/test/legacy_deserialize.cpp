//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE legacy_deserialize

#include "vast/detail/legacy_deserialize.hpp"

#include "vast/as_bytes.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/legacy_type.hpp"
#include "vast/test/test.hpp"

#include <cstddef>
#include <span>
#include <string>

using namespace vast;

TEST(deserialize_string) {
  const std::string str = "test string";

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, str), caf::none);
  CHECK_EQUAL(detail::legacy_deserialize<std::string>(as_bytes(buf)), str);
}

TEST(deserialize_bytes) {
  const std::array bytes = {std::byte{'a'}, std::byte{'c'}};

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, bytes), caf::none);
  CHECK_EQUAL(
    (detail::legacy_deserialize<std::array<std::byte, 2>>(as_bytes(buf))),
    bytes);
}

TEST(deserialize_record_type) {
  const auto r = legacy_record_type{
    {"x",
     legacy_record_type{
       {"y", legacy_record_type{{"z", legacy_integer_type{}},
                                {"k", legacy_bool_type{}}}},
       {"m",
        legacy_record_type{{"y",
                            legacy_record_type{{"a", legacy_address_type{}}}},
                           {"f", legacy_real_type{}}}},
       {"b", legacy_bool_type{}}}},
    {"y", legacy_record_type{{"b", legacy_bool_type{}}}}};

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, r), caf::none);
  CHECK_EQUAL(detail::legacy_deserialize<legacy_record_type>(as_bytes(buf)), r);
}
