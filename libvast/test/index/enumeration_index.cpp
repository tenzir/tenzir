//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE value_index

#include "vast/index/enumeration_index.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitmap.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace vast;
using namespace std::string_literals;

TEST(enumeration) {
  auto e = enumeration_type{{{"foo"}, {"bar"}}};
  auto idx = enumeration_index(type{e});
  REQUIRE(idx.append(enumeration{0}));
  REQUIRE(idx.append(enumeration{0}));
  REQUIRE(idx.append(enumeration{1}));
  REQUIRE(idx.append(enumeration{0}));
  auto foo
    = idx.lookup(relational_operator::equal, make_data_view(enumeration{0}));
  REQUIRE_NOERROR(foo);
  CHECK_EQUAL(to_string(*foo), "1101");
  auto bar = idx.lookup(relational_operator::not_equal,
                        make_data_view(enumeration{0}));
  REQUIRE_NOERROR(bar);
  CHECK_EQUAL(to_string(*bar), "0010");
}
