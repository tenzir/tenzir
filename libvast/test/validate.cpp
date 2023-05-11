//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/validate.hpp"

#include "vast/data.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"

namespace {

using namespace vast;

// clang-format off

auto test_schema = record_type{
    {"time", record_type{
      {"interval", duration_type{}},
      {"rules", list_type{record_type{
        {"name", string_type{}},
      }}},
    }},
    {"space", record_type{
      {"mode", string_type{}},
      {"weights", list_type{record_type{
        {"types", list_type{string_type{}}},
      }}},
    }}
  };

auto test_layout2 = record_type{
  {"struct", record_type{
    {"foo", type{string_type{}, {{"required"}}}},
    {"bar", string_type{}}
  }}};

auto test_layout3 = record_type{
  {"struct", type{record_type{
    {"dummy", string_type{}}
  }, {{"opaque"}}}},
};

// clang-format on

} // namespace

TEST(exhaustive validation) {
  auto data = vast::from_yaml(R"_(
    time:
      interval: 24 hours
      rules:
        - name: rule1
    space:
      mode: depeche
      weights:
        - types: ["type1", "type2"]
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::permissive), valid);
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::strict), valid);
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::exhaustive), valid);
}

TEST(no rules configured) {
  auto data = vast::from_yaml(R"_(
    time:
      interval: 1 day
      rules: []
    space:
      mode: weighted-age
      weights: []
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  // Should be fine
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::strict), valid);
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::exhaustive), valid);
}

TEST(extra field) {
  auto data = vast::from_yaml(R"_(
    time:
      rules:
        - name: foo
          jkl: false
    asdf: true
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  CHECK_EQUAL(vast::validate(*data, test_schema, validate::permissive), valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_schema, validate::strict), valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_schema, validate::exhaustive),
                  valid);
}

TEST(incompatible field) {
  auto data = vast::from_yaml(R"_(
    space:
      weights:
        - # !! types should be a list
          types: zeek.conn
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  CHECK_NOT_EQUAL(vast::validate(*data, test_schema, validate::permissive),
                  valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_schema, validate::strict), valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_schema, validate::exhaustive),
                  valid);
}

TEST(required field) {
  auto data = vast::from_yaml(R"_(
    struct:
      bar: no
      # !! missing required field 'foo'
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  CHECK_NOT_EQUAL(vast::validate(*data, test_layout2, validate::permissive),
                  valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_layout2, validate::strict), valid);
  CHECK_NOT_EQUAL(vast::validate(*data, test_layout2, validate::exhaustive),
                  valid);
}

TEST(opaque fields) {
  auto data = vast::from_yaml(R"_(
    struct:
      bar: no
      baz: yes
  )_");
  REQUIRE_NOERROR(data);
  auto valid = caf::error{};
  CHECK_EQUAL(vast::validate(*data, test_layout3, validate::permissive), valid);
  CHECK_EQUAL(vast::validate(*data, test_layout3, validate::strict), valid);
  CHECK_EQUAL(vast::validate(*data, test_layout3, validate::exhaustive), valid);
  auto data2 = vast::from_yaml(R"_(
    # !! 'struct' should be a record
    struct: foo
  )_");
  CHECK_NOT_EQUAL(vast::validate(*data2, test_layout3, validate::permissive),
                  valid);
  // Invalid, only records may have 'opaque' label.
  auto invalid_layout = record_type{
    {"struct", type{string_type{}, {{"opaque"}}}},
  };
  CHECK_NOT_EQUAL(vast::validate(*data, test_layout2, validate::exhaustive),
                  valid);
}
