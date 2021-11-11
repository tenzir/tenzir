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
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/detail/serialize.hpp"
#include "vast/factory.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"
#include "vast/time_synopsis.hpp"

#include <span>
#include <string>
#include <string_view>

using namespace std::chrono_literals;
using namespace vast;

TEST(deserialize_string) {
  const auto str = "test string"s;

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, str), caf::none);
  CHECK_EQUAL(detail::legacy_deserialize<std::string>(as_bytes(buf)), str);
}

TEST(deserialize_bytes) {
  const std::array bytes{std::byte{'a'}, std::byte{'c'}};

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, bytes), caf::none);
  CHECK_EQUAL(
    (detail::legacy_deserialize<std::array<std::byte, 2>>(as_bytes(buf))),
    bytes);
}

TEST(deserialize_record_type) {
  const auto r = legacy_record_type{
    {
      "x",
      legacy_record_type{
        {"y",
         legacy_record_type{
           {"z", legacy_integer_type{}},
           {"k", legacy_bool_type{}},
         }},
        {"m",
         legacy_record_type{
           {"y",
            legacy_record_type{
              {"a", legacy_address_type{}},
            }},
           {"f", legacy_real_type{}},
         }},
        {"b", legacy_bool_type{}},
      },
    },
    {
      "y",
      legacy_record_type{
        {"b", legacy_bool_type{}},
      },
    },
  };

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, r), caf::none);
  CHECK_EQUAL(detail::legacy_deserialize<legacy_record_type>(as_bytes(buf)), r);
}

TEST(deserialize_qualified_record_field) {
  const auto field = qualified_record_field{
    "zeek.conn",
    record_field{
      "conn.id",
      legacy_address_type{},
    },
  };

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, field), caf::none);
  CHECK_EQUAL(
    (detail::legacy_deserialize<qualified_record_field>(as_bytes(buf))), field);
}

TEST(deserialize_ids) {
  auto values = ids{};
  values.append_bits(true, 20);
  values.append_bits(false, 5);
  values.append_bits(true, 1);
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, values), caf::none);
  CHECK_EQUAL((detail::legacy_deserialize<ids>(as_bytes(buf))), values);
}

auto to_addr_view(std::string_view str) {
  return make_data_view(unbox(to<address>(str)));
}

TEST(deserialize_time_synopsis) {
  using vast::time;
  const time epoch;
  factory<synopsis>::initialize();
  auto ts = factory<synopsis>::make(legacy_time_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(ts, nullptr);
  ts->add(time{epoch + 4s});
  ts->add(time{epoch + 7s});
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, ts), caf::none);
  auto ts2 = detail::legacy_deserialize<std::unique_ptr<vast::synopsis>>(
    as_bytes(buf));
  REQUIRE_EQUAL(ts2.has_value(), true);
  CHECK_EQUAL(*(ts2.value()), *ts);
}

TEST(deserialize_bool_synopsis) {
  factory<synopsis>::initialize();
  auto bs = factory<synopsis>::make(legacy_bool_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(bs, nullptr);
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, bs), caf::none);
  auto bs2 = detail::legacy_deserialize<std::unique_ptr<vast::synopsis>>(
    as_bytes(buf));
  CHECK_EQUAL(*(bs2.value()), *bs);
}

TEST(deserialize_address_synopsis) {
  factory<synopsis>::initialize();
  auto lat
    = legacy_address_type{}.attributes({{"synopsis", "bloomfilter(1,0.1)"}});
  auto as = factory<synopsis>::make(lat, caf::settings{});
  REQUIRE_NOT_EQUAL(as, nullptr);
  as->add(to_addr_view("192.168.0.1"));
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, as), caf::none);
  auto as2 = detail::legacy_deserialize<std::unique_ptr<vast::synopsis>>(
    as_bytes(buf));
  CHECK_EQUAL(*(as2.value()), *as);
}

TEST(deserialize_string_synopsis) {
  factory<synopsis>::initialize();
  auto lst
    = legacy_string_type{}.attributes({{"synopsis", "bloomfilter(1,0.1)"}});
  auto ss = factory<synopsis>::make(lst, caf::settings{});
  REQUIRE_NOT_EQUAL(ss, nullptr);
  ss->add(std::string_view{"192.168.0.1"});
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, ss), caf::none);
  auto ss2 = detail::legacy_deserialize<std::unique_ptr<vast::synopsis>>(
    as_bytes(buf));
  CHECK_EQUAL(*(ss2.value()), *ss);
}
