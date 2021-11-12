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

#include <climits>
#include <span>
#include <string>
#include <string_view>

using namespace std::chrono_literals;
using namespace vast;

template <class T>
std::optional<T> ldes(std::vector<char>& buf) {
  return detail::legacy_deserialize<T>(as_bytes(buf));
}

TEST(deserialize_string) {
  const auto str = "test string"s;

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, str), caf::none);
  CHECK_EQUAL(ldes<std::string>(buf), str);
}

TEST(deserialize_integrals) {
  // signed min
  std::vector<char> buf;
  const signed char sc_min = SCHAR_MIN;
  CHECK_EQUAL(detail::serialize(buf, sc_min), caf::none);
  CHECK_EQUAL(ldes<signed char>(buf), sc_min);
  buf.clear();
  const short si_min = SHRT_MIN;
  CHECK_EQUAL(detail::serialize(buf, si_min), caf::none);
  CHECK_EQUAL(ldes<short>(buf), si_min);
  buf.clear();
  const int i_min = INT_MIN;
  CHECK_EQUAL(detail::serialize(buf, i_min), caf::none);
  CHECK_EQUAL(ldes<int>(buf), i_min);
  buf.clear();
  const long li_min = LONG_MIN;
  CHECK_EQUAL(detail::serialize(buf, li_min), caf::none);
  CHECK_EQUAL(ldes<long>(buf), li_min);
  buf.clear();
  const long long lli_min = LLONG_MIN;
  CHECK_EQUAL(detail::serialize(buf, lli_min), caf::none);
  CHECK_EQUAL(ldes<long long>(buf), lli_min);
  // signed max
  buf.clear();
  const signed char sc_max = SCHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, sc_max), caf::none);
  CHECK_EQUAL(ldes<signed char>(buf), sc_max);
  buf.clear();
  const short si_max = SHRT_MAX;
  CHECK_EQUAL(detail::serialize(buf, si_max), caf::none);
  CHECK_EQUAL(ldes<short>(buf), si_max);
  buf.clear();
  const int i_max = INT_MAX;
  CHECK_EQUAL(detail::serialize(buf, i_max), caf::none);
  CHECK_EQUAL(ldes<int>(buf), i_max);
  buf.clear();
  const long li_max = LONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, li_max), caf::none);
  CHECK_EQUAL(ldes<long>(buf), li_max);
  buf.clear();
  const long long lli_max = LLONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, lli_max), caf::none);
  CHECK_EQUAL(ldes<long long>(buf), lli_max);
  // unsigned min
  buf.clear();
  const unsigned char uc_min = 0;
  CHECK_EQUAL(detail::serialize(buf, uc_min), caf::none);
  CHECK_EQUAL(ldes<unsigned char>(buf), uc_min);
  buf.clear();
  const unsigned short usi_min = 0;
  CHECK_EQUAL(detail::serialize(buf, usi_min), caf::none);
  CHECK_EQUAL(ldes<unsigned short>(buf), usi_min);
  buf.clear();
  const unsigned ui_min = 0;
  CHECK_EQUAL(detail::serialize(buf, ui_min), caf::none);
  CHECK_EQUAL(ldes<unsigned>(buf), ui_min);
  buf.clear();
  const unsigned long uli_min = 0;
  CHECK_EQUAL(detail::serialize(buf, uli_min), caf::none);
  CHECK_EQUAL(ldes<unsigned long>(buf), uli_min);
  buf.clear();
  const unsigned long long ulli_min = 0;
  CHECK_EQUAL(detail::serialize(buf, ulli_min), caf::none);
  CHECK_EQUAL(ldes<unsigned long long>(buf), ulli_min);
  // unsigned max
  buf.clear();
  const unsigned char uc_max = UCHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, uc_max), caf::none);
  CHECK_EQUAL(ldes<unsigned char>(buf), uc_max);
  buf.clear();
  const unsigned short usi_max = USHRT_MAX;
  CHECK_EQUAL(detail::serialize(buf, usi_max), caf::none);
  CHECK_EQUAL(ldes<unsigned short>(buf), usi_max);
  buf.clear();
  const unsigned ui_max = UINT_MAX;
  CHECK_EQUAL(detail::serialize(buf, ui_max), caf::none);
  CHECK_EQUAL(ldes<unsigned>(buf), ui_max);
  buf.clear();
  const unsigned long uli_max = ULONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, uli_max), caf::none);
  CHECK_EQUAL(ldes<unsigned long>(buf), uli_max);
  buf.clear();
  const unsigned long long ulli_max = ULLONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, ulli_max), caf::none);
  CHECK_EQUAL(ldes<unsigned long long>(buf), ulli_max);
  // char
  buf.clear();
  const char c_min = CHAR_MIN;
  CHECK_EQUAL(detail::serialize(buf, c_min), caf::none);
  CHECK_EQUAL(ldes<char>(buf), c_min);
  buf.clear();
  const char c_max = CHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, c_max), caf::none);
  CHECK_EQUAL(ldes<char>(buf), c_max);
}

TEST(deserialize_bytes) {
  const std::array bytes{std::byte{'a'}, std::byte{'c'}};

  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, bytes), caf::none);
  CHECK_EQUAL((ldes<std::array<std::byte, 2>>(buf)), bytes);
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
  CHECK_EQUAL(ldes<legacy_record_type>(buf), r);
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
  CHECK_EQUAL(ldes<qualified_record_field>(buf), field);
}

TEST(deserialize_ids) {
  auto values = ids{};
  values.append_bits(true, 20);
  values.append_bits(false, 5);
  values.append_bits(true, 1);
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, values), caf::none);
  CHECK_EQUAL(ldes<ids>(buf), values);
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
  auto ts2 = ldes<std::unique_ptr<vast::synopsis>>(buf);
  REQUIRE_EQUAL(ts2.has_value(), true);
  CHECK_EQUAL(*(ts2.value()), *ts);
}

TEST(deserialize_bool_synopsis) {
  factory<synopsis>::initialize();
  auto bs = factory<synopsis>::make(legacy_bool_type{}, caf::settings{});
  REQUIRE_NOT_EQUAL(bs, nullptr);
  std::vector<char> buf;
  CHECK_EQUAL(detail::serialize(buf, bs), caf::none);
  auto bs2 = ldes<std::unique_ptr<vast::synopsis>>(buf);
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
  auto as2 = ldes<std::unique_ptr<vast::synopsis>>(buf);
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
  auto ss2 = ldes<std::unique_ptr<vast::synopsis>>(buf);
  CHECK_EQUAL(*(ss2.value()), *ss);
}
