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
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/synopsis_factory.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/type.hpp"

#include <climits>
#include <span>
#include <string>
#include <string_view>

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace vast;

template <class... Ts>
  requires(!std::is_rvalue_reference_v<Ts> && ...)
bool ldes(caf::byte_buffer& buf, Ts&... xs) {
  return detail::legacy_deserialize(buf, xs...);
}

TEST(string) {
  // serialize
  const auto str = "test string"s;
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, str), true);
  // deserialize
  auto str2 = ""s;
  REQUIRE(ldes(buf, str2));
  CHECK_EQUAL(str, str2);
}

TEST(integrals) {
  // signed char min
  caf::byte_buffer buf;
  const signed char sc_min = SCHAR_MIN;
  CHECK_EQUAL(detail::serialize(buf, sc_min), true);
  signed char sc_min2 = SCHAR_MAX;
  REQUIRE(ldes(buf, sc_min2));
  CHECK_EQUAL(sc_min, sc_min2);
  // signed short int min
  buf.clear();
  const short si_min = SHRT_MIN;
  CHECK_EQUAL(detail::serialize(buf, si_min), true);
  short si_min2 = SHRT_MAX;
  REQUIRE(ldes(buf, si_min2));
  CHECK_EQUAL(si_min, si_min2);
  // signed int min
  buf.clear();
  const int i_min = INT_MIN;
  CHECK_EQUAL(detail::serialize(buf, i_min), true);
  int i_min2 = INT_MAX;
  REQUIRE(ldes(buf, i_min2));
  CHECK_EQUAL(i_min, i_min2);
  // signed long int min
  buf.clear();
  const long li_min = LONG_MIN;
  CHECK_EQUAL(detail::serialize(buf, li_min), true);
  long li_min2 = LONG_MAX;
  REQUIRE(ldes(buf, li_min2));
  CHECK_EQUAL(li_min, li_min2);
  // signed long long int min
  buf.clear();
  const long long lli_min = LLONG_MIN;
  CHECK_EQUAL(detail::serialize(buf, lli_min), true);
  long long lli_min2 = LLONG_MAX;
  REQUIRE(ldes(buf, lli_min2));
  CHECK_EQUAL(lli_min, lli_min2);
  // signed char max
  buf.clear();
  const signed char sc_max = SCHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, sc_max), true);
  signed char sc_max2 = SCHAR_MIN;
  REQUIRE(ldes(buf, sc_max2));
  CHECK_EQUAL(sc_max, sc_max2);
  // signed short int max
  buf.clear();
  const short si_max = SHRT_MAX;
  CHECK_EQUAL(detail::serialize(buf, si_max), true);
  short si_max2 = SHRT_MIN;
  REQUIRE(ldes(buf, si_max2));
  CHECK_EQUAL(si_max, si_max2);
  // signed int max
  buf.clear();
  const int i_max = INT_MAX;
  CHECK_EQUAL(detail::serialize(buf, i_max), true);
  int i_max2 = INT_MIN;
  REQUIRE(ldes(buf, i_max2));
  CHECK_EQUAL(i_max, i_max2);
  // signed long int max
  buf.clear();
  const long li_max = LONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, li_max), true);
  long li_max2 = LONG_MIN;
  REQUIRE(ldes(buf, li_max2));
  CHECK_EQUAL(li_max, li_max2);
  buf.clear();
  // signed long long int max
  const long long lli_max = LLONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, lli_max), true);
  long long lli_max2 = LLONG_MIN;
  REQUIRE(ldes(buf, lli_max2));
  CHECK_EQUAL(lli_max, lli_max2);
  buf.clear();
  // unsigned char min
  buf.clear();
  const unsigned char uc_min = 0;
  CHECK_EQUAL(detail::serialize(buf, uc_min), true);
  unsigned char uc_min2 = UCHAR_MAX;
  REQUIRE(ldes(buf, uc_min2));
  CHECK_EQUAL(uc_min, uc_min2);
  // unsigned short int min
  buf.clear();
  const unsigned short usi_min = 0;
  CHECK_EQUAL(detail::serialize(buf, usi_min), true);
  unsigned short usi_min2 = USHRT_MAX;
  REQUIRE(ldes(buf, usi_min2));
  CHECK_EQUAL(usi_min, usi_min2);
  // unsigned int min
  buf.clear();
  const unsigned ui_min = 0;
  CHECK_EQUAL(detail::serialize(buf, ui_min), true);
  unsigned ui_min2 = UINT_MAX;
  REQUIRE(ldes(buf, ui_min2));
  CHECK_EQUAL(ui_min, ui_min2);
  // unsigned long int min
  buf.clear();
  const unsigned long uli_min = 0;
  CHECK_EQUAL(detail::serialize(buf, uli_min), true);
  unsigned long uli_min2 = ULONG_MAX;
  REQUIRE(ldes(buf, uli_min2));
  CHECK_EQUAL(uli_min, uli_min2);
  // unsigned long long int min
  buf.clear();
  const unsigned long long ulli_min = 0;
  CHECK_EQUAL(detail::serialize(buf, ulli_min), true);
  unsigned long long ulli_min2 = ULLONG_MAX;
  REQUIRE(ldes(buf, ulli_min2));
  CHECK_EQUAL(ulli_min, ulli_min2);
  // unsigned char max
  buf.clear();
  const unsigned char uc_max = UCHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, uc_max), true);
  unsigned char uc_max2 = 0;
  REQUIRE(ldes(buf, uc_max2));
  CHECK_EQUAL(uc_max, uc_max2);
  // unsigned short int max
  buf.clear();
  const unsigned short usi_max = USHRT_MAX;
  CHECK_EQUAL(detail::serialize(buf, usi_max), true);
  unsigned short usi_max2 = 0;
  REQUIRE(ldes(buf, usi_max2));
  CHECK_EQUAL(usi_max, usi_max2);
  // unsigned int max
  buf.clear();
  const unsigned ui_max = UINT_MAX;
  CHECK_EQUAL(detail::serialize(buf, ui_max), true);
  unsigned ui_max2 = 0;
  REQUIRE(ldes(buf, ui_max2));
  CHECK_EQUAL(ui_max, ui_max2);
  // unsigned long max
  buf.clear();
  const unsigned long uli_max = ULONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, uli_max), true);
  unsigned long uli_max2 = 0;
  REQUIRE(ldes(buf, uli_max2));
  CHECK_EQUAL(uli_max2, uli_max);
  // unsigned long long max
  buf.clear();
  const unsigned long long ulli_max = ULLONG_MAX;
  CHECK_EQUAL(detail::serialize(buf, ulli_max), true);
  unsigned long long ulli_max2 = 0;
  REQUIRE(ldes(buf, ulli_max2));
  CHECK_EQUAL(ulli_max, ulli_max2);
  // char min
  buf.clear();
  const char c_min = CHAR_MIN;
  CHECK_EQUAL(detail::serialize(buf, c_min), true);
  char c_min2 = CHAR_MAX;
  REQUIRE(ldes(buf, c_min2));
  CHECK_EQUAL(c_min, c_min2);
  // char max
  buf.clear();
  const char c_max = CHAR_MAX;
  CHECK_EQUAL(detail::serialize(buf, c_max), true);
  char c_max2 = 0;
  REQUIRE(ldes(buf, c_max2));
  CHECK_EQUAL(c_max, c_max2);
  // bool
  buf.clear();
  const bool b = false;
  CHECK_EQUAL(detail::serialize(buf, b), true);
  bool b2 = true;
  REQUIRE(ldes(buf, b2));
  CHECK_EQUAL(b, b2);
}

TEST(bytes) {
  const std::array bytes{std::byte{'a'}, std::byte{'c'}};
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, bytes), true);
  std::array<std::byte, 2> bytes2{};
  REQUIRE(ldes(buf, bytes2));
  CHECK_EQUAL(bytes, bytes2);
}

TEST(record_type) {
  const auto r = type{record_type{
    {
      "x",
      record_type{
        {"y",
         record_type{
           {"z", integer_type{}},
           {"k", bool_type{}},
         }},
        {"m",
         record_type{
           {"y",
            record_type{
              {"a", address_type{}},
            }},
           {"f", real_type{}},
         }},
        {"b", bool_type{}},
      },
    },
    {
      "y",
      record_type{
        {"b", bool_type{}},
      },
    },
  }};

  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, r), true);
  auto r2 = type{};
  REQUIRE(ldes(buf, r2));
  CHECK_EQUAL(r, r2);
}

TEST(qualified_record_field) {
  const auto field = qualified_record_field{
    "zeek.conn",
    "conn.id",
    type{address_type{}},
  };

  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, field), true);
  auto field2 = qualified_record_field{};
  REQUIRE(ldes(buf, field2));
  CHECK_EQUAL(field, field2);
}

TEST(ids) {
  auto i = ids{};
  i.append_bits(true, 20);
  i.append_bits(false, 5);
  i.append_bits(true, 1);
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, i), true);
  auto i2 = ids{};
  REQUIRE(ldes(buf, i2));
  CHECK_EQUAL(i, i2);
}

namespace {

auto to_addr_view(std::string_view str) {
  return make_data_view(unbox(to<address>(str)));
}

} // namespace

TEST(time_synopsis) {
  using vast::time;
  const time epoch;
  factory<synopsis>::initialize();
  auto ts = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(ts, nullptr);
  ts->add(time{epoch + 4s});
  ts->add(time{epoch + 7s});
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, ts), true);
  auto ts2 = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE(ldes(buf, ts2));
  CHECK_EQUAL(*ts, *ts2);
}

TEST(bool_synopsis) {
  factory<synopsis>::initialize();
  auto bs = factory<synopsis>::make(type{bool_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(bs, nullptr);
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, bs), true);
  auto bs2 = factory<synopsis>::make(type{bool_type{}}, caf::settings{});
  REQUIRE(ldes(buf, bs2));
  CHECK_EQUAL(*bs, *bs2);
}

TEST(address_synopsis) {
  factory<synopsis>::initialize();
  auto lat = type{address_type{}, {{"synopsis", "bloomfilter(1,0.1)"}}};
  auto as = factory<synopsis>::make(lat, caf::settings{});
  REQUIRE_NOT_EQUAL(as, nullptr);
  as->add(to_addr_view("192.168.0.1"));
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, as), true);
  auto as2 = factory<synopsis>::make(lat, caf::settings{});
  REQUIRE(ldes(buf, as2));
  CHECK_EQUAL(*as2, *as);
}

TEST(string_synopsis) {
  factory<synopsis>::initialize();
  auto lst = type{string_type{}, {{"synopsis", "bloomfilter(1,0.1)"}}};
  auto ss = factory<synopsis>::make(lst, caf::settings{});
  REQUIRE_NOT_EQUAL(ss, nullptr);
  ss->add(std::string_view{"192.168.0.1"});
  caf::byte_buffer buf;
  CHECK_EQUAL(detail::serialize(buf, ss), true);
  auto ss2 = factory<synopsis>::make(lst, caf::settings{});
  REQUIRE(ldes(buf, ss2));
  CHECK_EQUAL(*ss, *ss2);
}
