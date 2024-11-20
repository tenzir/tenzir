//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/legacy_deserialize.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/factory.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/synopsis.hpp"
#include "tenzir/synopsis_factory.hpp"
#include "tenzir/test/fixtures/actor_system.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/time_synopsis.hpp"
#include "tenzir/type.hpp"

#include <climits>
#include <span>
#include <string>
#include <string_view>

using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace tenzir;

template <class... Ts>
  requires(!std::is_rvalue_reference_v<Ts> && ...) bool
ldes(caf::byte_buffer& buf, Ts&... xs) {
  return detail::legacy_deserialize(buf, xs...);
}

TEST(string) {
  // serialize
  const auto str = "test string"s;
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, str));
  // deserialize
  auto str2 = ""s;
  REQUIRE(ldes(buf, str2));
  CHECK_EQUAL(str, str2);
}

TEST(integrals) {
  // signed char min
  caf::byte_buffer buf;
  const signed char sc_min = SCHAR_MIN;
  CHECK(detail::serialize(buf, sc_min));
  signed char sc_min2 = SCHAR_MAX;
  REQUIRE(ldes(buf, sc_min2));
  CHECK_EQUAL(sc_min, sc_min2);
  // signed short int min
  buf.clear();
  const short si_min = SHRT_MIN;
  CHECK(detail::serialize(buf, si_min));
  short si_min2 = SHRT_MAX;
  REQUIRE(ldes(buf, si_min2));
  CHECK_EQUAL(si_min, si_min2);
  // signed int min
  buf.clear();
  const int i_min = INT_MIN;
  CHECK(detail::serialize(buf, i_min));
  int i_min2 = INT_MAX;
  REQUIRE(ldes(buf, i_min2));
  CHECK_EQUAL(i_min, i_min2);
  // signed long int min
  buf.clear();
  const long li_min = LONG_MIN;
  CHECK(detail::serialize(buf, li_min));
  long li_min2 = LONG_MAX;
  REQUIRE(ldes(buf, li_min2));
  CHECK_EQUAL(li_min, li_min2);
  // signed long long int min
  buf.clear();
  const long long lli_min = LLONG_MIN;
  CHECK(detail::serialize(buf, lli_min));
  long long lli_min2 = LLONG_MAX;
  REQUIRE(ldes(buf, lli_min2));
  CHECK_EQUAL(lli_min, lli_min2);
  // signed char max
  buf.clear();
  const signed char sc_max = SCHAR_MAX;
  CHECK(detail::serialize(buf, sc_max));
  signed char sc_max2 = SCHAR_MIN;
  REQUIRE(ldes(buf, sc_max2));
  CHECK_EQUAL(sc_max, sc_max2);
  // signed short int max
  buf.clear();
  const short si_max = SHRT_MAX;
  CHECK(detail::serialize(buf, si_max));
  short si_max2 = SHRT_MIN;
  REQUIRE(ldes(buf, si_max2));
  CHECK_EQUAL(si_max, si_max2);
  // signed int max
  buf.clear();
  const int i_max = INT_MAX;
  CHECK(detail::serialize(buf, i_max));
  int i_max2 = INT_MIN;
  REQUIRE(ldes(buf, i_max2));
  CHECK_EQUAL(i_max, i_max2);
  // signed long int max
  buf.clear();
  const long li_max = LONG_MAX;
  CHECK(detail::serialize(buf, li_max));
  long li_max2 = LONG_MIN;
  REQUIRE(ldes(buf, li_max2));
  CHECK_EQUAL(li_max, li_max2);
  buf.clear();
  // signed long long int max
  const long long lli_max = LLONG_MAX;
  CHECK(detail::serialize(buf, lli_max));
  long long lli_max2 = LLONG_MIN;
  REQUIRE(ldes(buf, lli_max2));
  CHECK_EQUAL(lli_max, lli_max2);
  buf.clear();
  // unsigned char min
  buf.clear();
  const unsigned char uc_min = 0;
  CHECK(detail::serialize(buf, uc_min));
  unsigned char uc_min2 = UCHAR_MAX;
  REQUIRE(ldes(buf, uc_min2));
  CHECK_EQUAL(uc_min, uc_min2);
  // unsigned short int min
  buf.clear();
  const unsigned short usi_min = 0;
  CHECK(detail::serialize(buf, usi_min));
  unsigned short usi_min2 = USHRT_MAX;
  REQUIRE(ldes(buf, usi_min2));
  CHECK_EQUAL(usi_min, usi_min2);
  // unsigned int min
  buf.clear();
  const unsigned ui_min = 0;
  CHECK(detail::serialize(buf, ui_min));
  unsigned ui_min2 = UINT_MAX;
  REQUIRE(ldes(buf, ui_min2));
  CHECK_EQUAL(ui_min, ui_min2);
  // unsigned long int min
  buf.clear();
  const unsigned long uli_min = 0;
  CHECK(detail::serialize(buf, uli_min));
  unsigned long uli_min2 = ULONG_MAX;
  REQUIRE(ldes(buf, uli_min2));
  CHECK_EQUAL(uli_min, uli_min2);
  // unsigned long long int min
  buf.clear();
  const unsigned long long ulli_min = 0;
  CHECK(detail::serialize(buf, ulli_min));
  unsigned long long ulli_min2 = ULLONG_MAX;
  REQUIRE(ldes(buf, ulli_min2));
  CHECK_EQUAL(ulli_min, ulli_min2);
  // unsigned char max
  buf.clear();
  const unsigned char uc_max = UCHAR_MAX;
  CHECK(detail::serialize(buf, uc_max));
  unsigned char uc_max2 = 0;
  REQUIRE(ldes(buf, uc_max2));
  CHECK_EQUAL(uc_max, uc_max2);
  // unsigned short int max
  buf.clear();
  const unsigned short usi_max = USHRT_MAX;
  CHECK(detail::serialize(buf, usi_max));
  unsigned short usi_max2 = 0;
  REQUIRE(ldes(buf, usi_max2));
  CHECK_EQUAL(usi_max, usi_max2);
  // unsigned int max
  buf.clear();
  const unsigned ui_max = UINT_MAX;
  CHECK(detail::serialize(buf, ui_max));
  unsigned ui_max2 = 0;
  REQUIRE(ldes(buf, ui_max2));
  CHECK_EQUAL(ui_max, ui_max2);
  // unsigned long max
  buf.clear();
  const unsigned long uli_max = ULONG_MAX;
  CHECK(detail::serialize(buf, uli_max));
  unsigned long uli_max2 = 0;
  REQUIRE(ldes(buf, uli_max2));
  CHECK_EQUAL(uli_max2, uli_max);
  // unsigned long long max
  buf.clear();
  const unsigned long long ulli_max = ULLONG_MAX;
  CHECK(detail::serialize(buf, ulli_max));
  unsigned long long ulli_max2 = 0;
  REQUIRE(ldes(buf, ulli_max2));
  CHECK_EQUAL(ulli_max, ulli_max2);
  // char min
  buf.clear();
  const char c_min = CHAR_MIN;
  CHECK(detail::serialize(buf, c_min));
  char c_min2 = CHAR_MAX;
  REQUIRE(ldes(buf, c_min2));
  CHECK_EQUAL(c_min, c_min2);
  // char max
  buf.clear();
  const char c_max = CHAR_MAX;
  CHECK(detail::serialize(buf, c_max));
  char c_max2 = 0;
  REQUIRE(ldes(buf, c_max2));
  CHECK_EQUAL(c_max, c_max2);
  // bool
  buf.clear();
  const bool b = false;
  CHECK(detail::serialize(buf, b));
  bool b2 = true;
  REQUIRE(ldes(buf, b2));
  CHECK_EQUAL(b, b2);
}

TEST(bytes) {
  const std::array bytes{std::byte{'a'}, std::byte{'c'}};
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, bytes));
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
           {"z", int64_type{}},
           {"k", bool_type{}},
         }},
        {"m",
         record_type{
           {"y",
            record_type{
              {"a", ip_type{}},
            }},
           {"f", double_type{}},
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
  CHECK(detail::serialize(buf, r));
  auto r2 = type{};
  REQUIRE(ldes(buf, r2));
  CHECK_EQUAL(r, r2);
}

TEST(qualified_record_field) {
  const auto field = qualified_record_field{
    "zeek.conn",
    "conn.id",
    type{ip_type{}},
  };

  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, field));
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
  CHECK(detail::serialize(buf, i));
  auto i2 = ids{};
  REQUIRE(ldes(buf, i2));
  CHECK_EQUAL(i, i2);
}

namespace {

auto to_ip_view(std::string_view str) {
  return make_data_view(unbox(to<ip>(str)));
}

} // namespace

TEST(time_synopsis) {
  using tenzir::time;
  const time epoch;
  factory<synopsis>::initialize();
  auto ts = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(ts, nullptr);
  ts->add(time{epoch + 4s});
  ts->add(time{epoch + 7s});
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, ts));
  auto ts2 = factory<synopsis>::make(type{time_type{}}, caf::settings{});
  REQUIRE(ldes(buf, ts2));
  CHECK_EQUAL(*ts, *ts2);
}

TEST(bool_synopsis) {
  factory<synopsis>::initialize();
  auto bs = factory<synopsis>::make(type{bool_type{}}, caf::settings{});
  REQUIRE_NOT_EQUAL(bs, nullptr);
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, bs));
  auto bs2 = factory<synopsis>::make(type{bool_type{}}, caf::settings{});
  REQUIRE(ldes(buf, bs2));
  CHECK_EQUAL(*bs, *bs2);
}

TEST(ip_synopsis) {
  factory<synopsis>::initialize();
  auto lat = type{ip_type{}, {{"synopsis", "bloomfilter(1,0.1)"}}};
  auto as = factory<synopsis>::make(lat, caf::settings{});
  REQUIRE_NOT_EQUAL(as, nullptr);
  as->add(to_ip_view("192.168.0.1"));
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, as));
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
  CHECK(detail::serialize(buf, ss));
  auto ss2 = factory<synopsis>::make(lst, caf::settings{});
  REQUIRE(ldes(buf, ss2));
  CHECK_EQUAL(*ss, *ss2);
}

namespace {
struct custom {
  friend bool inspect(auto& inspector, custom& in) {
    return inspector.apply(in.x) && inspector.apply(in.y);
  }

  std::string x;
  std::size_t y{0u};
};

} // namespace

TEST(caf_optional) {
  caf::byte_buffer serialization_output;
  caf::binary_serializer s{nullptr, serialization_output};
  auto in = std::optional<custom>{custom{"test str", 221}};
  REQUIRE(s.apply(in));
  auto out = std::optional<custom>{};
  REQUIRE(ldes(serialization_output, out));
  REQUIRE_EQUAL(in->x, out->x);
  REQUIRE_EQUAL(in->y, out->y);
  // The empty optional should be also deserialized as empty one.
  in.reset();
  serialization_output.clear();
  caf::binary_serializer s2{nullptr, serialization_output};
  REQUIRE(s2.apply(in));
  // the out contains a set optional now so it should be reassigned to empty one
  // after deserialization
  CHECK(ldes(serialization_output, out));
  CHECK(!out);
}

TEST(caf_config_value integer) {
  caf::config_value::integer in{362};
  // in legacy caf::config_value the integer was at index 0 in the underlying
  // variant
  auto legacy_integer_index = std::uint8_t{0u};
  caf::byte_buffer serialization_output;
  caf::binary_serializer s{nullptr, serialization_output};
  REQUIRE(s.apply(legacy_integer_index) && s.apply(in));
  caf::config_value out;
  REQUIRE(ldes(serialization_output, out));
  auto out_as_integer = get_as<caf::config_value::integer>(out);
  REQUIRE_NOERROR(out_as_integer);
  CHECK_EQUAL(in, *out_as_integer);
}

TEST(caf_config_value boolean) {
  caf::config_value::boolean in{true};
  // in legacy caf::config_value the boolean was at index 1 in the underlying
  // variant
  auto legacy_boolean_index = std::uint8_t{1u};
  caf::byte_buffer serialization_output;
  caf::binary_serializer s{nullptr, serialization_output};
  REQUIRE(s.apply(legacy_boolean_index) && s.apply(in));
  caf::config_value out;
  REQUIRE(ldes(serialization_output, out));
  auto out_as_boolean = get_as<caf::config_value::boolean>(out);
  REQUIRE_NOERROR(out_as_boolean);
  CHECK_EQUAL(in, *out_as_boolean);
}

TEST(caf_config_value real) {
  caf::config_value::real in{6459.0};
  // in legacy caf::config_value the real was at index 2 in the underlying
  // variant
  auto legacy_real_index = std::uint8_t{2u};
  caf::byte_buffer serialization_output;
  caf::binary_serializer s{nullptr, serialization_output};
  REQUIRE(s.apply(legacy_real_index) && s.apply(in));
  caf::config_value out;
  REQUIRE(ldes(serialization_output, out));
  auto out_as_real = get_as<caf::config_value::real>(out);
  REQUIRE_NOERROR(out_as_real);
  CHECK_EQUAL(in, *out_as_real);
}

TEST(caf_config_value string) {
  // in legacy caf::config_value the string was at the same index as in the
  // current config_value
  caf::config_value in{caf::config_value::string{"example_str"}};
  caf::byte_buffer serialization_output;
  caf::binary_serializer s{nullptr, serialization_output};
  REQUIRE(s.apply(in));
  caf::config_value out;
  REQUIRE(ldes(serialization_output, out));
  CHECK_EQUAL(in, out);
}
