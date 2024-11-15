//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/cast.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

namespace {

struct fixture {};

constexpr auto max_loseless_double_integer
  = (uint64_t{1} << std::numeric_limits<double>::digits) - 1;

} // namespace

FIXTURE_SCOPE(cast_value_tests, fixture)

TEST(int64 to uint64 works for positive values)
{
  constexpr auto in = std::numeric_limits<int64_t>::max();
  auto out
    = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<uint64_t>(in));
}

TEST(int64 to uint64 doesnt work for negative values)
{
  constexpr auto in = int64_t{-1};
  auto out
    = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::uint64_type{});
  REQUIRE(not out);
}

TEST(uint64 to int64 works for max int64)
{
  constexpr auto in
    = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  auto out
    = tenzir::cast_value(tenzir::uint64_type{}, in, tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<int64_t>(in));
}

TEST(int64 to uint64 doesnt work for values bigger than int64 max)
{
  constexpr auto in
    = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1u;
  auto out
    = tenzir::cast_value(tenzir::uint64_type{}, in, tenzir::int64_type{});
  REQUIRE(not out);
}

TEST(int64 to bool works for 0)
{
  constexpr auto in = int64_t{0};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(int64 to bool works for 1)
{
  constexpr auto in = int64_t{1};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(int64 to bool doesnt work for negative value)
{
  constexpr auto in = int64_t{-1};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::bool_type{});
  REQUIRE(not out);
}

TEST(int64 to bool doesnt work for value bigger than 1)
{
  constexpr auto in = int64_t{2};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, tenzir::bool_type{});
  REQUIRE(not out);
}

TEST(bool to int64 works for false)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, false, tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{0});
}

TEST(bool to int64 works for true)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, true, tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{1});
}

TEST(bool to uint64 works for false)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, false, tenzir::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{0});
}

TEST(bool to uint64 works for true)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, true, tenzir::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{1});
}

TEST(uint64_t to bool works for 0)
{
  auto out = tenzir::cast_value(tenzir::uint64_type{}, uint64_t{0},
                                tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(uint64_t to bool works for 1)
{
  auto out = tenzir::cast_value(tenzir::uint64_type{}, uint64_t{1},
                                tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(uint64 to bool doesnt work for value bigger than 1)
{
  auto out = tenzir::cast_value(tenzir::uint64_type{}, uint64_t{2},
                                tenzir::bool_type{});
  REQUIRE(not out);
}

TEST(bool to double works for false)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, false, tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, double{0.0});
}

TEST(bool to double works for true)
{
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, true, tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, double{1.0});
}

TEST(double to bool works for 0.0)
{
  auto out = tenzir::cast_value(tenzir::double_type{}, double{0.0},
                                tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(double to bool works for 1.0)
{
  auto out = tenzir::cast_value(tenzir::double_type{}, double{1.0},
                                tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(double to bool doesnt work for integral value bigger than 1.0)
{
  auto out = tenzir::cast_value(tenzir::double_type{}, double{2.0},
                                tenzir::bool_type{});
  REQUIRE(not out);
}

TEST(double to bool doesnt work for a value with fractional part)
{
  auto out = tenzir::cast_value(tenzir::double_type{}, double{0.1},
                                tenzir::bool_type{});
  REQUIRE(not out);
}

TEST(int64_t to double works for max loseless integer)
{
  auto out
    = tenzir::cast_value(tenzir::int64_type{},
                         static_cast<int64_t>(max_loseless_double_integer),
                         tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<double>(max_loseless_double_integer));
}

TEST(int64_t to double works for negative max loseless integer)
{
  auto out
    = tenzir::cast_value(tenzir::int64_type{},
                         -static_cast<int64_t>(max_loseless_double_integer),
                         tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, -static_cast<double>(max_loseless_double_integer));
}

TEST(double to int64_t works for positive value smaller than int64_t max)
{
  auto out
    = tenzir::cast_value(tenzir::double_type{}, 1.0, tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{1});
}

TEST(double to int64_t works for negative value bigger than int64_t min)
{
  auto out
    = tenzir::cast_value(tenzir::double_type{}, -1.0, tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{-1});
}

TEST(uint64_t to double works for max loseless integer)
{
  auto out = tenzir::cast_value(
    tenzir::uint64_type{}, max_loseless_double_integer, tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<double>(max_loseless_double_integer));
}

TEST(double to uint64_t) {
  auto out
    = tenzir::cast_value(tenzir::double_type{}, 15.0, tenzir::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{15});
}

TEST(uint64_t to enumeration doesnt work if the input has values higher than
       uint32_t max) {
  auto in = uint64_t{std::numeric_limits<uint32_t>::max()} + 1;
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 1}}};
  auto out = tenzir::cast_value(tenzir::uint64_type{}, in, type);
  REQUIRE(not out);
}

TEST(uint64_t to enumeration doesnt work if the input doesnt have a
       corresponding field) {
  auto in = uint64_t{1};
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 2}}};
  auto out = tenzir::cast_value(tenzir::uint64_type{}, in, type);
  REQUIRE(not out);
}

TEST(uint64_t to enumeration works if the input has a corresponding field) {
  auto in = uint64_t{1};
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 1}}};
  auto out = tenzir::cast_value(tenzir::uint64_type{}, in, type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{1});
}

TEST(int64_t to enumeration doesnt work if the input has values higher than
       uint32_t max) {
  auto in = int64_t{std::numeric_limits<uint32_t>::max()} + 1;
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 1}}};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration doesnt work if the input is negative) {
  auto in = int64_t{-1};
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 1}}};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration doesnt work if the input doesnt have a corresponding
       field) {
  auto in = int64_t{5};
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 2}}};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration works if the input has a corresponding field) {
  auto in = int64_t{5};
  auto type
    = tenzir::enumeration_type{{tenzir::enumeration_type::field_view{"1", 5}}};
  auto out = tenzir::cast_value(tenzir::int64_type{}, in, type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{5});
}

// TODO double to enum when double/integral is properly working

TEST(positive int64_t to string) {
  auto out = tenzir::cast_value(tenzir::int64_type{}, int64_t{5},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "+5");
}

TEST(negative int64_t to string) {
  auto out = tenzir::cast_value(tenzir::int64_type{}, int64_t{-5},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "-5");
}

TEST(positive uint64_t to string) {
  auto out = tenzir::cast_value(tenzir::uint64_type{}, uint64_t{5},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "5");
}

TEST(positive double to string) {
  auto out = tenzir::cast_value(tenzir::double_type{}, double{2352.1362},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "2352.1362");
}

TEST(negative double to string) {
  auto out = tenzir::cast_value(tenzir::double_type{}, double{-12352.13623252},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "-12352.13623252");
}

// todo handdle such values?
// TEST(INF double to string)
// {
//   auto out = tenzir::cast_value(tenzir::double_type{},
//   std::numeric_limits<double>::infinity(), tenzir::string_type{});
//   REQUIRE(out);
//   CHECK_EQUAL(*out, "-12352.13623252");
// }

TEST(bool to string) {
  auto out
    = tenzir::cast_value(tenzir::bool_type{}, false, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "false");
  out = tenzir::cast_value(tenzir::bool_type{}, true, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "true");
}

TEST(duration to string) {
  auto out = tenzir::cast_value(tenzir::duration_type{},
                                tenzir::duration{std::chrono::milliseconds{27}},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "27.0ms");
}

TEST(time to string) {
  auto out = tenzir::cast_value(tenzir::time_type{},
                                tenzir::time{std::chrono::milliseconds{27}},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1970-01-01T00:00:00.027000");
}

TEST(string to string) {
  constexpr auto in = std::string_view{"amazing_string!@#%Q@&*@"};
  auto out
    = tenzir::cast_value(tenzir::string_type{}, in, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(out, in);
}

TEST(ip to string) {
  auto in = tenzir::ip::v4(uint32_t{0x01'02'03'04});
  auto out = tenzir::cast_value(tenzir::ip_type{}, in, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4");
}

TEST(subnet to string) {
  auto in = tenzir::subnet{tenzir::ip::v4(uint32_t{0x01'02'03'04}), 128};
  auto out
    = tenzir::cast_value(tenzir::subnet_type{}, in, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4/32");
}

TEST(enumeration to string) {
  auto type = tenzir::enumeration_type{
    {tenzir::enumeration_type::field_view{"enum_val_1", 1},
     tenzir::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out
    = tenzir::cast_value(type, tenzir::enumeration{3}, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "enum_val_3");
}

TEST(list to string) {
  auto out = tenzir::cast_value(tenzir::list_type{tenzir::int64_type{}},
                                tenzir::list{int64_t{1}, int64_t{-1}},
                                tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "[+1, -1]");
}

TEST(record to string) {
  auto out = tenzir::cast_value(
    tenzir::record_type{
      {"int", tenzir::int64_type{}},
      {"str", tenzir::string_type{}},
    },
    tenzir::record{{"int", int64_t{100}}, {"str", "strr"}},
    tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, R"(<int: +100, str: "strr">)");
}

TEST(string to time) {
  auto out = tenzir::cast_value(
    tenzir::string_type{}, "1970-01-01T00:00:00.027000", tenzir::time_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, tenzir::time{std::chrono::milliseconds{27}});
}

TEST(string to time retruns an error for an input that doesnt resemble a time point)
{
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "10:00", tenzir::time_type{});
  REQUIRE(not out);
}

TEST(string to duration) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "30s", tenzir::duration_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::seconds{30});
}

TEST(string to duration retruns an error for an input doesnt have a unit)
{
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "30", tenzir::duration_type{});
  REQUIRE(not out);
}

TEST(string to subnet) {
  auto out = tenzir::cast_value(tenzir::string_type{}, "1.2.3.4/32",
                                tenzir::subnet_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out,
              (tenzir::subnet{tenzir::ip::v4(uint32_t{0x01'02'03'04}), 128}));
}

TEST(string to ip) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "1.2.3.4", tenzir::ip_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, tenzir::ip::v4(uint32_t{0x01'02'03'04}));
}

TEST(string to bool) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "true", tenzir::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(string to uint64_t) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "3245", tenzir::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{3245});
}

TEST(string to uint64_t fails for string value that would overflow int64_t) {
  auto out
    = tenzir::cast_value(tenzir::string_type{},
                         "322154326534213214123523523523623283409567843597"
                         "23498047219803445",
                         tenzir::uint64_type{});
  REQUIRE(not out);
}

TEST(string to uint64_t fails for negative string_value) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "-1", tenzir::uint64_type{});
  REQUIRE(not out);
}

TEST(string to int64_t) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "3245", tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{3245});
  out
    = tenzir::cast_value(tenzir::string_type{}, "-3245", tenzir::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{-3245});
}

TEST(string to int64_t fails for string value that would overflow int64_t) {
  auto out
    = tenzir::cast_value(tenzir::string_type{},
                         "322154326534213214123523523523623283409567843597"
                         "23498047219803445",
                         tenzir::int64_type{});
  REQUIRE(not out);
  out = tenzir::cast_value(tenzir::string_type{},
                           "-322154326534213214123523523523623283409567843597"
                           "23498047219803445",
                           tenzir::int64_type{});
  REQUIRE(not out);
}

TEST(string to double) {
  auto out = tenzir::cast_value(tenzir::string_type{}, "3245.85932",
                                tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, 3245.85932);
  out = tenzir::cast_value(tenzir::string_type{}, "-3245.3251",
                           tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, -3245.3251);
}

TEST(string to scientific notation) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, "3E8", tenzir::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, 3'000'000'00.0);
}

TEST(string to enumeration) {
  auto type = tenzir::enumeration_type{
    {tenzir::enumeration_type::field_view{"enum_val_1", 1},
     tenzir::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out = tenzir::cast_value(tenzir::string_type{}, "enum_val_3", type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{3});
}

TEST(string to enumeration fails when a string doesnt represent any enum state) {
  auto type = tenzir::enumeration_type{
    {tenzir::enumeration_type::field_view{"enum_val_1", 1},
     tenzir::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out = tenzir::cast_value(tenzir::string_type{}, "enum_val_2", type);
  REQUIRE(not out);
}

TEST(string to list) {
  auto out = tenzir::cast_value(tenzir::string_type{}, "[+1, -1]",
                                tenzir::list_type{tenzir::int64_type{}});
  REQUIRE(out);
  CHECK_EQUAL(*out, (list{int64_t{1}, int64_t{-1}}));
}

TEST(string to record) {
  auto out
    = tenzir::cast_value(tenzir::string_type{}, R"(<int: +100, str: "strr">)",
                         tenzir::record_type{
                           {"int", tenzir::int64_type{}},
                           {"str", tenzir::string_type{}},
                         });
  REQUIRE(out);
  CHECK_EQUAL(*out, (tenzir::record{{"int", int64_t{100}}, {"str", "strr"}}));
}

TEST(negative int64_t to duration results in error) {
  auto out = tenzir::cast_value(tenzir::int64_type{}, int64_t{-10},
                                tenzir::duration_type{});
  REQUIRE(not out);
}

TEST(positive int64_t to duration with a custom unit) {
  auto out = tenzir::cast_value(tenzir::int64_type{}, int64_t{10},
                                tenzir::duration_type{}, "hours");
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::hours{10});
}

TEST(uint64_t to duration) {
  auto out = tenzir::cast_value(tenzir::uint64_type{}, uint64_t{120},
                                tenzir::duration_type{});
  REQUIRE(out);
  // the default unit is seconds if not provided in tenzir::cast_value
  CHECK_EQUAL(*out, std::chrono::seconds{120});
}

TEST(negative double to duration results in error) {
  auto out = tenzir::cast_value(tenzir::double_type{}, double{-120},
                                tenzir::duration_type{});
  CHECK(not out);
}

TEST(positive double to duration) {
  auto out = tenzir::cast_value(tenzir::double_type{}, double{120},
                                tenzir::duration_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::seconds{120});
}

TEST(cast value type erased) {
  auto type = tenzir::type{tenzir::int64_type{}};
  auto out
    = tenzir::cast_value(type, tenzir::data{int64_t{2}}, tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "+2");
}

TEST(cast value type erased 2) {
  auto type = tenzir::type{tenzir::ip_type{}};
  auto out
    = tenzir::cast_value(type,
                         tenzir::data{tenzir::ip::v4(uint32_t{0x01'02'03'04})},
                         tenzir::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4");
}

TEST(cast value type erased 3) {
  auto type = tenzir::type{tenzir::ip_type{}};
  auto out
    = tenzir::cast_value(type,
                         tenzir::data{tenzir::ip::v4(uint32_t{0x01'02'03'04})},
                         tenzir::type{tenzir::string_type{}});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4");
}

TEST(cast lists) {
  auto in_type = tenzir::list_type{tenzir::ip_type{}};
  auto out_type = tenzir::list_type{tenzir::string_type{}};
  auto in_list = tenzir::list{tenzir::ip::v4(uint32_t{0x01'02'03'04}),
                              tenzir::ip::v4(uint32_t{0x01'02'03'05})};
  auto out = tenzir::cast_value(in_type, in_list, out_type);
  REQUIRE(out);
  CHECK_EQUAL(*out, (tenzir::list{"1.2.3.4", "1.2.3.5"}));
}

TEST(cast record success) {
  auto in_type = tenzir::record_type{{"a", tenzir::ip_type{}}};
  auto out_type = tenzir::record_type{{"a", tenzir::string_type{}}};
  auto in_val = tenzir::record{{"a", tenzir::ip::v4(uint32_t{0x01'02'03'04})}};
  auto out = tenzir::cast_value(in_type, in_val, out_type);
  REQUIRE(out);
  CHECK_EQUAL(*out, (tenzir::record{{"a", "1.2.3.4"}}));
}

TEST(cast record inserts nulls for fields that dont exist in the input) {
  auto in_type = tenzir::record_type{{"a", tenzir::int64_type{}}};
  auto out_type = tenzir::record_type{{"a", tenzir::string_type{}},
                                      {"b", tenzir::int64_type{}}};
  auto in_val = tenzir::record{{"a", int64_t{-10}}};
  auto out = tenzir::cast_value(in_type, in_val, out_type);
  REQUIRE(out);
  CHECK_EQUAL(*out, (tenzir::record{{"a", "-10"}, {"b", caf::none}}));
}

TEST(cast lists of records) {
  auto in_type
    = tenzir::list_type{tenzir::record_type{{"a", tenzir::ip_type{}}}};
  auto out_type
    = tenzir::list_type{tenzir::record_type{{"a", tenzir::string_type{}}}};
  auto in_list = tenzir::list{
    tenzir::record{{"a", tenzir::ip::v4(uint32_t{0x01'02'03'04})}},
    tenzir::record{{"a", tenzir::ip::v4(uint32_t{0x01'02'03'05})}}};
  auto out = tenzir::cast_value(in_type, in_list, out_type);
  REQUIRE(out);
  CHECK_EQUAL(*out, (tenzir::list{tenzir::record{{"a", "1.2.3.4"}},
                                  tenzir::record{{"a", "1.2.3.5"}}}));
}

FIXTURE_SCOPE_END()

TEST(cast int64_t array to a string builder) {
  auto int_builder
    = tenzir::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  status = int_builder->Append(2);
  status = int_builder->AppendNull();
  status = int_builder->Append(3);
  auto array
    = std::static_pointer_cast<tenzir::type_to_arrow_array_t<tenzir::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out = tenzir::cast_to_builder(tenzir::int64_type{}, *array,
                                     tenzir::string_type{});
  REQUIRE(out);
  auto arr = (*out)->Finish().ValueOrDie();
  auto vals = tenzir::values(tenzir::type{tenzir::string_type{}}, *arr);
  std::vector<tenzir::data_view> views;
  for (const auto& val : vals) {
    views.push_back(val);
  }
  REQUIRE_EQUAL(views.size(), 4u);
  CHECK_EQUAL(materialize(views[0]), "+1");
  CHECK_EQUAL(materialize(views[1]), "+2");
  CHECK_EQUAL(materialize(views[2]), caf::none);
  CHECK_EQUAL(materialize(views[3]), "+3");
}

TEST(casting builder with no compatible types results in an error) {
  auto int_builder
    = tenzir::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  auto array
    = std::static_pointer_cast<tenzir::type_to_arrow_array_t<tenzir::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out = tenzir::cast_to_builder(tenzir::int64_type{}, *array,
                                     tenzir::list_type{tenzir::string_type{}});
  CHECK(not out);
}

TEST(
  casting int64_t array to uint64_t builder works when all values can be cast) {
  auto int_builder
    = tenzir::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  status = int_builder->Append(2);
  status = int_builder->Append(3);
  auto array
    = std::static_pointer_cast<tenzir::type_to_arrow_array_t<tenzir::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out = tenzir::cast_to_builder(tenzir::int64_type{}, *array,
                                     tenzir::uint64_type{});
  REQUIRE(out);
  auto arr = (*out)->Finish().ValueOrDie();
  auto vals = tenzir::values(tenzir::type{tenzir::uint64_type{}}, *arr);
  std::vector<tenzir::data_view> views;
  for (const auto& val : vals) {
    views.push_back(val);
  }
  REQUIRE_EQUAL(views.size(), 3u);
  CHECK_EQUAL(materialize(views[0]), uint64_t{1});
  CHECK_EQUAL(materialize(views[1]), uint64_t{2});
  CHECK_EQUAL(materialize(views[2]), uint64_t{3});
}

TEST(casting int64_t array to uint64_t builder fails due to negative value) {
  auto int_builder
    = tenzir::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(-1);
  auto array
    = std::static_pointer_cast<tenzir::type_to_arrow_array_t<tenzir::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out = tenzir::cast_to_builder(tenzir::int64_type{}, *array,
                                     tenzir::uint64_type{});
  CHECK(not out);
}

TEST(string to blob without padding) {
  auto out = tenzir::cast_value(tenzir::string_type{}, "dGVuemly",
                                tenzir::blob_type{});
  REQUIRE_NOERROR(out);
  CHECK_EQUAL(std::span{out.value()}, as_bytes("tenzir", 6));
}

TEST(string to blob with padding) {
  auto out = tenzir::cast_value(tenzir::string_type{},
                                "dmFzdA==", tenzir::blob_type{});
  REQUIRE_NOERROR(out);
  CHECK_EQUAL(std::span{out.value()}, as_bytes("vast", 4));
}

TEST(string to blob error) {
  auto out = tenzir::cast_value(tenzir::string_type{}, "dmFzdA==!",
                                tenzir::blob_type{});
  REQUIRE_ERROR(out);
}
