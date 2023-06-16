//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/cast.hpp"

#include "vast/test/test.hpp"

namespace {

struct fixture {};

constexpr auto max_loseless_double_integer
  = (uint64_t{1} << std::numeric_limits<double>::digits) - 1;

} // namespace

FIXTURE_SCOPE(cast_value_tests, fixture)

TEST(int64 to uint64 works for positive values)
{
  constexpr auto in = std::numeric_limits<int64_t>::max();
  auto out = vast::cast_value(vast::int64_type{}, in, vast::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<uint64_t>(in));
}

TEST(int64 to uint64 doesnt work for negative values)
{
  constexpr auto in = int64_t{-1};
  auto out = vast::cast_value(vast::int64_type{}, in, vast::uint64_type{});
  REQUIRE(not out);
}

TEST(uint64 to int64 works for max int64)
{
  constexpr auto in
    = static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
  auto out = vast::cast_value(vast::uint64_type{}, in, vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<int64_t>(in));
}

TEST(int64 to uint64 doesnt work for values bigger than int64 max)
{
  constexpr auto in
    = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1u;
  auto out = vast::cast_value(vast::uint64_type{}, in, vast::int64_type{});
  REQUIRE(not out);
}

TEST(int64 to bool works for 0)
{
  constexpr auto in = int64_t{0};
  auto out = vast::cast_value(vast::int64_type{}, in, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(int64 to bool works for 1)
{
  constexpr auto in = int64_t{1};
  auto out = vast::cast_value(vast::int64_type{}, in, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(int64 to bool doesnt work for negative value)
{
  constexpr auto in = int64_t{-1};
  auto out = vast::cast_value(vast::int64_type{}, in, vast::bool_type{});
  REQUIRE(not out);
}

TEST(int64 to bool doesnt work for value bigger than 1)
{
  constexpr auto in = int64_t{2};
  auto out = vast::cast_value(vast::int64_type{}, in, vast::bool_type{});
  REQUIRE(not out);
}

TEST(bool to int64 works for false)
{
  auto out = vast::cast_value(vast::bool_type{}, false, vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{0});
}

TEST(bool to int64 works for true)
{
  auto out = vast::cast_value(vast::bool_type{}, true, vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{1});
}

TEST(bool to uint64 works for false)
{
  auto out = vast::cast_value(vast::bool_type{}, false, vast::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{0});
}

TEST(bool to uint64 works for true)
{
  auto out = vast::cast_value(vast::bool_type{}, true, vast::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{1});
}

TEST(uint64_t to bool works for 0)
{
  auto out
    = vast::cast_value(vast::uint64_type{}, uint64_t{0}, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(uint64_t to bool works for 1)
{
  auto out
    = vast::cast_value(vast::uint64_type{}, uint64_t{1}, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(uint64 to bool doesnt work for value bigger than 1)
{
  auto out
    = vast::cast_value(vast::uint64_type{}, uint64_t{2}, vast::bool_type{});
  REQUIRE(not out);
}

TEST(bool to double works for false)
{
  auto out = vast::cast_value(vast::bool_type{}, false, vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, double{0.0});
}

TEST(bool to double works for true)
{
  auto out = vast::cast_value(vast::bool_type{}, true, vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, double{1.0});
}

TEST(double to bool works for 0.0)
{
  auto out
    = vast::cast_value(vast::double_type{}, double{0.0}, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, false);
}

TEST(double to bool works for 1.0)
{
  auto out
    = vast::cast_value(vast::double_type{}, double{1.0}, vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(double to bool doesnt work for integral value bigger than 1.0)
{
  auto out
    = vast::cast_value(vast::double_type{}, double{2.0}, vast::bool_type{});
  REQUIRE(not out);
}

TEST(double to bool doesnt work for a value with fractional part)
{
  auto out
    = vast::cast_value(vast::double_type{}, double{0.1}, vast::bool_type{});
  REQUIRE(not out);
}

TEST(int64_t to double works for max loseless integer)
{
  auto out = vast::cast_value(vast::int64_type{},
                              static_cast<int64_t>(max_loseless_double_integer),
                              vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<double>(max_loseless_double_integer));
}

TEST(int64_t to double works for negative max loseless integer)
{
  auto out
    = vast::cast_value(vast::int64_type{},
                       -static_cast<int64_t>(max_loseless_double_integer),
                       vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, -static_cast<double>(max_loseless_double_integer));
}

TEST(double to int64_t works for positive value smaller than int64_t max)
{
  auto out = vast::cast_value(vast::double_type{}, 1.0, vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{1});
}

TEST(double to int64_t works for negative value bigger than int64_t min)
{
  auto out = vast::cast_value(vast::double_type{}, -1.0, vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{-1});
}

TEST(uint64_t to double works for max loseless integer)
{
  auto out = vast::cast_value(vast::uint64_type{}, max_loseless_double_integer,
                              vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, static_cast<double>(max_loseless_double_integer));
}

TEST(double to uint64_t) {
  auto out = vast::cast_value(vast::double_type{}, 15.0, vast::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{15});
}

TEST(uint64_t to enumeration doesnt work if the input has values higher than
       uint32_t max) {
  auto in = uint64_t{std::numeric_limits<uint32_t>::max()} + 1;
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 1}}};
  auto out = vast::cast_value(vast::uint64_type{}, in, type);
  REQUIRE(not out);
}

TEST(uint64_t to enumeration doesnt work if the input doesnt have a
       corresponding field) {
  auto in = uint64_t{1};
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 2}}};
  auto out = vast::cast_value(vast::uint64_type{}, in, type);
  REQUIRE(not out);
}

TEST(uint64_t to enumeration works if the input has a corresponding field) {
  auto in = uint64_t{1};
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 1}}};
  auto out = vast::cast_value(vast::uint64_type{}, in, type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{1});
}

TEST(int64_t to enumeration doesnt work if the input has values higher than
       uint32_t max) {
  auto in = int64_t{std::numeric_limits<uint32_t>::max()} + 1;
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 1}}};
  auto out = vast::cast_value(vast::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration doesnt work if the input is negative) {
  auto in = int64_t{-1};
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 1}}};
  auto out = vast::cast_value(vast::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration doesnt work if the input doesnt have a corresponding
       field) {
  auto in = int64_t{5};
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 2}}};
  auto out = vast::cast_value(vast::int64_type{}, in, type);
  REQUIRE(not out);
}

TEST(int64_t to enumeration works if the input has a corresponding field) {
  auto in = int64_t{5};
  auto type
    = vast::enumeration_type{{vast::enumeration_type::field_view{"1", 5}}};
  auto out = vast::cast_value(vast::int64_type{}, in, type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{5});
}

// TODO double to enum when double/integral is properly working

TEST(positive int64_t to string) {
  auto out
    = vast::cast_value(vast::int64_type{}, int64_t{5}, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "+5");
}

TEST(negative int64_t to string) {
  auto out
    = vast::cast_value(vast::int64_type{}, int64_t{-5}, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "-5");
}

TEST(positive uint64_t to string) {
  auto out
    = vast::cast_value(vast::uint64_type{}, uint64_t{5}, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "5");
}

TEST(positive double to string) {
  auto out = vast::cast_value(vast::double_type{}, double{2352.1362},
                              vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "2352.1362");
}

TEST(negative double to string) {
  auto out = vast::cast_value(vast::double_type{}, double{-12352.13623252},
                              vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "-12352.13623252");
}

// todo handdle such values?
// TEST(INF double to string)
// {
//   auto out = vast::cast_value(vast::double_type{},
//   std::numeric_limits<double>::infinity(), vast::string_type{});
//   REQUIRE(out);
//   CHECK_EQUAL(*out, "-12352.13623252");
// }

TEST(bool to string) {
  auto out = vast::cast_value(vast::bool_type{}, false, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "false");
  out = vast::cast_value(vast::bool_type{}, true, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "true");
}

TEST(duration to string) {
  auto out = vast::cast_value(vast::duration_type{},
                              vast::duration{std::chrono::milliseconds{27}},
                              vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "27.0ms");
}

TEST(time to string) {
  auto out = vast::cast_value(vast::time_type{},
                              vast::time{std::chrono::milliseconds{27}},
                              vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1970-01-01T00:00:00.027000");
}

TEST(string to string) {
  constexpr auto in = "amazing_string!@#%Q@&*@";
  auto out = vast::cast_value(vast::string_type{}, in, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(out, in);
}

TEST(ip to string) {
  auto in = vast::ip::v4(uint32_t{0x01'02'03'04});
  auto out = vast::cast_value(vast::ip_type{}, in, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4");
}

TEST(subnet to string) {
  auto in = vast::subnet{vast::ip::v4(uint32_t{0x01'02'03'04}), 128};
  auto out = vast::cast_value(vast::subnet_type{}, in, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "1.2.3.4/32");
}

TEST(enumeration to string) {
  auto type = vast::enumeration_type{
    {vast::enumeration_type::field_view{"enum_val_1", 1},
     vast::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out = vast::cast_value(type, vast::enumeration{3}, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "enum_val_3");
}

TEST(list to string) {
  auto out = vast::cast_value(vast::list_type{vast::int64_type{}},
                              vast::list{int64_t{1}, int64_t{-1}},
                              vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, "[+1, -1]");
}

TEST(record to string) {
  auto out = vast::cast_value(
    vast::record_type{
      {"int", vast::int64_type{}},
      {"str", vast::string_type{}},
    },
    vast::record{{"int", int64_t{100}}, {"str", "strr"}}, vast::string_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, R"(<int: +100, str: "strr">)");
}

TEST(string to time) {
  auto out = vast::cast_value(vast::string_type{}, "1970-01-01T00:00:00.027000",
                              vast::time_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, vast::time{std::chrono::milliseconds{27}});
}

TEST(string to time retruns an error for an input that doesnt resemble a time point)
{
  auto out = vast::cast_value(vast::string_type{}, "10:00", vast::time_type{});
  REQUIRE(not out);
}

TEST(string to duration) {
  auto out
    = vast::cast_value(vast::string_type{}, "30s", vast::duration_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::seconds{30});
}

TEST(string to duration retruns an error for an input doesnt have a unit)
{
  auto out = vast::cast_value(vast::string_type{}, "30", vast::duration_type{});
  REQUIRE(not out);
}

TEST(string to subnet) {
  auto out
    = vast::cast_value(vast::string_type{}, "1.2.3.4/32", vast::subnet_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, (vast::subnet{vast::ip::v4(uint32_t{0x01'02'03'04}), 128}));
}

TEST(string to ip) {
  auto out = vast::cast_value(vast::string_type{}, "1.2.3.4", vast::ip_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, vast::ip::v4(uint32_t{0x01'02'03'04}));
}

TEST(string to bool) {
  auto out = vast::cast_value(vast::string_type{}, "true", vast::bool_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, true);
}

TEST(string to uint64_t) {
  auto out = vast::cast_value(vast::string_type{}, "3245", vast::uint64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, uint64_t{3245});
}

TEST(string to uint64_t fails for string value that would overflow int64_t) {
  auto out = vast::cast_value(vast::string_type{},
                              "322154326534213214123523523523623283409567843597"
                              "23498047219803445",
                              vast::uint64_type{});
  REQUIRE(not out);
}

TEST(string to uint64_t fails for negative string_value) {
  auto out = vast::cast_value(vast::string_type{}, "-1", vast::uint64_type{});
  REQUIRE(not out);
}

TEST(string to int64_t) {
  auto out = vast::cast_value(vast::string_type{}, "3245", vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{3245});
  out = vast::cast_value(vast::string_type{}, "-3245", vast::int64_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, int64_t{-3245});
}

TEST(string to int64_t fails for string value that would overflow int64_t) {
  auto out = vast::cast_value(vast::string_type{},
                              "322154326534213214123523523523623283409567843597"
                              "23498047219803445",
                              vast::int64_type{});
  REQUIRE(not out);
  out = vast::cast_value(vast::string_type{},
                         "-322154326534213214123523523523623283409567843597"
                         "23498047219803445",
                         vast::int64_type{});
  REQUIRE(not out);
}

TEST(string to double) {
  auto out
    = vast::cast_value(vast::string_type{}, "3245.85932", vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, 3245.85932);
  out
    = vast::cast_value(vast::string_type{}, "-3245.3251", vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, -3245.3251);
}

TEST(string to scientific notation) {
  auto out = vast::cast_value(vast::string_type{}, "3E8", vast::double_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, 3'000'000'00.0);
}

TEST(string to enumeration) {
  auto type = vast::enumeration_type{
    {vast::enumeration_type::field_view{"enum_val_1", 1},
     vast::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out = vast::cast_value(vast::string_type{}, "enum_val_3", type);
  REQUIRE(out);
  CHECK_EQUAL(*out, enumeration{3});
}

TEST(string to enumeration fails when a string doesnt represent any enum state) {
  auto type = vast::enumeration_type{
    {vast::enumeration_type::field_view{"enum_val_1", 1},
     vast::enumeration_type::field_view{"enum_val_3", 3}}};
  auto out = vast::cast_value(vast::string_type{}, "enum_val_2", type);
  REQUIRE(not out);
}

TEST(string to list) {
  auto out = vast::cast_value(vast::string_type{}, "[+1, -1]",
                              vast::list_type{vast::int64_type{}});
  REQUIRE(out);
  CHECK_EQUAL(*out, (list{int64_t{1}, int64_t{-1}}));
}

TEST(string to record) {
  auto out
    = vast::cast_value(vast::string_type{}, R"(<int: +100, str: "strr">)",
                       vast::record_type{
                         {"int", vast::int64_type{}},
                         {"str", vast::string_type{}},
                       });
  REQUIRE(out);
  CHECK_EQUAL(*out, (vast::record{{"int", int64_t{100}}, {"str", "strr"}}));
}

TEST(negative int64_t to duration results in error) {
  auto out
    = vast::cast_value(vast::int64_type{}, int64_t{-10}, vast::duration_type{});
  REQUIRE(not out);
}

TEST(positive int64_t to duration with a custom unit) {
  auto out = vast::cast_value(vast::int64_type{}, int64_t{10},
                              vast::duration_type{}, "hours");
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::hours{10});
}

TEST(uint64_t to duration) {
  auto out = vast::cast_value(vast::uint64_type{}, uint64_t{120},
                              vast::duration_type{});
  REQUIRE(out);
  // the default unit is seconds if not provided in vast::cast_value
  CHECK_EQUAL(*out, std::chrono::seconds{120});
}

TEST(negative double to duration results in error) {
  auto out = vast::cast_value(vast::double_type{}, double{-120},
                              vast::duration_type{});
  CHECK(not out);
}

TEST(positive double to duration) {
  auto out
    = vast::cast_value(vast::double_type{}, double{120}, vast::duration_type{});
  REQUIRE(out);
  CHECK_EQUAL(*out, std::chrono::seconds{120});
}

FIXTURE_SCOPE_END()

TEST(cast int64_t array to a string builder) {
  auto int_builder
    = vast::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  status = int_builder->Append(2);
  status = int_builder->AppendNull();
  status = int_builder->Append(3);
  auto array
    = std::static_pointer_cast<vast::type_to_arrow_array_t<vast::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out
    = vast::cast_to_builder(vast::int64_type{}, array, vast::string_type{});
  REQUIRE(out);
  auto arr = (*out)->Finish().ValueOrDie();
  auto vals = vast::values(vast::type{vast::string_type{}}, *arr);
  std::vector<vast::data_view> views;
  for (const auto& val : vals)
    views.push_back(val);
  REQUIRE_EQUAL(views.size(), 4u);
  CHECK_EQUAL(materialize(views[0]), "+1");
  CHECK_EQUAL(materialize(views[1]), "+2");
  CHECK_EQUAL(materialize(views[2]), caf::none);
  CHECK_EQUAL(materialize(views[3]), "+3");
}

TEST(casting builder with no compatible types results in an error) {
  auto int_builder
    = vast::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  auto array
    = std::static_pointer_cast<vast::type_to_arrow_array_t<vast::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out = vast::cast_to_builder(vast::int64_type{}, array,
                                   vast::list_type{vast::string_type{}});
  CHECK(not out);
}

TEST(
  casting int64_t array to uint64_t builder works when all values can be cast) {
  auto int_builder
    = vast::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(1);
  status = int_builder->Append(2);
  status = int_builder->Append(3);
  auto array
    = std::static_pointer_cast<vast::type_to_arrow_array_t<vast::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out
    = vast::cast_to_builder(vast::int64_type{}, array, vast::uint64_type{});
  REQUIRE(out);
  auto arr = (*out)->Finish().ValueOrDie();
  auto vals = vast::values(vast::type{vast::uint64_type{}}, *arr);
  std::vector<vast::data_view> views;
  for (const auto& val : vals)
    views.push_back(val);
  REQUIRE_EQUAL(views.size(), 3u);
  CHECK_EQUAL(materialize(views[0]), uint64_t{1});
  CHECK_EQUAL(materialize(views[1]), uint64_t{2});
  CHECK_EQUAL(materialize(views[2]), uint64_t{3});
}

TEST(casting int64_t array to uint64_t builder fails due to negative value) {
  auto int_builder
    = vast::int64_type::make_arrow_builder(arrow::default_memory_pool());
  auto status = int_builder->Append(-1);
  auto array
    = std::static_pointer_cast<vast::type_to_arrow_array_t<vast::int64_type>>(
      int_builder->Finish().ValueOrDie());
  auto out
    = vast::cast_to_builder(vast::int64_type{}, array, vast::uint64_type{});
  CHECK(not out);
}
