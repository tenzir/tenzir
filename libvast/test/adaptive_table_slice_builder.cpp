//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"

#include "vast/data.hpp"
#include "vast/test/test.hpp"

using namespace vast;
using namespace std::chrono_literals;

TEST(add two rows of nested records) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("int1").add(int64_t{5});
    row.push_field("str1").add("some_str");
    auto nested = row.push_field("nested").push_record();
    nested.push_field("rec1").add(int64_t{10});
    nested.push_field("rec2").add("rec_str");
  }
  {
    auto row = sut.push_row();
    row.push_field("int1").add(int64_t{5});
    row.push_field("str1").add("some_str");
    auto nested = row.push_field("nested").push_record();
    nested.push_field("rec1").add(int64_t{10});
    nested.push_field("rec2").add("rec_str");
  }
  auto out = std::move(sut).finish();
  REQUIRE_EQUAL(out.rows(), 2u);
  REQUIRE_EQUAL(out.columns(), 4u);
  for (std::size_t i = 0u; i < out.rows(); ++i) {
    CHECK_EQUAL((materialize(out.at(i, 0u))), int64_t{5});
    CHECK_EQUAL((materialize(out.at(i, 1u))), "some_str");
    CHECK_EQUAL((materialize(out.at(i, 2u))), int64_t{10});
    CHECK_EQUAL((materialize(out.at(i, 3u))), "rec_str");
  }
  const auto schema
    = vast::type{record_type{{"int1", int64_type{}},
                             {"str1", string_type{}},
                             {"nested", record_type{
                                          {"rec1", int64_type{}},
                                          {"rec2", string_type{}},
                                        }}}};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(single record with nested lists) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{5});
    auto outer_list = row.push_field("arr").push_list();
    {
      auto level_1_list = outer_list.push_list();
      {
        auto level_2_list = level_1_list.push_list();
        level_2_list.add(int64_t{1});
        level_2_list.add(int64_t{2});
      }
      {
        auto level_2_list = level_1_list.push_list();
        level_2_list.add(int64_t{3});
        level_2_list.add(int64_t{4});
      }
    }
    {
      auto level_1_list = outer_list.push_list();
      {
        auto level_2_list = level_1_list.push_list();
        level_2_list.add(int64_t{5});
        level_2_list.add(int64_t{6});
      }
      {
        auto level_2_list = level_1_list.push_list();
        level_2_list.add(int64_t{7});
        level_2_list.add(int64_t{8});
      }
    }
  }
  auto out = std::move(sut).finish();
  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{5});
  CHECK_EQUAL(
    (materialize(out.at(0u, 1u))),
    (list{list{list{int64_t{1}, int64_t{2}}, list{int64_t{3}, int64_t{4}}},
          list{list{int64_t{5}, int64_t{6}}, list{int64_t{7}, int64_t{8}}}}));
  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
    {"arr", list_type{type{list_type{type{list_type{int64_type{}}}}}}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(single record with array inside nested record) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("bool").add(true);
    auto nested = row.push_field("nested").push_record();
    auto nested_arr = nested.push_field("arr").push_list();
    nested_arr.add(uint64_t{10});
    nested_arr.add(uint64_t{100});
    nested_arr.add(uint64_t{1000});
  }
  auto out = std::move(sut).finish();
  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), true);
  CHECK_EQUAL((materialize(out.at(0u, 1u))),
              (list{uint64_t{10}, uint64_t{100}, uint64_t{1000}}));
  const auto schema = vast::type{record_type{
    {"bool", bool_type{}},
    {"nested", record_type{{"arr", list_type{uint64_type{}}}}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(record nested in array of records in two rows) {
  adaptive_table_slice_builder sut;
  const auto row_1_time_point = std::chrono::system_clock::now();
  const auto row_2_time_point = row_1_time_point + std::chrono::seconds{5u};
  {
    auto row = sut.push_row();
    auto arr = row.push_field("arr").push_list();
    auto rec = arr.push_record();
    rec.push_field("rec double").add(2.0);
    rec.push_field("rec time").add(vast::time{row_1_time_point});
    auto nested_rec = rec.push_field("nested rec").push_record();
    nested_rec.push_field("nested duration").add(duration{20us});
  }
  {
    auto row = sut.push_row();
    auto arr = row.push_field("arr").push_list();
    auto rec = arr.push_record();
    rec.push_field("rec double").add(4.0);
    rec.push_field("rec time").add(vast::time{row_2_time_point});
    auto nested_rec = rec.push_field("nested rec").push_record();
    nested_rec.push_field("nested duration").add(duration{6ms});
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 2u);
  CHECK_EQUAL(out.columns(), 1u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))),
              (list{record{{"rec double", double{2.0}},
                           {"rec time", vast::time{row_1_time_point}},
                           {"nested rec",
                            record{{"nested duration", duration{20us}}}}}}));
  CHECK_EQUAL((materialize(out.at(1u, 0u))),
              (list{record{{{"rec double", double{4.0}},
                            {"rec time", vast::time{row_2_time_point}},
                            {"nested rec",
                             record{{{"nested duration", duration{6ms}}}}}}}}));
  const auto schema = vast::type{record_type{
    {"arr", list_type{record_type{
              {"rec double", double_type{}},
              {"rec time", time_type{}},
              {"nested rec", record_type{{"nested duration", duration_type{}}}},
            }}}}};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(two rows of array of complex records) {
  const auto row_1_1_subnet = subnet{ip::v4(1u), 1u};
  const auto row_1_2_subnet = subnet{ip::v4(5u), 5u};
  const auto row_2_1_subnet = subnet{ip::v4(0xFF), 10u};
  const auto row_2_2_subnet = subnet{ip::v4(0u), 4u};
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto arr = row.push_field("arr").push_list();
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_1_1_subnet);
      auto ip_arr_arr = rec.push_field("ip arr").push_list();
      auto ip_arr_1 = ip_arr_arr.push_list();
      ip_arr_1.add(ip::v4(2u));
      ip_arr_1.add(ip::v4(3u));
      ip_arr_arr.push_list().add(ip::v4(4u));
    }
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_1_2_subnet);
      rec.push_field("ip arr").push_list().push_list().add(ip::v4(6u));
    }
  }
  {
    auto row = sut.push_row();
    auto arr = row.push_field("arr").push_list();
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_2_1_subnet);
      auto ip_arr_arr = rec.push_field("ip arr").push_list();
      auto ip_arr_1 = ip_arr_arr.push_list();
      ip_arr_1.add(ip::v4(0xFF << 1));
      ip_arr_1.add(ip::v4(0xFF << 2));
      auto ip_arr_2 = ip_arr_arr.push_list();
      ip_arr_2.add(ip::v4(0xFF << 3));
      ip_arr_2.add(ip::v4(0xFF << 4));
    }
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_2_2_subnet);
      rec.push_field("ip arr").push_list().push_list().add(ip::v4(0xFF << 5));
    }
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 2u);
  CHECK_EQUAL(out.columns(), 1u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))),
              (list{record{{"subnet", row_1_1_subnet},
                           {"ip arr", list{list{ip::v4(2u), ip::v4(3u)},
                                           list{ip::v4(4u)}}}},
                    {record{
                      {"subnet", row_1_2_subnet},
                      {"ip arr", list{{list{ip::v4(6u)}}}},
                    }}}));

  CHECK_EQUAL(
    (materialize(out.at(1u, 0u))),
    (list{record{{"subnet", row_2_1_subnet},
                 {"ip arr", list{list{ip::v4(0xFF << 1), ip::v4(0xFF << 2)},
                                 list{ip::v4(0xFF << 3), ip::v4(0xFF << 4)}}}},
          {record{
            {"subnet", row_2_2_subnet},
            {"ip arr", list{{list{ip::v4(0xFF << 5)}}}},
          }}}));
  const auto schema = vast::type{
    record_type{{"arr", list_type{record_type{
                          {"subnet", subnet_type{}},
                          {"ip arr", list_type{type{list_type{ip_type{}}}}},
                        }}}}};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(two rows with array) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{5});
    auto arr = row.push_field("arr").push_list();
    arr.add(int64_t{1});
    arr.add(int64_t{2});
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{10});
    row.push_field("arr").push_list().add(int64_t{3});
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 2u);
  CHECK_EQUAL(out.columns(), 2u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{5});
  CHECK_EQUAL((materialize(out.at(1u, 0u))), int64_t{10});
  CHECK_EQUAL((materialize(out.at(0u, 1u))), (list{int64_t{1}, int64_t{2}}));
  CHECK_EQUAL((materialize(out.at(1u, 1u))), (list{int64_t{3}}));
  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
    {"arr", list_type{int64_type{}}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(new fields added in new rows) {
  adaptive_table_slice_builder sut;
  sut.push_row().push_field("int").add(int64_t{5});
  {
    auto row = sut.push_row();
    auto arr = row.push_field("arr").push_list();
    arr.push_list().add(int64_t{3});
    arr.push_list().add(int64_t{4});
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{1});
    row.push_field("str").add("strr");
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 3u);
  CHECK_EQUAL(out.columns(), 3u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{5});
  CHECK_EQUAL((materialize(out.at(1u, 0u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 0u))), int64_t{1});

  CHECK_EQUAL((materialize(out.at(0u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 1u))),
              (list{list{int64_t{3}}, list{int64_t{4}}}));
  CHECK_EQUAL((materialize(out.at(2u, 1u))), caf::none);

  CHECK_EQUAL((materialize(out.at(0u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 2u))), "strr");

  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
    {"arr", list_type{type{list_type{int64_type{}}}}},
    {"str", string_type{}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(single empty struct field results in empty table slice) {
  adaptive_table_slice_builder sut;
  sut.push_row().push_field("struct").push_record();
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 0u);
  CHECK_EQUAL(out.columns(), 0u);
}

TEST(empty struct is not added to the output table slice) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("struct").push_record();
    row.push_field("int").add(int64_t{2312});
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 1u);
  CHECK_EQUAL(out.columns(), 1u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{2312});
  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(single empty array field results in empty table slice) {
  adaptive_table_slice_builder sut;
  sut.push_row().push_field("arr").push_list();
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 0u);
  CHECK_EQUAL(out.columns(), 0u);
}

TEST(empty array is not added to the output table slice) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("arr").push_list();
    row.push_field("int").add(int64_t{2312});
  }
  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 1u);
  CHECK_EQUAL(out.columns(), 1u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{2312});
  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}

TEST(
  empty structs and arrays fields change into non empty ones in the next rows) {
  adaptive_table_slice_builder sut;
  sut.push_row().push_field("int").add(int64_t{5});
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{10});
    row.push_field("arr").push_list();
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{15});
    row.push_field("struct").push_record();
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{20});
    row.push_field("arr").push_list().add("arr1");
  }
  sut.push_row().push_field("int").add(int64_t{25});
  sut.push_row()
    .push_field("struct")
    .push_record()
    .push_field("struct.str")
    .add("str");
  {
    auto row = sut.push_row();
    auto root_struct = row.push_field("struct").push_record();
    root_struct.push_field("struct.str").add("str2");
    auto inner_struct = root_struct.push_field("struct.struct").push_record();
    inner_struct.push_field("struct.struct.int").add(int64_t{90});
    auto arr = inner_struct.push_field("struct.struct.array").push_list();
    arr.add(int64_t{10});
    arr.add(int64_t{20});
  }

  auto out = std::move(sut).finish();
  CHECK_EQUAL(out.rows(), 7u);
  CHECK_EQUAL(out.columns(), 5u);

  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{5});
  CHECK_EQUAL((materialize(out.at(1u, 0u))), int64_t{10});
  CHECK_EQUAL((materialize(out.at(2u, 0u))), int64_t{15});
  CHECK_EQUAL((materialize(out.at(3u, 0u))), int64_t{20});
  CHECK_EQUAL((materialize(out.at(4u, 0u))), int64_t{25});
  CHECK_EQUAL((materialize(out.at(5u, 0u))), caf::none);
  CHECK_EQUAL((materialize(out.at(6u, 0u))), caf::none);

  CHECK_EQUAL((materialize(out.at(0u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(3u, 1u))), list{"arr1"});
  CHECK_EQUAL((materialize(out.at(4u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(5u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(6u, 1u))), caf::none);

  CHECK_EQUAL((materialize(out.at(0u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(3u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(4u, 2u))), caf::none);
  CHECK_EQUAL((materialize(out.at(5u, 2u))), "str");
  CHECK_EQUAL((materialize(out.at(6u, 2u))), "str2");

  CHECK_EQUAL((materialize(out.at(0u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(3u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(4u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(5u, 3u))), caf::none);
  CHECK_EQUAL((materialize(out.at(6u, 3u))), int64_t{90});

  CHECK_EQUAL((materialize(out.at(0u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(2u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(3u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(4u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(5u, 4u))), caf::none);
  CHECK_EQUAL((materialize(out.at(6u, 4u))), (list{int64_t{10}, int64_t{20}}));

  const auto schema = vast::type{record_type{
    {"int", int64_type{}},
    {"arr", list_type{string_type{}}},
    {"struct",
     record_type{
       {"struct.str", string_type{}},
       {"struct.struct",
        record_type{
          {"struct.struct.int", int64_type{}},
          {"struct.struct.array", list_type{int64_type{}}},
        }},
     }},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());
}
