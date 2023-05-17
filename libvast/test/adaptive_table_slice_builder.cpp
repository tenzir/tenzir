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

#include <arrow/record_batch.h>

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
  auto out = sut.finish();
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

TEST(single row with nested lists) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{5});
    auto arr_field = row.push_field("arr");
    auto outer_list = arr_field.push_list();
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
  auto out = sut.finish();
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
    auto nested_rec_field = row.push_field("nested");
    auto nested = nested_rec_field.push_record();
    auto nested_arr_field = nested.push_field("arr");
    auto nested_arr = nested_arr_field.push_list();
    nested_arr.add(uint64_t{10});
    nested_arr.add(uint64_t{100});
    nested_arr.add(uint64_t{1000});
  }
  auto out = sut.finish();
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
    auto arr_field = row.push_field("arr");
    auto arr = arr_field.push_list();
    auto rec = arr.push_record();
    rec.push_field("rec double").add(2.0);
    rec.push_field("rec time").add(vast::time{row_1_time_point});
    auto nested_rec_fied = rec.push_field("nested rec");
    auto nested_rec = nested_rec_fied.push_record();
    nested_rec.push_field("nested duration").add(duration{20us});
  }
  {
    auto row = sut.push_row();
    auto arr_field = row.push_field("arr");
    auto arr = arr_field.push_list();
    auto rec = arr.push_record();
    rec.push_field("rec double").add(4.0);
    rec.push_field("rec time").add(vast::time{row_2_time_point});
    auto nested_rec_fied = rec.push_field("nested rec");
    auto nested_rec = nested_rec_fied.push_record();
    nested_rec.push_field("nested duration").add(duration{6ms});
  }
  auto out = sut.finish();
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
    auto arr_field = row.push_field("arr");
    auto arr = arr_field.push_list();
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_1_1_subnet);
      auto ip_arr_arr_field = rec.push_field("ip arr");
      auto ip_arr_arr = ip_arr_arr_field.push_list();
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
    auto arr_field = row.push_field("arr");
    auto arr = arr_field.push_list();
    {
      auto rec = arr.push_record();
      rec.push_field("subnet").add(row_2_1_subnet);
      auto ip_arr_arr_field = rec.push_field("ip arr");
      auto ip_arr_arr = ip_arr_arr_field.push_list();
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
  auto out = sut.finish();
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
    auto arrow_field = row.push_field("arr");
    auto arr = arrow_field.push_list();
    arr.add(int64_t{1});
    arr.add(int64_t{2});
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{10});
    row.push_field("arr").push_list().add(int64_t{3});
  }
  auto out = sut.finish();
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
    auto arr_field = row.push_field("arr");
    auto arr = arr_field.push_list();
    arr.push_list().add(int64_t{3});
    arr.push_list().add(int64_t{4});
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{1});
    row.push_field("str").add("strr");
  }
  auto out = sut.finish();
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
  auto out = sut.finish();
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
  auto out = sut.finish();
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
  auto out = sut.finish();
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
  auto out = sut.finish();
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
    auto inner_struct_field = root_struct.push_field("struct.struct");
    auto inner_struct = inner_struct_field.push_record();
    inner_struct.push_field("struct.struct.int").add(int64_t{90});
    auto arr_field = inner_struct.push_field("struct.struct.array");
    auto arr = arr_field.push_list();
    arr.add(int64_t{10});
    arr.add(int64_t{20});
  }

  auto out = sut.finish();
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

TEST(append nulls to the first field of a record field when a different field
       was added on the second row and the first field didnt have value added) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto record_field = row.push_field("record");
    record_field.push_record().push_field("field1").add(int64_t{1});
  }
  {
    auto row = sut.push_row();
    auto record_field = row.push_field("record");
    record_field.push_record().push_field("field2").add("field2 val");
  }

  auto out = sut.finish();
  REQUIRE_EQUAL(out.rows(), 2u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL((materialize(out.at(0u, 0u))), int64_t{1});
  CHECK_EQUAL((materialize(out.at(0u, 1u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 0u))), caf::none);
  CHECK_EQUAL((materialize(out.at(1u, 1u))), "field2 val");
}

TEST(field not present after removing the row which introduced the field) {
  adaptive_table_slice_builder sut;
  sut.push_row().push_field("int").add(int64_t{5});
  auto row = sut.push_row();
  row.push_field("int").add(int64_t{10});
  row.push_field("str").add("str");
  row.cancel();
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 1u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), int64_t{5});
}

TEST(remove basic row) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto rec = row.push_field("record").push_record();
    rec.push_field("rec int").add(int64_t{1});
    rec.push_field("rec str").add("str");
  }
  auto row = sut.push_row();
  {
    auto rec = row.push_field("record").push_record();
    rec.push_field("rec int").add(int64_t{2});
    rec.push_field("rec str").add("str2");
  }
  row.cancel();
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 1u);
  REQUIRE_EQUAL(output.columns(), 2u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), int64_t{1});
  CHECK_EQUAL(materialize(output.at(0u, 1u)), "str");
}

TEST(remove row list) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    list.add(int64_t{1});
    list.add(int64_t{2});
  }

  auto row = sut.push_row();
  {
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    list.add(int64_t{3});
    list.add(int64_t{4});
  }
  row.cancel();
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 1u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), (list{int64_t{1}, int64_t{2}}));
}

TEST(remove row list of records) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto record = list.push_record();
    record.push_field("list_rec_int").add(int64_t{1});
  }

  auto row = sut.push_row();
  {
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto record = list.push_record();
    record.push_field("list_rec_int").add(int64_t{2});
  }
  row.cancel();

  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto record = list.push_record();
    record.push_field("list_rec_int").add(int64_t{3});
  }
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 2u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), (list{record{
                                                {"list_rec_int", int64_t{1}},
                                              }}));
  CHECK_EQUAL(materialize(output.at(1u, 0u)), (list{record{
                                                {"list_rec_int", int64_t{3}},
                                              }}));
}

TEST(remove row list of lists) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto outer_list_field = row.push_field("list");
    auto outer_list = outer_list_field.push_list();
    auto inner_list = outer_list.push_list();
    inner_list.add(int64_t{1u});
  }

  auto row = sut.push_row();
  auto outer_list_field = row.push_field("list");
  auto outer_list = outer_list_field.push_list();
  auto inner_list = outer_list.push_list();
  inner_list.add(int64_t{2u});
  inner_list.add(int64_t{3u});
  row.cancel();

  {
    auto row = sut.push_row();
    auto outer_list_field = row.push_field("list");
    auto outer_list = outer_list_field.push_list();
    auto inner_list = outer_list.push_list();
    inner_list.add(int64_t{4u});
    inner_list.add(int64_t{5u});
  }

  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 2u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), list{{list{int64_t{1}}}});
  CHECK_EQUAL(materialize(output.at(1u, 0u)),
              (list{{list{int64_t{4}, int64_t{5}}}}));
}

TEST(remove row list of records with list fields) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto rec = list.push_record();
    rec.push_field("int").add(int64_t{1});
    auto inner_list_field = rec.push_field("inner list");
    auto inner_list = inner_list_field.push_list();
    auto inner_record = inner_list.push_record();
    inner_record.push_field("str").add("str1");
  }

  auto row = sut.push_row();
  {
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto rec = list.push_record();
    rec.push_field("int").add(int64_t{2});
    auto inner_list_field = rec.push_field("inner list");
    auto inner_list = inner_list_field.push_list();
    auto inner_record = inner_list.push_record();
    inner_record.push_field("str").add("str2");
  }

  row.cancel();
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    auto rec = list.push_record();
    rec.push_field("int").add(int64_t{3});
    auto inner_list_field = rec.push_field("inner list");
    auto inner_list = inner_list_field.push_list();
    auto inner_record = inner_list.push_record();
    inner_record.push_field("str").add("str3");
  }
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 2u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)),
              (list{record{
                {"int", int64_t{1}},
                {"inner list", list{record{
                                 {"str", "str1"},
                               }}},
              }}));
  CHECK_EQUAL(materialize(output.at(1u, 0u)),
              (list{record{
                {"int", int64_t{3}},
                {"inner list", list{record{
                                 {"str", "str3"},
                               }}},
              }}));
}

TEST(remove row with non empty list after pushing empty lists to previous rows) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("list").push_list();
    row.push_field("int").add(int64_t{10});
  }
  {
    auto row = sut.push_row();
    row.push_field("int").add(int64_t{20});
  }

  auto row = sut.push_row();
  {
    row.push_field("list").push_list().add(int64_t{1});
    row.push_field("int").add(int64_t{30});
  }

  row.cancel();
  sut.push_row().push_field("str").add("str0");
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 3u);
  REQUIRE_EQUAL(output.columns(), 2u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), int64_t{10});
  CHECK_EQUAL(materialize(output.at(0u, 1u)), caf::none);
  CHECK_EQUAL(materialize(output.at(1u, 0u)), int64_t{20});
  CHECK_EQUAL(materialize(output.at(1u, 1u)), caf::none);
  CHECK_EQUAL(materialize(output.at(2u, 0u)), caf::none);
  CHECK_EQUAL(materialize(output.at(2u, 1u)), "str0");
}

TEST(remove row empty list) {
  adaptive_table_slice_builder sut;
  {
    auto row = sut.push_row();
    row.push_field("list").push_list();
    row.push_field("int").add(int64_t{10});
  }

  auto row = sut.push_row();
  row.push_field("int").add(int64_t{20});

  row.cancel();
  auto output = sut.finish();
  REQUIRE_EQUAL(output.rows(), 1u);
  REQUIRE_EQUAL(output.columns(), 1u);
  CHECK_EQUAL(materialize(output.at(0u, 0u)), int64_t{10});
}

TEST(Add nulls to fields that didnt have values added when adaptive builder is
       constructed with a schema) {
  const auto schema = vast::type{
    "a nice name", record_type{{"int1", int64_type{}},
                               {"str1", string_type{}},
                               {"nested", record_type{
                                            {"rec1", int64_type{}},
                                            {"rec2", string_type{}},
                                          }}}};
  auto sut = adaptive_table_slice_builder{schema};
  sut.push_row().push_field("int1").add(int64_t{5238592});
  auto out = sut.finish(schema.name());
  REQUIRE_EQUAL(schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 4u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{5238592});
  CHECK_EQUAL(materialize(out.at(0u, 1u)), caf::none);
  CHECK_EQUAL(materialize(out.at(0u, 2u)), caf::none);
  CHECK_EQUAL(materialize(out.at(0u, 3u)), caf::none);
}

TEST(Allow new fields to be added when adaptive builder is constructed with a
       schema and allow_fields_discovery set to true) {
  const auto starting_schema
    = vast::type{"a nice name", record_type{{"int1", int64_type{}}}};

  auto sut = adaptive_table_slice_builder{starting_schema, true};
  sut.push_row().push_field("int1").add(int64_t{5238592});
  sut.push_row().push_field("int2").add(int64_t{1});
  auto out = sut.finish();

  const auto schema = vast::type{record_type{
    {"int1", int64_type{}},
    {"int2", int64_type{}},
  }};
  const auto expected_schema = vast::type{schema.make_fingerprint(), schema};
  CHECK_EQUAL(expected_schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 2u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{5238592});
  CHECK_EQUAL(materialize(out.at(0u, 1u)), caf::none);
  CHECK_EQUAL(materialize(out.at(1u, 0u)), caf::none);
  CHECK_EQUAL(materialize(out.at(1u, 1u)), int64_t{1});
}

TEST(Add enumeration type from a string representation to a basic field) {
  const auto enum_type = enumeration_type{{"enum1"}, {"enum2"}, {"enum3"}};
  const auto starting_schema
    = vast::type{"a nice name", record_type{{"enum", enum_type}}};

  auto sut = adaptive_table_slice_builder{starting_schema};
  sut.push_row().push_field("enum").add("enum2");
  auto out = sut.finish(starting_schema.name());
  CHECK_EQUAL(starting_schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 1u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)),
              detail::narrow_cast<enumeration>(*enum_type.resolve("enum2")));
}

TEST(Add enumeration type from a string representation to a list of enums) {
  const auto enum_type = enumeration_type{{"enum5"}, {"enum6"}, {"enum7"}};
  const auto starting_schema
    = vast::type{"a nice name", record_type{{"list", list_type{enum_type}}}};

  auto sut = adaptive_table_slice_builder{starting_schema};
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    list.add("enum7");
    list.add("enum5");
  }
  auto out = sut.finish(starting_schema.name());
  CHECK_EQUAL(starting_schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 1u);
  CHECK_EQUAL(
    materialize(out.at(0u, 0u)),
    (list{detail::narrow_cast<enumeration>(*enum_type.resolve("enum7")),
          detail::narrow_cast<enumeration>(*enum_type.resolve("enum5"))}));
}

TEST(Add enumeration type from an enum representation to a basic field) {
  const auto enum_type = enumeration_type{{"enum1"}, {"enum2"}, {"enum3"}};
  const auto starting_schema
    = vast::type{"a nice name", record_type{{"enum", enum_type}}};

  auto sut = adaptive_table_slice_builder{starting_schema};
  const auto input
    = detail::narrow_cast<enumeration>(*enum_type.resolve("enum2"));
  sut.push_row().push_field("enum").add(input);
  auto out = sut.finish(starting_schema.name());
  CHECK_EQUAL(starting_schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 1u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), input);
}

TEST(Add enumeration type from an enum representation to a list of enums) {
  const auto enum_type = enumeration_type{{"enum5"}, {"enum6"}, {"enum7"}};
  const auto starting_schema
    = vast::type{record_type{{"list", list_type{enum_type}}}};

  const auto input_1
    = detail::narrow_cast<enumeration>(*enum_type.resolve("enum7"));
  const auto input_2
    = detail::narrow_cast<enumeration>(*enum_type.resolve("enum5"));
  auto sut = adaptive_table_slice_builder{starting_schema};
  {
    auto row = sut.push_row();
    auto list_field = row.push_field("list");
    auto list = list_field.push_list();
    list.add(input_1);
    list.add(input_2);
  }
  auto out = sut.finish();
  CHECK_EQUAL((type{starting_schema.make_fingerprint(), starting_schema}),
              out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 1u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), (list{input_1, input_2}));
}

TEST(Add none for enumerations that dont exist)
{
  const auto enum_type = enumeration_type{{"enum1"}, {"enum2"}, {"enum3"}};
  const auto starting_schema
    = vast::type{"a nice name", record_type{{"enum", enum_type}}};

  auto sut = adaptive_table_slice_builder{starting_schema};
  sut.push_row().push_field("enum").add("enum4");
  auto out = sut.finish(starting_schema.name());
  CHECK_EQUAL(starting_schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 1u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), caf::none);
}

TEST(Fixed fields builder can be reused after finish call) {
  const auto schema
    = vast::type{"a nice name",
                 record_type{{"int1", int64_type{}}, {"str1", string_type{}}}};
  auto sut = adaptive_table_slice_builder{schema, true};

  {
    auto row = sut.push_row();
    row.push_field("int1").add(int64_t{1});
    row.push_field("str1").add("str");
  }
  auto out = sut.finish(schema.name());
  REQUIRE_EQUAL(schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{1});
  CHECK_EQUAL(materialize(out.at(0u, 1u)), "str");

  {
    auto row = sut.push_row();
    row.push_field("int1").add(int64_t{2});
    row.push_field("str1").add("str2");
  }
  out = sut.finish(schema.name());
  REQUIRE_EQUAL(schema, out.schema());

  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{2});
  CHECK_EQUAL(materialize(out.at(0u, 1u)), "str2");
}

TEST(Fixed fields builder add record type) {
  const auto schema = vast::type{
    "a nice name",
    record_type{
      {"record", record_type{
                   {"int", int64_type{}},
                   {"list", list_type{record_type{
                              {"str", string_type{}},
                              {"nested list", list_type{int64_type{}}},
                            }}},
                 }}}};
  auto sut = adaptive_table_slice_builder{schema};
  {
    auto row = sut.push_row();
    auto record_field = row.push_field("record");
    auto record = record_field.push_record();
    record.push_field("int").add(int64_t{1});
    auto list_field = record.push_field("list");
    auto list = list_field.push_list();
    auto list_record = list.push_record();
    list_record.push_field("str").add("str1");
    auto nested_list_field = list_record.push_field("nested list");
    auto nested_list = nested_list_field.push_list();
    nested_list.add(int64_t{1});
    nested_list.add(int64_t{2});
  }
  {
    auto row = sut.push_row();
    auto record_field = row.push_field("record");
    auto record = record_field.push_record();
    record.push_field("int").add(int64_t{2});
    auto list_field = record.push_field("list");
    auto list = list_field.push_list();
    {
      auto list_record = list.push_record();
      list_record.push_field("str").add("str2");
      auto nested_list_field = list_record.push_field("nested list");
      auto nested_list = nested_list_field.push_list();
      nested_list.add(int64_t{3});
      nested_list.add(int64_t{4});
    }
    {
      auto list_record = list.push_record();
      list_record.push_field("str").add("str3");
      auto nested_list_field = list_record.push_field("nested list");
      auto nested_list = nested_list_field.push_list();
      nested_list.add(int64_t{100});
    }
  }
  auto out = sut.finish(schema.name());
  REQUIRE_EQUAL(schema, out.schema());
  REQUIRE_EQUAL(out.rows(), 2u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{1});
  CHECK_EQUAL(materialize(out.at(0u, 1u)),
              (list{
                {record{
                  {"str", "str1"},
                  {"nested list", list{int64_t{1}, int64_t{2}}},
                }},
              }));
  CHECK_EQUAL(materialize(out.at(1u, 0u)), int64_t{2});
  CHECK_EQUAL(materialize(out.at(1u, 1u)),
              (list{
                {record{
                  {"str", "str2"},
                  {"nested list", list{int64_t{3}, int64_t{4}}},
                }},
                {record{
                  {"str", "str3"},
                  {"nested list", list{{int64_t{100}}}},
                }},
              }));
}

TEST(Fixed fields builder remove record type row) {
  const auto schema = vast::type{
    "a nice name",
    record_type{
      {"record", record_type{
                   {"int", int64_type{}},
                   {"list", list_type{record_type{
                              {"str", string_type{}},
                              {"nested list", list_type{int64_type{}}},
                            }}},
                 }}}};
  auto sut = adaptive_table_slice_builder{schema};
  auto row_1 = sut.push_row();
  {
    auto record_field = row_1.push_field("record");
    auto record = record_field.push_record();
    record.push_field("int").add(int64_t{1});
    auto list_field = record.push_field("list");
    auto list = list_field.push_list();
    auto list_record = list.push_record();
    list_record.push_field("str").add("str1");
    auto nested_list_field = list_record.push_field("nested list");
    auto nested_list = nested_list_field.push_list();
    nested_list.add(int64_t{1});
    nested_list.add(int64_t{2});
  }
  row_1.cancel();
  {
    auto row = sut.push_row();
    auto record_field = row.push_field("record");
    auto record = record_field.push_record();
    record.push_field("int").add(int64_t{2});
    auto list_field = record.push_field("list");
    auto list = list_field.push_list();
    {
      auto list_record = list.push_record();
      list_record.push_field("str").add("str2");
      auto nested_list_field = list_record.push_field("nested list");
      auto nested_list = nested_list_field.push_list();
      nested_list.add(int64_t{3});
      nested_list.add(int64_t{4});
    }
    {
      auto list_record = list.push_record();
      list_record.push_field("str").add("str3");
      auto nested_list_field = list_record.push_field("nested list");
      auto nested_list = nested_list_field.push_list();
      nested_list.add(int64_t{100});
    }
  }
  auto out = sut.finish(schema.name());
  REQUIRE_EQUAL(schema, out.schema());
  REQUIRE_EQUAL(out.rows(), 1u);
  REQUIRE_EQUAL(out.columns(), 2u);
  CHECK_EQUAL(materialize(out.at(0u, 0u)), int64_t{2});
  CHECK_EQUAL(materialize(out.at(0u, 1u)),
              (list{
                {record{
                  {"str", "str2"},
                  {"nested list", list{int64_t{3}, int64_t{4}}},
                }},
                {record{
                  {"str", "str3"},
                  {"nested list", list{{int64_t{100}}}},
                }},
              }));
}
