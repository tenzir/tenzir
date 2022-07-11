//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE transform

#include "vast/transform.hpp"

#include "vast/plugin.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/test.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/data.h>
#include <caf/settings.hpp>
#include <caf/test/dsl.hpp>

#include <string_view>

using namespace std::literals;

const auto testdata_layout = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"desc", vast::string_type{}},
    {"index", vast::integer_type{}},
  },
};

const auto testdata_layout2 = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"desc", vast::string_type{}},
    {"index", vast::integer_type{}},
    {"note", vast::string_type{}},
  },
};

const auto testresult_layout2 = vast::type{
  "testdata",
  vast::record_type{
    {"uid", vast::string_type{}},
    {"index", vast::integer_type{}},
  },
};

struct transforms_fixture {
  transforms_fixture() {
    vast::factory<vast::table_slice_builder>::initialize();
  }

  // Creates a table slice with a single string field and random data.
  static vast::table_slice
  make_transforms_testdata(vast::table_slice_encoding encoding
                           = vast::defaults::import::table_slice_type) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      REQUIRE(builder->add(str, "test-datum", vast::integer{i}));
    }
    vast::table_slice slice = builder->finish();
    return slice;
  }

  /// Creates a table slice with four fields and another with two of the same
  /// fields.
  static std::tuple<vast::table_slice, vast::table_slice>
  make_proj_and_del_testdata() {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, testdata_layout2);
    REQUIRE(builder);
    auto builder2 = vast::factory<vast::table_slice_builder>::make(
      vast::defaults::import::table_slice_type, testresult_layout2);
    REQUIRE(builder2);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      auto str3 = fmt::format("note {}", i);
      REQUIRE(builder->add(str, str2, vast::integer{i}, str3));
      REQUIRE(builder2->add(str, vast::integer{i}));
    }
    return {builder->finish(), builder2->finish()};
  }

  /// Creates a table slice with ten rows(type, record_batch), a second having
  /// only the row with index==2 and a third having only the rows with index>5.
  static std::tuple<vast::table_slice, vast::table_slice, vast::table_slice>
  make_where_testdata(vast::table_slice_encoding encoding
                      = vast::defaults::import::table_slice_type) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder);
    auto builder2 = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder2);
    auto builder3 = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout);
    REQUIRE(builder3);
    for (int i = 0; i < 10; ++i) {
      auto uuid = vast::uuid::random();
      auto str = fmt::format("{}", uuid);
      auto str2 = fmt::format("test-datum {}", i);
      REQUIRE(builder->add(str, str2, vast::integer{i}));
      if (i == 2) {
        REQUIRE(builder2->add(str, str2, vast::integer{i}));
      }
      if (i > 5) {
        REQUIRE(builder3->add(str, str2, vast::integer{i}));
      }
    }
    return {builder->finish(), builder2->finish(), builder3->finish()};
  }

  const vast::transform_plugin* rename_plugin
    = vast::plugins::find<vast::transform_plugin>("rename");
};

vast::type layout(caf::expected<std::vector<vast::transform_batch>> batches) {
  return (*batches)[0].layout;
}

vast::table_slice
as_table_slice(caf::expected<std::vector<vast::transform_batch>> batches) {
  return vast::table_slice{(*batches)[0].batch};
}

FIXTURE_SCOPE(transform_tests, transforms_fixture)

TEST(drop_step) {
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  const auto* drop_plugin = vast::plugins::find<vast::transform_plugin>("drop");
  REQUIRE(drop_plugin);
  auto drop_step = unbox(
    drop_plugin->make_transform_step({{"fields", vast::list{"desc", "note"}}}));
  auto add_failed = drop_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto deleted = unbox(drop_step->finish());
  REQUIRE_EQUAL(deleted.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(deleted), expected_slice);
  auto invalid_drop_step
    = unbox(drop_plugin->make_transform_step({{"fields", vast::list{"xxx"}}}));
  auto invalid_add_failed
    = invalid_drop_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_dropped = unbox(invalid_drop_step->finish());
  REQUIRE_EQUAL(not_dropped.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(not_dropped), slice);
  auto schema_drop_step = unbox(
    drop_plugin->make_transform_step({{"schemas", vast::list{"testdata"}}}));
  auto schema_add_failed
    = schema_drop_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!schema_add_failed);
  auto dropped = unbox(invalid_drop_step->finish());
  CHECK(dropped.empty());
}

TEST(select step) {
  auto project_step = unbox(
    vast::make_transform_step("select", {{"fields", vast::list{"index", "uid"}}}));
  auto invalid_project_step
    = unbox(vast::make_transform_step("select", {{"fields", vast::list{"xxx"}}}));
  // Arrow test:
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  auto add_failed = project_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto projected = unbox(project_step->finish());
  REQUIRE_EQUAL(projected.size(), 1ull);
  REQUIRE_EQUAL(as_table_slice(projected), expected_slice);
  auto invalid_add_failed
    = invalid_project_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_projected = unbox(invalid_project_step->finish());
  CHECK(not_projected.empty());
}

TEST(replace step) {
  auto slice = make_transforms_testdata();
  auto replace_step = unbox(
    vast::make_transform_step("replace", {{"field", "uid"}, {"value", "xxx"}}));
  auto add_failed = replace_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto replaced = unbox(replace_step->finish());
  REQUIRE_EQUAL(replaced.size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).num_fields(),
    3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(as_table_slice(replaced).layout()).field(0).name,
    "uid");
  const auto table_slice = as_table_slice(replaced);
  CHECK_EQUAL(table_slice.at(0, 0), vast::data_view{"xxx"sv});
}

TEST(where step) {
  auto [slice, single_row_slice, multi_row_slice]
    = make_where_testdata(vast::defaults::import::table_slice_type);
  CHECK_EQUAL(slice.rows(), 10ull);
  CHECK_EQUAL(single_row_slice.rows(), 1ull);
  CHECK_EQUAL(multi_row_slice.rows(), 4ull);
  auto where_plugin = vast::plugins::find<vast::transform_plugin>("where");
  REQUIRE(where_plugin);
  auto where_step
    = unbox(where_plugin->make_transform_step({{"expression", "index == +2"}}));
  REQUIRE(where_step);
  auto add_failed = where_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto selected = where_step->finish();
  REQUIRE_NOERROR(selected);
  REQUIRE_EQUAL(selected->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected), single_row_slice);
  auto where_step2
    = unbox(where_plugin->make_transform_step({{"expression", "index > +5"}}));
  REQUIRE(where_step2);
  auto add2_failed = where_step2->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add2_failed);
  auto selected2 = where_step2->finish();
  REQUIRE_NOERROR(selected2);
  REQUIRE_EQUAL(selected2->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected2), multi_row_slice);
  auto where_step3
    = unbox(where_plugin->make_transform_step({{"expression", "index > +9"}}));
  REQUIRE(where_step3);
  auto add3_failed = where_step3->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add3_failed);
  auto selected3 = where_step3->finish();
  REQUIRE_NOERROR(selected3);
  CHECK_EQUAL(selected3->size(), 0ull);
  auto where_step4 = unbox(where_plugin->make_transform_step(
    {{"expression", "#type == \"testdata\""}}));
  auto add4_failed = where_step4->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add4_failed);
  auto selected4 = where_step4->finish();
  REQUIRE_NOERROR(selected4);
  REQUIRE_EQUAL(selected4->size(), 1ull);
  CHECK_EQUAL(as_table_slice(selected4), slice);
  auto where_step5 = unbox(where_plugin->make_transform_step(
    {{"expression", "#type != \"testdata\""}}));
  auto add5_failed = where_step5->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add5_failed);
  auto selected5 = where_step5->finish();
  REQUIRE_NOERROR(selected5);
  CHECK_EQUAL(selected5->size(), 0ull);
}

TEST(anonymize step) {
  auto slice = make_transforms_testdata();
  auto hash_step = unbox(vast::make_transform_step(
    "hash", {{"field", "uid"}, {"out", "hashed_uid"}}));
  auto add_failed = hash_step->add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto anonymized = unbox(hash_step->finish());
  REQUIRE_EQUAL(anonymized.size(), 1ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(anonymized)).num_fields(),
                4ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(anonymized)).field(1).name,
                "hashed_uid");
  // TODO: not sure how we can check that the data was correctly hashed.
}

TEST(transform with multiple steps) {
  vast::transform transform("test_transform", {{"testdata"}});
  transform.add_step(unbox(vast::make_transform_step(
    "replace", {{"field", "uid"}, {"value", "xxx"}})));
  transform.add_step(unbox(
    vast::make_transform_step("drop", {{"fields", vast::list{"index"}}})));
  auto slice = make_transforms_testdata();
  auto add_failed = transform.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = transform.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(0).name, "ui"
                                                                           "d");
  CHECK_EQUAL(((*transformed)[0]).at(0, 0), vast::data_view{"xxx"sv});
  auto wrong_layout = vast::type{"stub", testdata_layout};
  wrong_layout.assign_metadata(vast::type{"foo", vast::type{}});
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, wrong_layout);
  REQUIRE(builder->add("asdf", "jklo", vast::integer{23}));
  auto wrong_slice = builder->finish();
  auto add2_failed = transform.add(std::move(wrong_slice));
  REQUIRE(!add2_failed);
  auto not_transformed = transform.finish();
  REQUIRE_NOERROR(not_transformed);
  REQUIRE_EQUAL(not_transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).num_fields(),
    3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(0).name,
    "uid");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(1).name,
    "desc");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*not_transformed)[0].layout()).field(2).name,
    "index");
  CHECK_EQUAL((*not_transformed)[0].at(0, 0), vast::data_view{"asdf"sv});
  CHECK_EQUAL((*not_transformed)[0].at(0, 1), vast::data_view{"jklo"sv});
  CHECK_EQUAL((*not_transformed)[0].at(0, 2), vast::data{vast::integer{23}});
}

TEST(transform rename layout) {
  vast::transform transform("test_transform", {{"testdata"}});
  auto rename_settings = vast::record{
    {"schemas", vast::list{vast::record{
                  {"from", std::string{"testdata"}},
                  {"to", std::string{"testdata_renamed"}},
                }}},
  };
  transform.add_step(
    unbox(rename_plugin->make_transform_step(rename_settings)));
  transform.add_step(unbox(
    vast::make_transform_step("drop", {{"fields", vast::list{"index"}}})));
  auto slice = make_transforms_testdata();
  REQUIRE_SUCCESS(transform.add(std::move(slice)));
  auto transformed = transform.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
}

TEST(transformation engine - single matching transform) {
  std::vector<vast::transform> transforms;
  transforms.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  transforms.emplace_back("t2", std::vector<std::string>{"foo"});
  auto& transform1 = transforms.at(0);
  auto& transform2 = transforms.at(1);
  transform1.add_step(
    unbox(vast::make_transform_step("drop", {{"fields", vast::list{"uid"}}})));
  transform2.add_step(unbox(
    vast::make_transform_step("drop", {{"fields", vast::list{"index"}}})));
  vast::transformation_engine engine(std::move(transforms));
  auto slice = make_transforms_testdata();
  auto add_failed = engine.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = engine.finish();
  REQUIRE_EQUAL(transformed->size(), 1ull);
  // We expect that only one transformation has been applied.
  REQUIRE_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 2ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(0).name, "des"
                                                                           "c");
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).field(1).name,
    "index");
}

TEST(transformation engine - multiple matching transforms) {
  std::vector<vast::transform> transforms;
  transforms.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  transforms.emplace_back("t2", std::vector<std::string>{"testdata"});
  auto& transform1 = transforms.at(0);
  auto& transform2 = transforms.at(1);
  transform1.add_step(
    unbox(vast::make_transform_step("drop", {{"fields", vast::list{"uid"}}})));
  transform2.add_step(unbox(
    vast::make_transform_step("drop", {{"fields", vast::list{"index"}}})));
  vast::transformation_engine engine(std::move(transforms));
  auto slice
    = make_transforms_testdata(vast::defaults::import::table_slice_type);
  REQUIRE_EQUAL(slice.encoding(), vast::defaults::import::table_slice_type);
  auto add_failed = engine.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = engine.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL((*transformed)[0].encoding(),
                vast::defaults::import::table_slice_type);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 1ull);
}

TEST(transformation engine - aggregate validation transforms) {
  std::vector<vast::transform> transforms;
  transforms.emplace_back("t", std::vector<std::string>{"testdata"});
  transforms.at(0).add_step(
    unbox(vast::make_transform_step("summarize", {{"group-by", {"foo"}}})));
  vast::transformation_engine engine(std::move(transforms));
  auto validation1 = engine.validate(
    vast::transformation_engine::allow_aggregate_transforms::yes);
  CHECK_SUCCESS(validation1);
  auto validation2 = engine.validate(
    vast::transformation_engine::allow_aggregate_transforms::no);
  CHECK_FAILURE(validation2);
}

FIXTURE_SCOPE_END()
