//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE transform

#include "vast/transform.hpp"

#include "vast/arrow_table_slice_builder.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/format/reader_factory.hpp"
#include "vast/msgpack_table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/schemas.hpp"
#include "vast/test/test.hpp"
#include "vast/transform_steps/aggregate_suricata_flow.hpp"
#include "vast/transform_steps/count.hpp"
#include "vast/transform_steps/delete.hpp"
#include "vast/transform_steps/hash.hpp"
#include "vast/transform_steps/project.hpp"
#include "vast/transform_steps/replace.hpp"
#include "vast/transform_steps/select.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <arrow/array/array_base.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/data.h>
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

/// Test events for the aggregation.
const auto* eve_flow_json = R"_(
{"timestamp":"2011-08-14T07:38:00.000000+0200","flow_id":929669869939483,"event_type":"flow","src_ip":"147.32.84.165","src_port":138,"dest_ip":"147.32.84.255","dest_port":138,"proto":"UDP","app_proto":"failed","flow":{"pkts_toserver":7,"pkts_toclient":2,"bytes_toserver":486,"bytes_toclient":17,"start":"2011-08-12T14:53:47.928539+0200","end":"2011-08-12T14:53:47.928552+0200","age":0,"state":"new","reason":"timeout","alerted":false}}
{"timestamp":"2011-08-14T07:38:53.914038+0200","flow_id":929669869939483,"event_type":"flow","src_ip":"147.32.84.165","src_port":138,"dest_ip":"147.32.84.255","dest_port":138,"proto":"UDP","app_proto":"failed","flow":{"pkts_toserver":7,"pkts_toclient":2,"bytes_toserver":486,"bytes_toclient":17,"start":"2011-08-12T14:53:47.928539+0200","end":"2011-08-12T14:53:47.928552+0200","age":0,"state":"new","reason":"timeout","alerted":false}}
{"timestamp":"2011-08-14T07:38:59.914038+0200","flow_id":929669869939483,"event_type":"flow","src_ip":"147.32.84.165","src_port":138,"dest_ip":"147.32.84.255","dest_port":138,"proto":"UDP","app_proto":"failed","flow":{"pkts_toserver":11,"pkts_toclient":3,"bytes_toserver":487,"bytes_toclient":19,"start":"2011-08-12T14:53:47.928539+0200","end":"2011-08-12T14:53:47.928552+0200","age":0,"state":"new","reason":"timeout","alerted":true}}
{"timestamp":"2011-08-14T07:40:53.914038+0200","flow_id":929669869939483,"event_type":"flow","src_ip":"147.32.84.165","src_port":138,"dest_ip":"147.32.84.255","dest_port":138,"proto":"UDP","app_proto":"failed","flow":{"pkts_toserver":13,"pkts_toclient":5,"bytes_toserver":488,"bytes_toclient":23,"start":"2011-08-12T14:53:47.928539+0200","end":"2011-08-12T14:53:47.928552+0200","age":0,"state":"new","reason":"timeout","alerted":false}}
)_";

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

  /// Creates a table slice with suricata fields.
  static std::vector<vast::table_slice> make_suricata_flow_testdata() {
    using factory = vast::factory<vast::format::reader>;
    factory::initialize();
    auto reader = factory::get("suricata")(caf::settings{});
    auto input = std::make_unique<std::istringstream>(eve_flow_json);
    (*reader)->reset(std::move(input));
    std::vector<vast::table_slice> slices;
    auto add_slice = [&](vast::table_slice slice) {
      slices.emplace_back(std::move(slice));
    };
    auto suricata_schema = unbox(vast::to<vast::schema>(
      vast::test::BASE_SCHEMA + vast::test::SURICATA_SCHEMA));
    (*reader)->schema(suricata_schema);
    auto [err, num] = (*reader)->read(10, 2, add_slice);
    CHECK_EQUAL(err, vast::ec::end_of_input);
    REQUIRE_EQUAL(num, 4u);
    REQUIRE_EQUAL(slices.size(), 2u);
    const vast::table_slice::size_type FLOW_COLUMNS{23};
    REQUIRE_EQUAL(slices[0].columns(), FLOW_COLUMNS);
    REQUIRE_EQUAL(slices[0].rows(), 2u);
    REQUIRE_EQUAL(slices[1].rows(), 2u);
    const vast::table_slice::size_type REASON_COLUMN{20};
    REQUIRE_EQUAL(slices[0].at(1, REASON_COLUMN), vast::data{"timeout"});
    return slices;
  }

  /// Creates a table slice with ten rows(type, record_batch), a second having
  /// only the row with index==2 and a third having only the rows with index>5.
  static std::tuple<vast::table_slice, vast::table_slice, vast::table_slice>
  make_select_testdata(vast::table_slice_encoding encoding
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
};

vast::type layout(caf::expected<std::vector<vast::transform_batch>> batches) {
  return (*batches)[0].layout;
}

vast::table_slice
first_slice(caf::expected<std::vector<vast::transform_batch>> batches) {
  return vast::arrow_table_slice_builder::create((*batches)[0].batch,
                                                 (*batches)[0].layout);
}

FIXTURE_SCOPE(transform_tests, transforms_fixture)

TEST(count step) {
  auto slice1 = make_transforms_testdata();
  auto slice2 = make_transforms_testdata();
  vast::count_step count{};
  auto slice1_err = count.add(slice1.layout(), to_record_batch(slice1));
  REQUIRE_SUCCESS(slice1_err);
  auto slice2_err = count.add(slice2.layout(), to_record_batch(slice2));
  REQUIRE_SUCCESS(slice2_err);
  auto counted = count.finish();
  REQUIRE_NOERROR(counted);
  REQUIRE_EQUAL(counted->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(first_slice(counted).layout()).num_fields(),
    1ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(first_slice(counted).layout()).field(0).name,
    "count");
  CHECK_EQUAL((first_slice(counted)).at(0, 0),
              vast::data_view{vast::count{20}});
}

TEST(delete_ step) {
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  vast::delete_step delete_step({"desc", "note"});
  auto add_failed = delete_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto deleted = delete_step.finish();
  REQUIRE_NOERROR(deleted);
  REQUIRE_EQUAL(deleted->size(), 1ull);
  REQUIRE_EQUAL(first_slice(deleted), expected_slice);
  vast::delete_step invalid_delete_step({"xxx"});
  auto invalid_add_failed
    = invalid_delete_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_deleted = invalid_delete_step.finish();
  REQUIRE_NOERROR(not_deleted);
  REQUIRE_EQUAL(not_deleted->size(), 1ull);
  REQUIRE_EQUAL(first_slice(not_deleted), slice);
}

TEST(project step) {
  vast::project_step project_step({"index", "uid"});
  vast::project_step invalid_project_step({"xxx"});
  // Arrow test:
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  auto add_failed = project_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto projected = project_step.finish();
  REQUIRE_NOERROR(projected);
  REQUIRE_EQUAL(projected->size(), 1ull);
  REQUIRE_EQUAL(first_slice(projected), expected_slice);
  auto invalid_add_failed
    = invalid_project_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!invalid_add_failed);
  auto not_projected = invalid_project_step.finish();
  REQUIRE_NOERROR(not_projected);
  REQUIRE_EQUAL(not_projected->size(), 1ull);
  REQUIRE_EQUAL(first_slice(not_projected), slice);
}

TEST(replace step) {
  auto slice = make_transforms_testdata();
  vast::replace_step replace_step("uid", "xxx");
  auto add_failed = replace_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto replaced = replace_step.finish();
  REQUIRE_NOERROR(replaced);
  REQUIRE_EQUAL(replaced->size(), 1ull);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(first_slice(replaced).layout()).num_fields(),
    3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(first_slice(replaced).layout()).field(0).name,
    "uid");
  CHECK_EQUAL((first_slice(replaced)).at(0, 0), vast::data_view{"xxx"sv});
}

TEST(select step) {
  auto [slice, single_row_slice, multi_row_slice]
    = make_select_testdata(vast::table_slice_encoding::msgpack);
  vast::select_step select_step("index==+2");
  auto add_failed = select_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto selected = select_step.finish();
  REQUIRE_NOERROR(selected);
  REQUIRE_EQUAL(selected->size(), 1ull);
  CHECK_EQUAL(first_slice(selected), single_row_slice);
  vast::select_step select_step2("index>+5");
  auto add2_failed = select_step2.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add2_failed);
  auto selected2 = select_step2.finish();
  REQUIRE_NOERROR(selected2);
  REQUIRE_EQUAL(selected2->size(), 1ull);
  CHECK_EQUAL(first_slice(selected2), multi_row_slice);
  vast::select_step select_step3("index>+9");
  auto add3_failed = select_step3.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add3_failed);
  auto selected3 = select_step3.finish();
  REQUIRE_NOERROR(selected3);
  REQUIRE_EQUAL(selected3->size(), 0ull);
}

TEST(aggregate suricata flow) {
  auto slices = make_suricata_flow_testdata();
  vast::aggregate_suricata_flow_step step{std::chrono::minutes(1)};
  for (const auto& slice : slices)
    REQUIRE_SUCCESS(step.add(slice.layout(), to_record_batch(slice)));
  auto aggregated_slices = step.finish(); // FIXME: Multiple slice out
  REQUIRE_NOERROR(aggregated_slices);
  auto aggregated1 = first_slice(aggregated_slices);
  // FIXME: More tests
}

TEST(anonymize step) {
  auto slice = make_transforms_testdata();
  vast::hash_step hash_step("uid", "hashed_uid");
  auto add_failed = hash_step.add(slice.layout(), to_record_batch(slice));
  REQUIRE(!add_failed);
  auto anonymized = hash_step.finish();
  REQUIRE_NOERROR(anonymized);
  REQUIRE_EQUAL(anonymized->size(), 1ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(anonymized)).num_fields(),
                4ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(layout(*anonymized)).field(3).name,
                "hashed_uid");
  // TODO: not sure how we can check that the data was correctly hashed.
}

TEST(transform with multiple steps) {
  vast::transform transform("test_transform", {"testdata"});
  transform.add_step(std::make_unique<vast::replace_step>("uid", "xxx"));
  transform.add_step(
    std::make_unique<vast::delete_step>(std::vector<std::string>{"index"}));
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
  wrong_layout.assign_metadata(vast::type{"foo", vast::none_type{}});
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

TEST(transformation engine - single matching transform) {
  std::vector<vast::transform> transforms;
  transforms.emplace_back("t1", std::vector<std::string>{"foo", "testdata"});
  transforms.emplace_back("t2", std::vector<std::string>{"foo"});
  auto& transform1 = transforms.at(0);
  auto& transform2 = transforms.at(1);
  transform1.add_step(
    std::make_unique<vast::delete_step>(std::vector<std::string>{"uid"}));
  transform2.add_step(
    std::make_unique<vast::delete_step>(std::vector<std::string>{"index"}));
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
    std::make_unique<vast::delete_step>(std::vector<std::string>{"uid"}));
  transform2.add_step(
    std::make_unique<vast::delete_step>(std::vector<std::string>{"index"}));
  vast::transformation_engine engine(std::move(transforms));
  auto slice = make_transforms_testdata(vast::table_slice_encoding::msgpack);
  REQUIRE_EQUAL(slice.encoding(), vast::table_slice_encoding::msgpack);
  auto add_failed = engine.add(std::move(slice));
  REQUIRE(!add_failed);
  auto transformed = engine.finish();
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(transformed->size(), 1ull);
  REQUIRE_EQUAL((*transformed)[0].encoding(),
                vast::table_slice_encoding::arrow);
  CHECK_EQUAL(
    caf::get<vast::record_type>((*transformed)[0].layout()).num_fields(), 1ull);
}

TEST(transformation engine - aggregate validation transforms) {
  std::vector<vast::transform> transforms;
  transforms.emplace_back("t", std::vector<std::string>{"testdata"});
  transforms.at(0).add_step(std::make_unique<vast::count_step>());
  vast::transformation_engine engine(std::move(transforms));
  auto validation1 = engine.validate(
    vast::transformation_engine::allow_aggregate_transforms::yes);
  CHECK_SUCCESS(validation1);
  auto validation2 = engine.validate(
    vast::transformation_engine::allow_aggregate_transforms::no);
  CHECK_FAILURE(validation2);
}

FIXTURE_SCOPE_END()
