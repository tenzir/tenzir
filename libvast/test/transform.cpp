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
#include "vast/msgpack_table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/test/test.hpp"
#include "vast/transform_steps/delete.hpp"
#include "vast/transform_steps/hash.hpp"
#include "vast/transform_steps/project.hpp"
#include "vast/transform_steps/replace.hpp"
#include "vast/uuid.hpp"

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
    return builder->finish();
  }

  /// Creates a table slice with four fields and another with two of the same
  /// fields.
  static std::pair<vast::table_slice, vast::table_slice>
  make_proj_and_del_testdata(vast::table_slice_encoding encoding
                             = vast::defaults::import::table_slice_type) {
    auto builder = vast::factory<vast::table_slice_builder>::make(
      encoding, testdata_layout2);
    REQUIRE(builder);
    auto builder2 = vast::factory<vast::table_slice_builder>::make(
      encoding, testresult_layout2);
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
};

FIXTURE_SCOPE(transform_tests, transforms_fixture)

TEST(delete_ step) {
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  vast::delete_step delete_step({"desc", "note"});
  auto deleted = delete_step.apply(vast::table_slice{slice});
  REQUIRE_NOERROR(deleted);
  REQUIRE_EQUAL(*deleted, expected_slice);
  vast::delete_step invalid_delete_step({"xxx"});
  auto not_deleted = invalid_delete_step.apply(vast::table_slice{slice});
  REQUIRE_NOERROR(not_deleted);
  REQUIRE_EQUAL(*not_deleted, slice);
  // The default format is Arrow, so we do one more test where we force
  // MessagePack.
  auto [msgpack_slice, expected_slice2]
    = make_proj_and_del_testdata(vast::table_slice_encoding::msgpack);
  auto msgpack_deleted = delete_step.apply(vast::table_slice{msgpack_slice});
  REQUIRE(msgpack_deleted);
  REQUIRE_EQUAL(*msgpack_deleted, expected_slice2);
  auto msgpack_not_deleted
    = invalid_delete_step.apply(vast::table_slice{msgpack_slice});
  REQUIRE_NOERROR(msgpack_not_deleted);
  REQUIRE_EQUAL(*msgpack_not_deleted, msgpack_slice);
}

TEST(project step) {
  vast::project_step project_step({"index", "uid"});
  vast::project_step invalid_project_step({"xxx"});
  // Arrow test:
  auto [slice, expected_slice] = make_proj_and_del_testdata();
  auto projected = project_step.apply(vast::table_slice{slice});
  REQUIRE_NOERROR(projected);
  REQUIRE_EQUAL(*projected, expected_slice);
  auto not_projected = invalid_project_step.apply(vast::table_slice{slice});
  REQUIRE_EQUAL(*not_projected, slice);
  // Non-Arrow test(MessagePack):
  auto [slice2, expected_slice2]
    = make_proj_and_del_testdata(vast::table_slice_encoding::msgpack);
  auto projected2 = project_step.apply(vast::table_slice{slice2});
  REQUIRE_NOERROR(projected2);
  REQUIRE_EQUAL(*projected2, expected_slice2);
  auto not_projected2 = invalid_project_step.apply(vast::table_slice{slice2});
  REQUIRE_EQUAL(*not_projected2, slice2);
}

TEST(replace step) {
  auto slice = make_transforms_testdata();
  vast::replace_step replace_step("uid", "xxx");
  auto replaced = replace_step.apply(vast::table_slice{slice});
  REQUIRE(replaced);
  REQUIRE_NOERROR(replaced);
  REQUIRE_EQUAL(caf::get<vast::record_type>(replaced->layout()).num_fields(),
                3ull);
  CHECK_EQUAL(caf::get<vast::record_type>(replaced->layout()).field(0).name,
              "uid");
  CHECK_EQUAL((*replaced).at(0, 0), vast::data_view{"xxx"sv});
}

TEST(anonymize step) {
  auto slice = make_transforms_testdata();
  vast::hash_step hash_step("uid", "hashed_uid");
  auto anonymized = hash_step.apply(vast::table_slice{slice});
  REQUIRE_NOERROR(anonymized);
  REQUIRE_EQUAL(caf::get<vast::record_type>(anonymized->layout()).num_fields(),
                4ull);
  REQUIRE_EQUAL(caf::get<vast::record_type>(anonymized->layout()).field(3).name,
                "hashed_uid");
  // TODO: Not sure how we can check that the data was correctly hashed.
}

TEST(transform with multiple steps) {
  vast::transform transform("test_transform", {"testdata"});
  transform.add_step(std::make_unique<vast::replace_step>("uid", "xxx"));
  transform.add_step(
    std::make_unique<vast::delete_step>(std::vector<std::string>{"index"}));
  auto slice = make_transforms_testdata();
  auto transformed = transform.apply(std::move(slice));
  REQUIRE_NOERROR(transformed);
  REQUIRE_EQUAL(caf::get<vast::record_type>(transformed->layout()).num_fields(),
                2ull);
  CHECK_EQUAL(caf::get<vast::record_type>(transformed->layout()).field(0).name,
              "uid");
  CHECK_EQUAL((*transformed).at(0, 0), vast::data_view{"xxx"sv});
  auto wrong_layout = vast::type{"stub", testdata_layout};
  wrong_layout.assign_metadata(vast::type{"foo", vast::none_type{}});
  auto builder = vast::factory<vast::table_slice_builder>::make(
    vast::defaults::import::table_slice_type, wrong_layout);
  REQUIRE(builder->add("asdf", "jklo", vast::integer{23}));
  auto wrong_slice = builder->finish();
  auto not_transformed = transform.apply(std::move(wrong_slice));
  REQUIRE_NOERROR(not_transformed);
  REQUIRE_EQUAL(
    caf::get<vast::record_type>(not_transformed->layout()).num_fields(), 3ull);
  CHECK_EQUAL(
    caf::get<vast::record_type>(not_transformed->layout()).field(0).name, "ui"
                                                                          "d");
  CHECK_EQUAL(
    caf::get<vast::record_type>(not_transformed->layout()).field(1).name, "des"
                                                                          "c");
  CHECK_EQUAL(
    caf::get<vast::record_type>(not_transformed->layout()).field(2).name, "inde"
                                                                          "x");
  CHECK_EQUAL((*not_transformed).at(0, 0), vast::data_view{"asdf"sv});
  CHECK_EQUAL((*not_transformed).at(0, 1), vast::data_view{"jklo"sv});
  CHECK_EQUAL((*not_transformed).at(0, 2), vast::data{vast::integer{23}});
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
  auto transformed = engine.apply(std::move(slice));
  // We expect that only one transformation has been applied.
  REQUIRE_EQUAL(caf::get<vast::record_type>(transformed->layout()).num_fields(),
                2ull);
  CHECK_EQUAL(caf::get<vast::record_type>(transformed->layout()).field(0).name,
              "desc");
  CHECK_EQUAL(caf::get<vast::record_type>(transformed->layout()).field(1).name,
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
  auto slice = make_transforms_testdata();
  auto transformed = engine.apply(std::move(slice));
  REQUIRE_NOERROR(transformed);
  CHECK_EQUAL(caf::get<vast::record_type>(transformed->layout()).num_fields(),
              1ull);
}

FIXTURE_SCOPE_END()
