//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/operator_control_plane.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/test.hpp"

using namespace vast;

namespace {

template <class OnWarnCallable = decltype([](caf::error) {})>
class operator_control_plane_mock : public operator_control_plane {
public:
  explicit operator_control_plane_mock(OnWarnCallable on_warn,
                                       std::vector<type> schemas = {})
    : on_warn_{std::move(on_warn)}, schemas_{std::move(schemas)} {
  }

  explicit operator_control_plane_mock(std::function<void()> on_abort)
    : on_abort_{std::move(on_abort)} {
  }

  auto self() noexcept -> system::execution_node_actor::base& override {
    FAIL("no mock implementation available");
  }

  auto node() noexcept -> system::node_actor override {
    FAIL("no mock implementation available");
  }

  auto abort(caf::error) noexcept -> void override {
    on_abort_();
  }

  auto warn(caf::error warning) noexcept -> void override {
    on_warn_(std::move(warning));
  }

  auto emit(table_slice) noexcept -> void override {
    FAIL("Unexpected call to operator_control_plane::emit");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    return schemas_;
  }

  auto concepts() const noexcept -> const concepts_map& override {
    FAIL("Unexpected call to operator_control_plane::concepts");
  }

private:
  OnWarnCallable on_warn_;
  std::vector<type> schemas_;
  std::function<void()> on_abort_ = []() {
    FAIL("Unexpected call to operator_control_plane::abort");
  };
};

auto create_sut(generator<chunk_ptr> json_chunk_gen,
                operator_control_plane& control_plane,
                std::vector<std::string> args = {}) -> generator<table_slice> {
  auto const* plugin = vast::plugins::find<vast::parser_plugin>("json");
  auto sut
    = plugin->make_parser(args, std::move(json_chunk_gen), control_plane);
  REQUIRE(sut);
  return std::move(*sut);
}

generator<chunk_ptr>
make_chunk_generator(std::vector<std::string_view> jsons_to_chunkify) {
  for (const auto json : jsons_to_chunkify) {
    co_yield chunk::make(json.data(), json.size(), vast::chunk::deleter_type{});
  }
  co_return;
}

struct unknown_schema_fixture {
  std::function<void(caf::error)> default_on_warn = [](caf::error e) {
    FAIL(fmt::format("Unexpected call to operator_control_plane::warn with {}",
                     e));
  };
  std::function<void(caf::error)> ignore_warn = [](caf::error) {};
  operator_control_plane_mock<decltype(default_on_warn)> control_plane_mock{
    default_on_warn};
};

auto make_expected_schema(const type& data_schema) -> type {
  return type{data_schema.make_fingerprint(), data_schema};
}

} // namespace

FIXTURE_SCOPE(unknown_schema_json_parser_tests, unknown_schema_fixture)

TEST(events with same schema) {
  auto in_json = R"(
        {"12345":{"a":1234,"b":5678,"c":9998877}}
        {"12345":{"a":1234,"b":5678,"c":9998877}}
        )";
  const auto expected_schema = make_expected_schema(
    vast::type{record_type{{"12345", record_type{
                                       {"a", int64_type{}},
                                       {"b", int64_type{}},
                                       {"c", int64_type{}},
                                     }}}});
  auto sut = create_sut(make_chunk_generator({in_json}), control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  const auto& slice = output_slices.front();
  REQUIRE_EQUAL(slice.columns(), 3u);
  REQUIRE_EQUAL(expected_schema, slice.schema());
  REQUIRE_EQUAL(slice.rows(), 2u);
  for (auto i = 0u; i < slice.rows(); ++i) {
    CHECK_EQUAL(materialize(slice.at(i, 0u)), int64_t{1234});
    CHECK_EQUAL(materialize(slice.at(i, 1u)), int64_t{5678});
    CHECK_EQUAL(materialize(slice.at(i, 2u)), int64_t{9998877});
  }
}

TEST(event split across two chunks) {
  auto first_json = R"(
        {"12345":{"a":1234,"b":5678,"c":9998877}}
        {"12345":{"a":1234
        )";
  auto second_json = R"(
    ,"b":5678,"c":9998877}}
  )";
  const auto expected_schema = make_expected_schema(
    vast::type{record_type{{"12345", record_type{
                                       {"a", int64_type{}},
                                       {"b", int64_type{}},
                                       {"c", int64_type{}},
                                     }}}});
  auto sut = create_sut(make_chunk_generator({first_json, second_json}),
                        control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  const auto& slice = output_slices.front();
  REQUIRE_EQUAL(slice.columns(), 3u);
  REQUIRE_EQUAL(expected_schema, slice.schema());
  REQUIRE_EQUAL(slice.rows(), 2u);
  for (auto i = 0u; i < slice.rows(); ++i) {
    CHECK_EQUAL(materialize(slice.at(i, 0u)), int64_t{1234});
    CHECK_EQUAL(materialize(slice.at(i, 1u)), int64_t{5678});
    CHECK_EQUAL(materialize(slice.at(i, 2u)), int64_t{9998877});
  }
}

TEST(skip field with invalid value and emit a warning) {
  auto in_json = R"(
        {"12345":{"a":1234,"b":5678,"c":1D}}
        )";
  auto warn_issued = false;
  auto mock = operator_control_plane_mock{[&warn_issued](auto&&) {
    if (warn_issued)
      FAIL("Warning expected to be emitted only once");
    warn_issued = true;
  }};
  auto sut = create_sut(make_chunk_generator({in_json}), mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK(warn_issued);
  auto& slice = output_slices.front();
  REQUIRE_EQUAL(slice.columns(), 2u);
  REQUIRE_EQUAL(slice.rows(), 1u);

  CHECK_EQUAL(materialize(slice.at(0u, 0u)), int64_t{1234});
  CHECK_EQUAL(materialize(slice.at(0u, 1u)), int64_t{5678});
}

TEST(different schemas in each event are combined into one) {
  auto in_json = R"(
        {"field1":{"a":-1,"b":-5,"c":-1000}}
        {"field2":[0.0, 1.0, 2.0]}
        {"field3":"str", "field2":[4.0]}
        )";

  const auto expected_schema = make_expected_schema(vast::type{record_type{
    {"field1",
     record_type{
       {"a", int64_type{}},
       {"b", int64_type{}},
       {"c", int64_type{}},
     }},
    {"field2", list_type{double_type{}}},
    {"field3", string_type{}},
  }});
  auto sut = create_sut(make_chunk_generator({in_json}), control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  auto& slice = output_slices.front();
  REQUIRE_EQUAL(expected_schema, slice.schema());
  REQUIRE_EQUAL(slice.columns(), 5u);
  REQUIRE_EQUAL(slice.rows(), 3u);

  CHECK_EQUAL(materialize(slice.at(0u, 0u)), int64_t{-1});
  CHECK_EQUAL(materialize(slice.at(0u, 1u)), int64_t{-5});
  CHECK_EQUAL(materialize(slice.at(0u, 2u)), int64_t{-1000});
  CHECK_EQUAL(materialize(slice.at(0u, 3u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(0u, 4u)), caf::none);

  CHECK_EQUAL(materialize(slice.at(1u, 0u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(1u, 1u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(1u, 2u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(1u, 3u)), (list{0.0, 1.0, 2.0}));
  CHECK_EQUAL(materialize(slice.at(1u, 4u)), caf::none);

  CHECK_EQUAL(materialize(slice.at(2u, 0u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(2u, 1u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(2u, 2u)), caf::none);
  CHECK_EQUAL(materialize(slice.at(2u, 3u)), (list{4.0}));
  CHECK_EQUAL(materialize(slice.at(2u, 4u)), "str");
}

TEST(inproperly formatted json in all input chunks results in 0 slices) {
  auto issues_warnings = 0u;
  auto mock = operator_control_plane_mock{[&issues_warnings](auto&&) {
    ++issues_warnings;
  }};
  auto json = R"({f3iujo5u3};fd/nha":1234)";
  auto sut = create_sut(make_chunk_generator({json, json, json}), mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  CHECK(output_slices.empty());
  // At least warn for each chunk.
  CHECK(issues_warnings >= 3u);
}

// This test stopped working after we started to ignore fields that can't be
// parsed. The {"12345":{"a":1234{ seems to be parsed correctly up until the
// last '{' which gives us a proper slice. Most likely we want such cases to be
// handled properly in the future (TODO). TEST(retrieve one event from joining
// 2nd and 3rd chunk despite 1st
//      and 2nd chunk being inproperly formatted json after join) {
//   auto warn_issued = false;
//   auto mock = operator_control_plane_mock{[&warn_issued](auto&&) {
//     if (warn_issued)
//       FAIL("Warning expected to be emitted only once");
//     warn_issued = true;
//   }};
//   auto json = R"({"12345":{"a":1234)";
//   auto json3 = "}}";
//   auto sut = create_sut(make_chunk_generator({json, json, json3}), mock);
//   auto output_slices = std::vector<vast::table_slice>{};
//   for (auto slice : sut) {
//     output_slices.push_back(std::move(slice));
//   }
//   CHECK(warn_issued);
//   REQUIRE_EQUAL(output_slices.size(), 1u);
//   REQUIRE_EQUAL(output_slices.front().rows(), 1u);
//   REQUIRE_EQUAL(output_slices.front().columns(), 1u);
//   CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), int64_t{1234});
// }

TEST(properly formatted json followed by inproperly formatted one and ending
       with a proper one in multiple chunks) {
  constexpr auto proper_json = std::string_view{R"({"123":"123"})"};
  constexpr auto not_a_json = std::string_view{"sfgsdger?}u"};
  auto warn_issued = false;
  auto mock = operator_control_plane_mock{[&warn_issued](auto&&) {
    // don't count how many times it was issued. It is sort of tested in other
    // places.
    warn_issued = true;
  }};
  auto sut = create_sut(
    make_chunk_generator({proper_json.substr(0, 2), proper_json.substr(2, 2),
                          proper_json.substr(4), not_a_json.substr(0, 2),
                          not_a_json.substr(2), proper_json}),
    mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  REQUIRE_EQUAL(output_slices.front().columns(), 1u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "123");
  CHECK_EQUAL(materialize(output_slices.front().at(1u, 0u)), "123");
  CHECK(warn_issued);
}

TEST(split results into two slices when input chunks has more events than a
       maximum a table_slice can hold) {
  auto in_json = std::string{};
  for (auto i = 0u; i <= defaults::import::table_slice_size; ++i) {
    in_json.append(R"({"a": 5})");
  }
  auto sut = create_sut(make_chunk_generator({in_json}), control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  CHECK_EQUAL(output_slices.size(), 2u);
  CHECK_EQUAL(output_slices.front().rows(), defaults::import::table_slice_size);
}

TEST(empty chunk from input generator causes the parser to yield an empty table
       slice) {
  auto gen = []() -> generator<chunk_ptr> {
    co_yield chunk::make_empty();
  };
  auto sut = create_sut(gen(), control_plane_mock);
  auto output_slice = std::optional<vast::table_slice>{};
  for (auto slice : sut) {
    output_slice = std::move(slice);
    break;
  }
  REQUIRE(output_slice);
  CHECK_EQUAL(*output_slice, table_slice{});
}

TEST(empty chunk after parsing json formatted chunk causes the parser to yield
       accumulated result) {
  auto gen = []() -> generator<chunk_ptr> {
    constexpr auto json = std::string_view{R"({"a": 5})"};
    co_yield chunk::make(json.data(), json.size(), vast::chunk::deleter_type{});
    co_yield chunk::make_empty();
    co_return;
  };
  auto sut = create_sut(gen(), control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
}

TEST(null in the input json results in the value being missing in the schema) {
  auto sut = create_sut(
    make_chunk_generator({R"({"a": 5, "b": null})", R"({"c": null})"}),
    control_plane_mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 1u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), int64_t{5u});
}

// This test may be used to test malformed JSON handling in the future.
// Currently we cannot handle such scenarios so the expected slices is 0.
TEST(0 slices and abort from event that is properly formatted JSON among
       multiple invalid JSONs in a single chunk) {
  auto abort_issued = false;
  auto mock
    = operator_control_plane_mock{std::function<void()>{[&abort_issued] {
        abort_issued = true;
      }}};
  auto sut = create_sut(make_chunk_generator({R"(
      {"1"{}{"dekh234rfweKKKKKKKKkkXDDDDDDDDDrjgbf} : 1}
      {"d}{}{"}|SDG:SDIKT83753
      gfd,knbfhgreg
      jumnlk
      {}
      {"2" : 2})"}),
                        mock);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE(output_slices.empty());
  CHECK(abort_issued);
}

FIXTURE_SCOPE_END()

struct known_schema_no_infer_fixture : public unknown_schema_fixture {
  std::vector<std::string> args{"--selector=field_to_chose:modulee",
                                "--no-infer"};
};

FIXTURE_SCOPE(no_infer_known_schema_json_parser_tests,
              known_schema_no_infer_fixture)

TEST(simple slice) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                    {"b", record_type{{"b.a", string_type{}}}},
                                    {"c", list_type{duration_type{}}},
                                  }};
  auto cp_mock = operator_control_plane_mock{default_on_warn, {fixed_schema}};
  auto sut = create_sut(
    make_chunk_generator(
      {R"({"a": 5, "field_to_chose" : "great_field", "b": {"b.a" : "str"}, "c" : ["10ns", "20ns"]})"}),
    cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }

  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().schema(), fixed_schema);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 4u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 1u)), int64_t{5u});
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 2u)), "str");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 3u)),
              (list{caf::timespan{std::chrono::nanoseconds{10}},
                    caf::timespan{std::chrono::nanoseconds{20}}}));
}

TEST(ignore fields not present in a schema) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  auto mock = operator_control_plane_mock{default_on_warn, {fixed_schema}};
  auto sut = create_sut(
    make_chunk_generator(
      {R"({"field_to_chose" : "great_field", "b": "will_i_be_ignored?"})"}),
    mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }

  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().schema(), fixed_schema);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 1u)), caf::none);
}

TEST(yield slice for each schema) {
  const auto fixed_schema_1
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  const auto fixed_schema_2
    = type{"modulee.even_greater_field", record_type{
                                           {"field_to_chose", string_type{}},
                                           {"b", string_type{}},
                                         }};

  auto cp_mock = operator_control_plane_mock{default_on_warn,
                                             {fixed_schema_1, fixed_schema_2}};
  auto sut = create_sut(
    make_chunk_generator(
      {R"({"field_to_chose" : "great_field", "a": 10})",
       R"({"field_to_chose" : "even_greater_field", "b": "str"})"}),
    cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }

  REQUIRE_EQUAL(output_slices.size(), 2u);
  CHECK_EQUAL(output_slices.front().schema(), fixed_schema_1);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 1u)), int64_t{10});

  CHECK_EQUAL(output_slices.back().schema(), fixed_schema_2);
  CHECK_EQUAL(output_slices.back().rows(), 1u);
  CHECK_EQUAL(output_slices.back().columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.back().at(0u, 0u)),
              "even_greater_field");
  CHECK_EQUAL(materialize(output_slices.back().at(0u, 1u)), "str");
}

TEST(yield empty slice when input chunk generator returns empty chunk) {
  auto cp_mock = operator_control_plane_mock{default_on_warn};
  auto sut = create_sut(make_chunk_generator({""}), cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front(), table_slice{});
}

TEST(yield two slices of the same schema due to receiving empty chunk in between
       chunks of same json events) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  auto cp_mock = operator_control_plane_mock{default_on_warn, {fixed_schema}};
  auto sut = create_sut(
    make_chunk_generator({R"({"field_to_chose" : "great_field", "a": 10})", "",
                          R"({"field_to_chose" : "great_field", "a": 10})"}),
    cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 2u);
  for (auto slice : output_slices) {
    CHECK_EQUAL(slice.schema(), fixed_schema);
    CHECK_EQUAL(slice.rows(), 1u);
    CHECK_EQUAL(slice.columns(), 2u);
    CHECK_EQUAL(materialize(slice.at(0u, 0u)), "great_field");
    CHECK_EQUAL(materialize(slice.at(0u, 1u)), int64_t{10});
  }
}

TEST(
  event split across two chunks with selector field being in the second part) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  auto cp_mock = operator_control_plane_mock{default_on_warn, {fixed_schema}};
  auto sut
    = create_sut(make_chunk_generator(
                   {R"({"a": 10,)", R"("field_to_chose" : "great_field"})"}),
                 cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().schema(), fixed_schema);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 1u)), int64_t{10});
}

TEST(issue a warning and dont output a slice when no schema can be found) {
  const auto fixed_schema
    = type{"unknown_module.great_field", record_type{
                                           {"field_to_chose", string_type{}},
                                           {"a", int64_type{}},
                                         }};
  auto warns_count = 0u;
  auto mock = operator_control_plane_mock{[&warns_count](auto&&) {
                                            ++warns_count;
                                          },
                                          {fixed_schema}};
  auto sut
    = create_sut(make_chunk_generator(
                   {R"({"a": 10,)", R"("field_to_chose" : "great_field"})"}),
                 mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE(output_slices.empty());
  CHECK_EQUAL(warns_count, 1u);
}

FIXTURE_SCOPE_END()

struct known_schema_fixture : public unknown_schema_fixture {
  std::vector<std::string> args{"--selector=field_to_chose:modulee"};
};

FIXTURE_SCOPE(known_schema_json_parser_tests, known_schema_fixture)

TEST(
  infer type for fields not within the schema) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  auto cp_mock = operator_control_plane_mock{ignore_warn, {fixed_schema}};
  auto sut = create_sut(
    make_chunk_generator(
      {R"({"a" : 10, "field_to_chose" : "great_field", "b": "some_str"})"}),
    cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 1u);
  const auto expected_schema
    = vast::type{"modulee.great_field", record_type{
                                          {"field_to_chose", string_type{}},
                                          {"a", int64_type{}},
                                          {"b", string_type{}},
                                        }};
  CHECK_EQUAL(output_slices.front().schema(), expected_schema);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 3u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 1u)), int64_t{10});
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 2u)), "some_str");
}

TEST(events with
     : no schema->schema->no schema results in 3 table slices with different
         schemas) {
  const auto fixed_schema
    = type{"modulee.great_field", record_type{
                                    {"field_to_chose", string_type{}},
                                    {"a", int64_type{}},
                                  }};
  auto cp_mock = operator_control_plane_mock{ignore_warn, {fixed_schema}};
  auto sut = create_sut(
    make_chunk_generator(
      {R"({"d" : [1, 2], "field_to_chose" : "no_schema_field"})",
       R"({"a" : 100, "field_to_chose" : "great_field"})",
       R"({"g" : "some_str", "field_to_chose" : "no_schema_field"})"}),
    cp_mock, args);
  auto output_slices = std::vector<vast::table_slice>{};
  for (auto slice : sut) {
    output_slices.push_back(std::move(slice));
  }
  REQUIRE_EQUAL(output_slices.size(), 3u);
  CHECK_EQUAL(output_slices.at(0).schema(),
              vast::type("modulee.no_schema_field",
                         record_type{
                           {"d", list_type{int64_type{}}},
                           {"field_to_chose", string_type{}},
                         }));
  CHECK_EQUAL(output_slices.at(0).rows(), 1u);
  CHECK_EQUAL(output_slices.at(0).columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.at(0).at(0u, 0u)),
              (list{int64_t{1}, int64_t{2}}));
  CHECK_EQUAL(materialize(output_slices.at(0).at(0u, 1u)), "no_schema_field");

  CHECK_EQUAL(output_slices.at(1).schema(), fixed_schema);
  CHECK_EQUAL(output_slices.at(1).rows(), 1u);
  CHECK_EQUAL(output_slices.at(1).columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.at(1).at(0u, 0u)), "great_field");
  CHECK_EQUAL(materialize(output_slices.at(1).at(0u, 1u)), int64_t{100});

  CHECK_EQUAL(output_slices.at(2).schema(),
              vast::type("modulee.no_schema_field",
                         record_type{
                           {"g", string_type{}},
                           {"field_to_chose", string_type{}},
                         }));
  CHECK_EQUAL(output_slices.at(2).rows(), 1u);
  CHECK_EQUAL(output_slices.at(2).columns(), 2u);
  CHECK_EQUAL(materialize(output_slices.at(2).at(0u, 0u)), "some_str");
  CHECK_EQUAL(materialize(output_slices.at(2).at(0u, 1u)), "no_schema_field");
}

FIXTURE_SCOPE_END()
