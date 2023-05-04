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

template <class OnWarnCallable = decltype([]() {})>
class operator_control_plane_mock : public operator_control_plane {
public:
  explicit operator_control_plane_mock(OnWarnCallable on_warn)
    : on_warn_{std::move(on_warn)} {
  }
  auto self() noexcept -> caf::event_based_actor& override {
    FAIL("Unexpected call to operator_control_plane::self");
  }

  auto abort(caf::error) noexcept -> void override {
    FAIL("Unexpected call to operator_control_plane::abort");
  }

  auto warn(caf::error warning) noexcept -> void override {
    on_warn_(std::move(warning));
  }

  auto emit(table_slice) noexcept -> void override {
    FAIL("Unexpected call to operator_control_plane::emit");
  }

  auto demand(type) const noexcept -> size_t override {
    FAIL("Unexpected call to operator_control_plane::demand");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    FAIL("Unexpected call to operator_control_plane::schemas");
  }

  auto concepts() const noexcept -> const concepts_map& override {
    FAIL("Unexpected call to operator_control_plane::concepts");
  }

private:
  OnWarnCallable on_warn_;
};

auto create_sut(generator<chunk_ptr> json_chunk_gen,
                operator_control_plane& control_plane)
  -> generator<table_slice> {
  auto const* plugin = vast::plugins::find<vast::parser_plugin>("json");
  auto sut = plugin->make_parser({}, std::move(json_chunk_gen), control_plane);
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

struct fixture {
  std::function<void(caf::error)> default_on_warn = [](caf::error e) {
    FAIL(fmt::format("Unexpected call to operator_control_plane::warn with {}",
                     e));
  };
  operator_control_plane_mock<decltype(default_on_warn)> control_plane_mock{
    default_on_warn};
};

auto make_expected_schema(const type& data_schema) -> type {
  return type{data_schema.make_fingerprint(), data_schema};
}

} // namespace

FIXTURE_SCOPE(json_parser_tests, fixture)

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
  CHECK_EQUAL(output_slice->rows(), 0u);
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

TEST(extract event from one event that is properly formatted JSON among multiple
       invalid JSONs in a single chunk) {
  auto warns_count = 0u;
  auto mock = operator_control_plane_mock{[&warns_count](auto&&) {
    ++warns_count;
  }};
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
  REQUIRE_EQUAL(output_slices.size(), 1u);
  CHECK_EQUAL(output_slices.front().rows(), 1u);
  CHECK_EQUAL(output_slices.front().columns(), 1u);
  CHECK_EQUAL(materialize(output_slices.front().at(0u, 0u)), int64_t{2u});
  // At least 4 invalid lines should be reported.
  CHECK(warns_count >= 4);
}

FIXTURE_SCOPE_END()
