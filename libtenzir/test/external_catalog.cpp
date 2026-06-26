//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/external_catalog.hpp"

#include "tenzir/detail/base64.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view.hpp"

#include <filesystem>
#include <fstream>

using namespace tenzir;

namespace {

auto encode_schema(const type& schema) -> std::string {
  return detail::base64::encode(as_bytes(schema));
}

auto write_temp(std::string_view content, std::string_view name)
  -> std::filesystem::path {
  auto path = std::filesystem::temp_directory_path()
              / fmt::format("tenzir-external-catalog-test-{}", name);
  auto out = std::ofstream{path, std::ios::binary | std::ios::trunc};
  out.write(content.data(), static_cast<std::streamsize>(content.size()));
  REQUIRE(out.good());
  return path;
}

const type test_schema{
  "zeek.conn",
  record_type{
    {"id", uint64_type{}},
    {"msg", string_type{}},
  },
};

} // namespace

TEST("parse external catalog with min/max synopses") {
  const auto manifest = fmt::format(R"json(
    {{
      "partitions": [
        {{
          "id": "00000000-0000-0000-0000-000000000001",
          "schema": "{}",
          "events": 42,
          "min_import_time": "2021-01-01T00:00:00Z",
          "max_import_time": "2021-01-02T00:00:00Z",
          "version": 3,
          "synopses": [
            {{ "type": "uint64", "min": 0, "max": 99 }},
            {{ "type": "time", "min": "2021-01-01T00:00:00Z",
               "max": "2021-01-02T00:00:00Z" }}
          ]
        }}
      ]
    }})json",
                                    encode_schema(test_schema));
  const auto path = write_temp(manifest, "object.json");
  auto parsed = load_external_catalog(path);
  std::filesystem::remove(path);
  REQUIRE(parsed);
  REQUIRE_EQUAL(parsed->size(), 1u);
  const auto& p = parsed->front();
  CHECK_EQUAL(p.events, 42u);
  CHECK_EQUAL(p.version, 3u);
  CHECK_EQUAL(p.schema, test_schema);
  REQUIRE_EQUAL(p.synopses.size(), 2u);
  CHECK_EQUAL(p.synopses[0].field_type, (type{uint64_type{}}));
  CHECK_EQUAL(as<uint64_t>(p.synopses[0].min), 0u);
  CHECK_EQUAL(as<uint64_t>(p.synopses[0].max), 99u);
  CHECK_EQUAL(p.synopses[1].field_type, (type{time_type{}}));
}

TEST("parse external catalog as bare array without synopses") {
  const auto manifest = fmt::format(R"json(
    [
      {{
        "id": "00000000-0000-0000-0000-000000000002",
        "schema": "{}",
        "events": 7,
        "min_import_time": "2021-01-01T00:00:00Z",
        "max_import_time": "2021-01-02T00:00:00Z",
        "version": 3
      }}
    ])json",
                                    encode_schema(test_schema));
  const auto path = write_temp(manifest, "array.json");
  auto parsed = load_external_catalog(path);
  std::filesystem::remove(path);
  REQUIRE(parsed);
  REQUIRE_EQUAL(parsed->size(), 1u);
  CHECK_EQUAL(parsed->front().events, 7u);
  CHECK(parsed->front().synopses.empty());
}

TEST("external catalog rejects entry with missing fields") {
  const auto manifest = R"json(
    [
      {
        "id": "00000000-0000-0000-0000-000000000003"
      }
    ])json";
  const auto path = write_temp(manifest, "missing.json");
  auto parsed = load_external_catalog(path);
  std::filesystem::remove(path);
  CHECK(not parsed);
}

TEST("external catalog rejects unsupported synopsis type") {
  const auto manifest = fmt::format(R"json(
    [
      {{
        "id": "00000000-0000-0000-0000-000000000004",
        "schema": "{}",
        "events": 1,
        "min_import_time": "2021-01-01T00:00:00Z",
        "max_import_time": "2021-01-02T00:00:00Z",
        "version": 3,
        "synopses": [ {{ "type": "string", "min": "a", "max": "z" }} ]
      }}
    ])json",
                                    encode_schema(test_schema));
  const auto path = write_temp(manifest, "badsyn.json");
  auto parsed = load_external_catalog(path);
  std::filesystem::remove(path);
  CHECK(not parsed);
}

TEST("make_min_max_synopsis prunes out-of-range values") {
  auto syn = make_min_max_synopsis(type{int64_type{}}, data{int64_t{10}},
                                   data{int64_t{20}});
  REQUIRE(syn);
  // Equality with a value below the range is prunable.
  CHECK_EQUAL(syn->lookup(relational_operator::equal,
                          make_view(data{int64_t{5}})),
              std::optional{false});
  // Equality with a value above the range is prunable.
  CHECK_EQUAL(syn->lookup(relational_operator::equal,
                          make_view(data{int64_t{25}})),
              std::optional{false});
  // Equality with an in-range value cannot be ruled out.
  {
    const auto r
      = syn->lookup(relational_operator::equal, make_view(data{int64_t{15}}));
    CHECK(not r or *r);
  }
  // `> 25` is prunable because the maximum is 20.
  CHECK_EQUAL(syn->lookup(relational_operator::greater,
                          make_view(data{int64_t{25}})),
              std::optional{false});
  // `> 5` cannot be ruled out.
  {
    const auto r
      = syn->lookup(relational_operator::greater, make_view(data{int64_t{5}}));
    CHECK(not r or *r);
  }
}

TEST("make_min_max_synopsis returns null for non-range types") {
  CHECK(make_min_max_synopsis(type{string_type{}}, data{"a"}, data{"z"})
        == nullptr);
}
