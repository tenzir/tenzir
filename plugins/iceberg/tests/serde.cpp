//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/facade.hpp"

#include <tenzir/test/test.hpp>

#include <cstdint>
#include <cstring>

namespace tenzir::plugins::iceberg {

namespace {

/// Little-endian bytes per the Iceberg spec's binary single-value
/// serialization.
template <class T>
auto le_bytes(T value) -> std::vector<uint8_t> {
  auto result = std::vector<uint8_t>(sizeof(T));
  std::memcpy(result.data(), &value, sizeof(T));
  return result;
}

auto utf8_bytes(std::string_view value) -> std::vector<uint8_t> {
  return {value.begin(), value.end()};
}

auto make_serialized_file() -> SerializedDataFile {
  return SerializedDataFile{
    .path = "s3://warehouse/ns/table/data/class_uid=1001/x.parquet",
    .record_count = 12345,
    .file_size = 67890,
    .spec_id = 0,
    .partition = {
      SerializedLiteral{
        .type = static_cast<int32_t>(LiteralType::int64),
        .is_null = false,
        .value = le_bytes(int64_t{1001}),
      },
      SerializedLiteral{
        .type = static_cast<int32_t>(LiteralType::int32),
        .is_null = false,
        .value = le_bytes(int32_t{20642}),
      },
      SerializedLiteral{
        .type = static_cast<int32_t>(LiteralType::string),
        .is_null = false,
        .value = utf8_bytes("eu-west-1"),
      },
      SerializedLiteral{
        .type = static_cast<int32_t>(LiteralType::string),
        .is_null = true,
        .value = {},
      },
      SerializedLiteral{
        .type = static_cast<int32_t>(LiteralType::timestamp_tz),
        .is_null = false,
        .value = le_bytes(int64_t{1783468800000000}),
      },
    },
    .column_sizes = {{1, 100}, {2, 200}},
    .value_counts = {{1, 12345}, {2, 12345}},
    .null_value_counts = {{1, 0}, {2, 42}},
    .nan_value_counts = {},
    .lower_bounds = {{1, le_bytes(int64_t{1})}},
    .upper_bounds = {{1, le_bytes(int64_t{99})}},
    .split_offsets = {4, 1048576},
    .sort_order_id = std::nullopt,
  };
}

TEST("data file handles round-trip through their persistable form") {
  auto serialized = make_serialized_file();
  auto file = DataFile::deserialize(serialized);
  REQUIRE(file.has_value());
  auto roundtripped = file->serialize();
  REQUIRE(roundtripped.has_value());
  CHECK(*roundtripped == serialized);
}

TEST("unpartitioned data file handles round-trip") {
  auto serialized = make_serialized_file();
  serialized.partition.clear();
  serialized.spec_id = std::nullopt;
  auto file = DataFile::deserialize(serialized);
  REQUIRE(file.has_value());
  auto roundtripped = file->serialize();
  REQUIRE(roundtripped.has_value());
  CHECK(*roundtripped == serialized);
}

TEST("unknown literal type tags fail to restore") {
  auto serialized = make_serialized_file();
  serialized.partition.push_back(SerializedLiteral{
    .type = 999,
    .is_null = false,
    .value = le_bytes(int64_t{1}),
  });
  auto file = DataFile::deserialize(serialized);
  REQUIRE(not file.has_value());
  CHECK(file.error().kind == Error::Kind::permanent);
}

} // namespace

} // namespace tenzir::plugins::iceberg
