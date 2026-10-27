//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/test/test.hpp>

#include <iceberg/expression/literal.h>
#include <iceberg/schema.h>
#include <iceberg/type.h>

namespace tenzir::plugins::iceberg {
namespace {

auto field(int32_t id, std::string_view name) -> ice::SchemaField {
  return ice::SchemaField::MakeOptional(id, name, ice::int64());
}

TEST("buffered schema accepts nullable additions and reordered fields") {
  auto before = ice::Schema{{field(1, "a")}};
  CHECK(can_extend_buffered_schema(before, before));
  CHECK(can_extend_buffered_schema(
    before, ice::Schema{{field(2, "b"), field(1, "a")}}));
}

TEST("buffered schema preserves field identity and existing types") {
  auto before = ice::Schema{{field(1, "a")}};
  CHECK(not can_extend_buffered_schema(before, ice::Schema{{field(2, "a")}}));
  CHECK(not can_extend_buffered_schema(before, ice::Schema{{field(1, "b")}}));
  CHECK(not can_extend_buffered_schema(
    ice::Schema{{field(1, "a"), field(2, "b")}}, before));
  CHECK(not can_extend_buffered_schema(
    before,
    ice::Schema{{ice::SchemaField::MakeOptional(1, "a", ice::string())}}));
}

TEST("buffered schema rejects required additions and tightened nullability") {
  auto before = ice::Schema{{field(1, "a")}};
  CHECK(not can_extend_buffered_schema(
    before, ice::Schema{{field(1, "a"), ice::SchemaField::MakeRequired(
                                          2, "b", ice::int64())}}));
  CHECK(not can_extend_buffered_schema(
    before,
    ice::Schema{{ice::SchemaField::MakeRequired(1, "a", ice::int64())}}));
}

TEST("buffered schema does not replace evolution defaults with nulls") {
  auto before = ice::Schema{{field(1, "a")}};
  auto added = ice::SchemaField::MakeOptional(2, "b", ice::int64());
  auto value = std::make_shared<ice::Literal>(ice::Literal::Long(7));
  CHECK(not can_extend_buffered_schema(
    before, ice::Schema{{field(1, "a"), added.WithInitialDefault(value)}}));
  CHECK(not can_extend_buffered_schema(
    before, ice::Schema{{field(1, "a"), added.WithWriteDefault(value)}}));
}

TEST("buffered schema checks nullable additions inside list elements") {
  auto make = [](std::vector<ice::SchemaField> children) {
    return ice::Schema{{ice::SchemaField::MakeOptional(
      1, "items",
      std::make_shared<ice::ListType>(ice::SchemaField::MakeOptional(
        2, "element",
        std::make_shared<ice::StructType>(std::move(children)))))}};
  };
  auto before = make({field(3, "a")});
  CHECK(
    can_extend_buffered_schema(before, make({field(3, "a"), field(4, "b")})));
  CHECK(not can_extend_buffered_schema(before, make({field(4, "a")})));
  CHECK(not can_extend_buffered_schema(
    before, make({field(3, "a"),
                  ice::SchemaField::MakeRequired(4, "b", ice::int64())})));
}

} // namespace
} // namespace tenzir::plugins::iceberg
