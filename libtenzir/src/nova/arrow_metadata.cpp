//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/arrow_metadata.hpp"

#include <arrow/type.h>

namespace tenzir::nova {

auto ArrowMetadata::from_arrow(arrow::Schema const& schema,
                               std::string_view fallback_name)
  -> ArrowMetadata {
  // Decode attributes without inspecting physical column types that legacy
  // type conversion may not support.
  auto decoded = type::from_arrow(*arrow::schema({}, schema.metadata()));
  return {std::string{decoded.name().empty() ? fallback_name : decoded.name()},
          decoded.attribute("internal").has_value()};
}

auto ArrowMetadata::to_meta(storage::Index length) const -> Events::Meta {
  auto result = Events::Meta::make_empty(length, name);
  result.internal = Array<Bool>{storage::BitMap{length, internal}};
  return result;
}

auto ArrowMetadata::apply(type schema) const -> type {
  if (schema.name() != name) {
    schema = type{name, schema};
  }
  if (internal) {
    schema = type{schema, {{"internal", ""}}};
  }
  return schema;
}

} // namespace tenzir::nova
