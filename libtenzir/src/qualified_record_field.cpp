//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/qualified_record_field.hpp"

#include "tenzir/data.hpp"
#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/legacy_type.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

namespace tenzir {

qualified_record_field::qualified_record_field(const class type& schema,
                                               const offset& index) noexcept {
  TENZIR_ASSERT(!schema.name().empty());
  TENZIR_ASSERT(!index.empty());
  const auto* rt = try_as<record_type>(&schema);
  TENZIR_ASSERT(rt);
  schema_name_ = schema.name();
  // We cannot assign the field_view directly, but rather need to store a field
  // with a corrected name, as that needs to be flattened here.
  auto field = rt->field(index);
  field_ = {
    rt->key(index),
    std::move(field.type),
  };
}

qualified_record_field::qualified_record_field(
  std::string_view schema_name, std::string_view field_name,
  const class type& field_type) noexcept {
  if (field_name.empty()) {
    // backwards compat with partition v0
    field_.type = {schema_name, field_type};
    schema_name_ = field_.type.name();
  } else if (schema_name.empty()) {
    field_.type = {field_name, field_type};
    field_.name = field_.type.name();
  } else {
    const class type intermediate = {
      schema_name,
      record_type{
        {field_name, field_type},
      },
    };
    // This works because the field shares the lifetime of the intermediate
    // record type, keeping the references alive.
    *this = qualified_record_field(intermediate, {0});
  }
}

std::string_view qualified_record_field::schema_name() const noexcept {
  return schema_name_;
}

std::string_view qualified_record_field::field_name() const noexcept {
  return field_.name;
}

std::string qualified_record_field::name() const noexcept {
  if (schema_name_.empty())
    return std::string{field_.name};
  if (field_.name.empty())
    return std::string{schema_name_};
  return fmt::format("{}.{}", schema_name_, field_.name);
}

bool qualified_record_field::is_standalone_type() const noexcept {
  return field_.name.empty();
}

class type qualified_record_field::type() const noexcept {
  return field_.type;
}

bool operator==(const qualified_record_field& x,
                const qualified_record_field& y) noexcept {
  return std::tie(x.schema_name_, x.field_.name, x.field_.type)
         == std::tie(y.schema_name_, y.field_.name, y.field_.type);
}

bool operator<(const qualified_record_field& x,
               const qualified_record_field& y) noexcept {
  return std::tie(x.schema_name_, x.field_.name, x.field_.type)
         < std::tie(y.schema_name_, y.field_.name, y.field_.type);
}

bool inspect(caf::binary_serializer& f, qualified_record_field& x) {
  std::string schema_name = std::string{x.schema_name_};
  legacy_type field_type = x.field_.type.to_legacy_type();
  return detail::apply_all(f, schema_name, x.field_.name, field_type);
}

bool inspect(caf::deserializer& f, qualified_record_field& x) {
  static_assert(caf::deserializer::is_loading);
  // This overload exists for backwards compatibility. In some cases, we used
  // to serialize qualified record fields using CAF. Back then, the qualified
  // record field had these three members:
  // - std::string schema_name
  // - std::string field_name
  // - legacy_type field_type
  std::string schema_name = {};
  std::string field_name = {};
  legacy_type field_type = {};
  auto result = detail::apply_all(f, schema_name, field_name, field_type);
  if (result)
    x = qualified_record_field{schema_name, field_name,
                               type::from_legacy_type(field_type)};
  return result;
}

bool inspect(detail::legacy_deserializer& f, qualified_record_field& x) {
  std::string schema_name = {};
  std::string field_name = {};
  legacy_type field_type = {};
  auto result = f(schema_name, field_name, field_type);
  if (result)
    x = qualified_record_field{schema_name, field_name,
                               type::from_legacy_type(field_type)};
  return result;
}

} // namespace tenzir

namespace std {

size_t hash<tenzir::qualified_record_field>::operator()(
  const tenzir::qualified_record_field& f) const noexcept {
  return tenzir::hash(f.name(), f.type());
}

} // namespace std
