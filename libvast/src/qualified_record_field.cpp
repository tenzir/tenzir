//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/qualified_record_field.hpp"

#include "vast/data.hpp"
#include "vast/detail/inspection_common.hpp"
#include "vast/hash/hash.hpp"
#include "vast/legacy_type.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

namespace vast {

qualified_record_field::qualified_record_field(const class type& layout,
                                               const offset& index) noexcept {
  VAST_ASSERT(!layout.name().empty());
  VAST_ASSERT(!index.empty());
  const auto* rt = caf::get_if<record_type>(&layout);
  VAST_ASSERT(rt);
  layout_name_ = layout.name();
  // We cannot assign the field_view directly, but rather need to store a field
  // with a corrected name, as that needs to be flattened here.
  auto field = rt->field(index);
  field_ = {
    rt->key(index),
    std::move(field.type),
  };
}

qualified_record_field::qualified_record_field(
  std::string_view layout_name, std::string_view field_name,
  const class type& field_type) noexcept {
  if (field_name.empty()) {
    // backwards compat with partition v0
    field_.type = {layout_name, field_type};
    layout_name_ = field_.type.name();
  } else if (layout_name.empty()) {
    field_.type = {field_name, field_type};
    field_.name = field_.type.name();
  } else {
    const class type intermediate = {
      layout_name,
      record_type{
        {field_name, field_type},
      },
    };
    // This works because the field shares the lifetime of the intermediate
    // record type, keeping the references alive.
    *this = qualified_record_field(intermediate, {0});
  }
}

std::string_view qualified_record_field::layout_name() const noexcept {
  return layout_name_;
}

std::string_view qualified_record_field::field_name() const noexcept {
  return field_.name;
}

std::string qualified_record_field::name() const noexcept {
  if (layout_name_.empty())
    return std::string{field_.name};
  if (field_.name.empty())
    return std::string{layout_name_};
  return fmt::format("{}.{}", layout_name_, field_.name);
}

bool qualified_record_field::is_standalone_type() const noexcept {
  return field_.name.empty();
}

class type qualified_record_field::type() const noexcept {
  return field_.type;
}

bool operator==(const qualified_record_field& x,
                const qualified_record_field& y) noexcept {
  return std::tie(x.layout_name_, x.field_.name, x.field_.type)
         == std::tie(y.layout_name_, y.field_.name, y.field_.type);
}

bool operator<(const qualified_record_field& x,
               const qualified_record_field& y) noexcept {
  return std::tie(x.layout_name_, x.field_.name, x.field_.type)
         < std::tie(y.layout_name_, y.field_.name, y.field_.type);
}

bool inspect(caf::binary_serializer& f, qualified_record_field& x) {
  std::string layout_name = std::string{x.layout_name_};
  legacy_type field_type = x.field_.type.to_legacy_type();
  return detail::apply_all(f, layout_name, x.field_.name, field_type);
}

bool inspect(caf::deserializer& f, qualified_record_field& x) {
  static_assert(caf::deserializer::is_loading);
  // This overload exists for backwards compatibility. In some cases, we used
  // to serialize qualified record fields using CAF. Back then, the qualified
  // record field had these three members:
  // - std::string layout_name
  // - std::string field_name
  // - legacy_type field_type
  std::string layout_name = {};
  std::string field_name = {};
  legacy_type field_type = {};
  auto result = detail::apply_all(f, layout_name, field_name, field_type);
  if (result)
    x = qualified_record_field{layout_name, field_name,
                               type::from_legacy_type(field_type)};
  return result;
}

bool inspect(detail::legacy_deserializer& f, qualified_record_field& x) {
  std::string layout_name = {};
  std::string field_name = {};
  legacy_type field_type = {};
  auto result = f(layout_name, field_name, field_type);
  if (result)
    x = qualified_record_field{layout_name, field_name,
                               type::from_legacy_type(field_type)};
  return result;
}

} // namespace vast

namespace std {

size_t hash<vast::qualified_record_field>::operator()(
  const vast::qualified_record_field& f) const noexcept {
  return vast::hash(f.name(), f.type());
}

} // namespace std
