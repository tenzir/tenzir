//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/passthrough.hpp"
#include "vast/die.hpp"
#include "vast/type.hpp"

#include <caf/expected.hpp>

namespace vast {

namespace detail {

template <type_or_concrete_type FromType, type_or_concrete_type ToType>
struct cast_helper {
  static auto can_cast(const FromType& from_type,
                       const ToType& to_type) noexcept -> caf::expected<void> {
    return caf::make_error(ec::convert_error,
                           fmt::format("cannot cast from '{}' to '{}': not "
                                       "implemented",
                                       from_type, to_type));
  }

  static auto
  cast(const FromType&, const std::shared_ptr<type_to_arrow_array_t<FromType>>&,
       const ToType&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<ToType>> {
    die("not implemented");
  }
};

template <type_or_concrete_type FromType, type_or_concrete_type ToType>
  requires(!concrete_type<FromType> || !concrete_type<ToType>)
struct cast_helper<FromType, ToType> {
  template <type_or_concrete_type Type>
  static auto is_valid(const Type& type) noexcept {
    if constexpr (!concrete_type<Type>)
      if (!type)
        return false;
    return true;
  }

  static auto can_cast(const FromType& from_type,
                       const ToType& to_type) noexcept -> caf::expected<void> {
    if (!is_valid(from_type) || !is_valid(to_type))
      return caf::make_error(ec::logic_error,
                             fmt::format("cannot cast from '{}' to '{}': both "
                                         "types must be valid",
                                         from_type, to_type));
    const auto f
      = [&]<concrete_type ConcreteFromType, concrete_type ConcreteToType>(
          const ConcreteFromType& from_type,
          const ConcreteToType& to_type) noexcept {
          return cast_helper<ConcreteFromType, ConcreteToType>::can_cast(
            from_type, to_type);
        };
    if constexpr (concrete_type<FromType>)
      return caf::visit(f, detail::passthrough(from_type), to_type);
    else if constexpr (concrete_type<ToType>)
      return caf::visit(f, from_type, detail::passthrough(to_type));
    else
      return caf::visit(f, from_type, to_type);
  }

  static auto
  cast(const type& from_type,
       const std::shared_ptr<type_to_arrow_array_t<type>>& from_array,
       const ToType& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<ToType>> {
    VAST_ASSERT(can_cast(from_type, to_type));
    const auto f
      = [&]<concrete_type ConcreteFromType, concrete_type ConcreteToType>(
          const ConcreteFromType& from_type,
          const ConcreteToType& to_type) noexcept
      -> std::shared_ptr<type_to_arrow_array_t<type>> {
      if constexpr (concrete_type<FromType>)
        return cast_helper<ConcreteFromType, ConcreteToType>::cast(
          from_type, from_array, to_type);
      else
        return cast_helper<ConcreteFromType, ConcreteToType>::cast(
          from_type,
          std::static_pointer_cast<type_to_arrow_array_t<ConcreteFromType>>(
            from_array),
          to_type);
    };
    if constexpr (concrete_type<FromType>)
      return caf::visit(f, detail::passthrough(from_type), to_type);
    else if constexpr (concrete_type<ToType>)
      return caf::visit(f, from_type, detail::passthrough(to_type));
    else
      return caf::visit(f, from_type, to_type);
  }
};

template <basic_type Type>
struct cast_helper<Type, Type> {
  static auto can_cast(const Type&, const Type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto
  cast(const Type&, std::shared_ptr<type_to_arrow_array_t<Type>> from_array,
       const Type&) noexcept -> std::shared_ptr<type_to_arrow_array_t<Type>> {
    return from_array;
  }
};

template <complex_type Type>
struct cast_helper<Type, Type> {
  static auto can_cast(const Type& from_type, const Type& to_type) noexcept
    -> caf::expected<void> {
    if (from_type != to_type)
      return caf::make_error(ec::convert_error,
                             fmt::format("cannot cast from '{}' to '{}': not "
                                         "implemented",
                                         from_type, to_type));
    return {};
  }

  static auto cast(const Type& from_type,
                   std::shared_ptr<type_to_arrow_array_t<Type>> from_array,
                   const Type& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<Type>> {
    VAST_ASSERT(can_cast(from_type, to_type));
    return from_array;
  }
};

template <>
struct cast_helper<record_type, record_type> {
  static auto
  can_cast(const record_type& from_type, const record_type& to_type) noexcept
    -> caf::expected<void> {
    if (from_type == to_type)
      return {};
    // Castts between record types are possible unless there is a matching field
    // name with conflicting types that cannot be casted.
    for (const auto& to_leaf : to_type.leaves()) {
      const auto to_key = to_type.key(to_leaf.index);
      if (const auto from_field_index = from_type.resolve_key(to_key)) {
        const auto& from_field = from_type.field(*from_field_index);
        auto can_cast_field_types = cast_helper<type, type>::can_cast(
          from_field.type, to_leaf.field.type);
        if (!can_cast_field_types) {
          return caf::make_error(ec::unspecified,
                                 fmt::format("cannot cast from '{}' to '{}' as "
                                             "cast for matching field '{}' is "
                                             "not possible: {}",
                                             from_type, to_type, to_key,
                                             can_cast_field_types.error()));
        }
      }
    }
    return {};
  }

  static auto
  cast(const record_type& from_type,
       std::shared_ptr<type_to_arrow_array_t<record_type>> from_array,
       const record_type& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<record_type>> {
    VAST_ASSERT(can_cast(from_type, to_type));
    if (from_type == to_type)
      return from_array;
    // NOLINTNEXTLINE
    auto impl = [&](const auto& impl, const record_type& to_type,
                    std::string_view key_prefix) noexcept
      -> std::shared_ptr<type_to_arrow_array_t<record_type>> {
      auto fields = arrow::FieldVector{};
      auto children = arrow::ArrayVector{};
      fields.reserve(to_type.num_fields());
      children.reserve(to_type.num_fields());
      for (const auto& to_field : to_type.fields()) {
        const auto key = key_prefix.empty()
                           ? std::string{to_field.name}
                           : fmt::format("{}.{}", key_prefix, to_field.name);
        fields.push_back(to_field.type.to_arrow_field(to_field.name));
        if (const auto* r = caf::get_if<record_type>(&to_field.type)) {
          children.push_back(impl(impl, *r, key));
          continue;
        }
        const auto index = from_type.resolve_key(key);
        if (!index) {
          // The field does not exist, so we insert a bunch of nulls.
          children.push_back(
            arrow::MakeArrayOfNull(to_field.type.to_arrow_type(),
                                   from_array->length())
              .ValueOrDie());
          continue;
        }
        // The field exists, so we can insert the casted column.
        children.push_back(cast_helper<type, type>::cast(
          from_type.field(*index).type,
          static_cast<arrow::FieldPath>(*index).Get(*from_array).ValueOrDie(),
          to_field.type));
      }
      return type_to_arrow_array_t<record_type>::Make(children, fields)
        .ValueOrDie();
    };
    return impl(impl, to_type, "");
  }
};

} // namespace detail

/// Determines whether a cast from *from_type* to *to_type* is possible.
/// @returns Nothing on success, or an error detailing why the cast is not
/// possible on failure.
template <type_or_concrete_type FromType, type_or_concrete_type ToType>
auto can_cast(const FromType& from_type, const ToType& to_type) noexcept
  -> caf::expected<void> {
  return detail::cast_helper<FromType, ToType>::can_cast(from_type, to_type);
}

/// Casts a table slice to another schema.
/// @param from_slice The table slice to cast.
/// @param to_schema The schema to cast towards.
/// @pre can_cast(from_slice.schema(), to_schema)
/// @post result.schema() == to_schema
/// @returns A slice that exactly matches *to_schema*.
auto cast(table_slice from_slice, const type& to_schema) noexcept
  -> table_slice;

} // namespace vast
