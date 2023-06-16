//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/detail/passthrough.hpp"
#include "vast/die.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <caf/expected.hpp>

namespace vast {

namespace detail {

template <class...>
struct supported_casts;

template <>
struct supported_casts<bool_type> {
  using types = caf::detail::type_list<bool_type, uint64_type, int64_type,
                                       double_type, string_type>;
};

template <>
struct supported_casts<int64_type> {
  using types
    = caf::detail::type_list<int64_type, double_type, uint64_type, bool_type,
                             duration_type, enumeration_type, string_type>;
};
template <>
struct supported_casts<uint64_type> {
  using types
    = caf::detail::type_list<uint64_type, double_type, int64_type, bool_type,
                             duration_type, enumeration_type, string_type>;
};
template <>
struct supported_casts<double_type> {
  using types
    = caf::detail::type_list<double_type, int64_type, uint64_type, bool_type,
                             duration_type, enumeration_type, string_type>;
};
template <>
struct supported_casts<duration_type> {
  using types = caf::detail::type_list<duration_type, double_type, int64_type,
                                       uint64_type, time_type, string_type>;
};
template <>
struct supported_casts<time_type> {
  using types = caf::detail::type_list<time_type, duration_type, string_type>;
};
template <>
struct supported_casts<string_type> {
  using types = concrete_types;
};
template <>
struct supported_casts<ip_type> {
  using types = caf::detail::type_list<ip_type, string_type>;
};
template <>
struct supported_casts<subnet_type> {
  using types = caf::detail::type_list<subnet_type, string_type>;
};

template <>
struct supported_casts<enumeration_type> {
  using types = caf::detail::type_list<enumeration_type, double_type,
                                       int64_type, uint64_type, string_type>;
};
template <>
struct supported_casts<list_type> {
  using types = caf::detail::type_list<list_type, string_type>;
};
template <>
struct supported_casts<map_type> {
  using types = caf::detail::type_list<map_type, string_type>;
};

template <>
struct supported_casts<record_type> {
  using types = caf::detail::type_list<record_type, string_type>;
};

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

  static auto cast_value(const FromType& from_type, auto&&,
                         const ToType& to_type, auto&&...) noexcept
    -> caf::expected<void> {
    return caf::make_error(ec::convert_error,
                           fmt::format("cannot cast from '{}' to '{}': not "
                                       "implemented",
                                       from_type, to_type));
  }

  static auto
  cast_to_builder(const FromType& from_type,
                  const std::shared_ptr<type_to_arrow_array_t<FromType>>&,
                  const ToType& to_type) noexcept -> caf::expected<void> {
    return caf::make_error(ec::convert_error,
                           fmt::format("cannot cast to builder from '{}' to "
                                       "'{}': not "
                                       "implemented",
                                       from_type, to_type));
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

  static auto cast_value(const Type&, auto&& value, const Type&) noexcept {
    return value;
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
struct cast_helper<list_type, list_type> {
  static auto
  can_cast(const list_type& from_type, const list_type& to_type) noexcept
    -> caf::expected<void> {
    auto can_cast_value_types = cast_helper<type, type>::can_cast(
      from_type.value_type(), to_type.value_type());
    if (!can_cast_value_types)
      return caf::make_error(ec::convert_error,
                             fmt::format("cannot cast from '{}' to '{}': {}",
                                         from_type, to_type,
                                         can_cast_value_types.error()));
    return {};
  }

  static auto cast(const list_type& from_type,
                   std::shared_ptr<type_to_arrow_array_t<list_type>> from_array,
                   const list_type& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<list_type>> {
    VAST_ASSERT(can_cast(from_type, to_type));
    if (from_type == to_type)
      return from_array;
    const auto offsets = from_array->offsets();
    const auto cast_values = cast_helper<type, type>::cast(
      from_type.value_type(), from_array->values(), to_type.value_type());
    return type_to_arrow_array_t<list_type>::FromArrays(
             *offsets, *cast_values, arrow::default_memory_pool(),
             from_array->null_bitmap(), from_array->null_count())
      .ValueOrDie();
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
          from_type.field(*index).type, index->get(*from_array),
          to_field.type));
      }
      return type_to_arrow_array_t<record_type>::Make(children, fields)
        .ValueOrDie();
    };
    return impl(impl, to_type, "");
  }
};

template <>
struct cast_helper<int64_type, uint64_type> {
  static auto can_cast(const int64_type&, const uint64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto
  cast_value(const int64_type&, int64_t value, const uint64_type&) noexcept
    -> caf::expected<uint64_t> {
    if (value < 0)
      return caf::make_error(
        ec::convert_error,
        fmt::format("unable to convert negative value {} into uint64", value));
    return static_cast<uint64_t>(value);
  }

  static auto
  cast(const int64_type&, std::shared_ptr<type_to_arrow_array_t<int64_type>>,
       const uint64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<uint64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<uint64_type, int64_type> {
  static auto can_cast(const uint64_type&, const int64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const uint64_type&, uint64_t value,
                         const int64_type&) noexcept -> caf::expected<int64_t> {
    if (value > std::numeric_limits<int64_t>::max())
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into int64: the "
                                         "value is above the int64_t limit",
                                         value));
    return static_cast<int64_t>(value);
  }

  static auto
  cast(const uint64_type&, std::shared_ptr<type_to_arrow_array_t<uint64_type>>,
       const int64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<int64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<int64_type, bool_type> {
  static auto can_cast(const int64_type&, const bool_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const int64_type&, int64_t value,
                         const bool_type&) noexcept -> caf::expected<bool> {
    if (value < std::numeric_limits<bool>::min()
        or value > std::numeric_limits<bool>::max())
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into int64: "
                                         "only '0' and '1' are supported.",
                                         value));
    return static_cast<bool>(value);
  }

  static auto
  cast(const int64_type&, std::shared_ptr<type_to_arrow_array_t<int64_type>>,
       const bool_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<bool_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<bool_type, int64_type> {
  static auto can_cast(const bool_type&, const int64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const bool_type&, bool value,
                         const int64_type&) noexcept -> caf::expected<int64_t> {
    return static_cast<int64_t>(value);
  }

  static auto
  cast(const bool_type&, std::shared_ptr<type_to_arrow_array_t<bool_type>>,
       const int64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<int64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<bool_type, uint64_type> {
  static auto can_cast(const bool_type&, const uint64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto
  cast_value(const bool_type&, bool value, const uint64_type&) noexcept
    -> caf::expected<uint64_t> {
    return static_cast<uint64_t>(value);
  }

  static auto
  cast(const bool_type&, std::shared_ptr<type_to_arrow_array_t<bool_type>>,
       const uint64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<uint64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<uint64_type, bool_type> {
  static auto can_cast(const uint64_type&, const bool_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const uint64_type&, uint64_t value,
                         const bool_type&) noexcept -> caf::expected<bool> {
    if (value > std::numeric_limits<bool>::max())
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into uint64: "
                                         "only '0' and '1' are supported.",
                                         value));
    return static_cast<bool>(value);
  }

  static auto
  cast(const uint64_type&, std::shared_ptr<type_to_arrow_array_t<uint64_type>>,
       const bool_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<bool_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<bool_type, double_type> {
  static auto can_cast(const bool_type&, const double_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const bool_type&, bool value,
                         const double_type&) noexcept -> caf::expected<double> {
    return static_cast<double>(value);
  }

  static auto
  cast(const bool_type&, std::shared_ptr<type_to_arrow_array_t<bool_type>>,
       const double_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<double_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<double_type, bool_type> {
  static auto can_cast(const double_type&, const bool_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const double_type&, double value,
                         const bool_type&) noexcept -> caf::expected<bool> {
    if (value != 0.0 and value != 1.0)
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into a double: "
                                         "only '0.0' and '1.0' are supported.",
                                         value));
    return static_cast<bool>(value);
  }

  static auto
  cast(const double_type&, std::shared_ptr<type_to_arrow_array_t<double_type>>,
       const bool_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<bool_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<int64_type, double_type> {
  static auto can_cast(const int64_type&, const double_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const int64_type&, int64_t value,
                         const double_type&) noexcept -> caf::expected<double> {
    return static_cast<double>(value);
  }

  static auto
  cast(const int64_type&, std::shared_ptr<type_to_arrow_array_t<int64_type>>,
       const double_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<double_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<double_type, int64_type> {
  static auto can_cast(const double_type&, const int64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const double_type&, double value,
                         const int64_type&) noexcept -> caf::expected<int64_t> {
    return static_cast<int64_t>(value);
  }

  static auto
  cast(const double_type&, std::shared_ptr<type_to_arrow_array_t<double_type>>,
       const int64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<int64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<uint64_type, double_type> {
  static auto can_cast(const uint64_type&, const double_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const uint64_type&, uint64_t value,
                         const double_type&) noexcept -> caf::expected<double> {
    return static_cast<double>(value);
  }

  static auto
  cast(const uint64_type&, std::shared_ptr<type_to_arrow_array_t<uint64_type>>,
       const double_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<double_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<double_type, uint64_type> {
  static auto can_cast(const double_type&, const uint64_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto
  cast_value(const double_type&, double value, const uint64_type&) noexcept
    -> caf::expected<uint64_t> {
    return static_cast<uint64_t>(value);
  }

  static auto
  cast(const double_type&, std::shared_ptr<type_to_arrow_array_t<double_type>>,
       const uint64_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<uint64_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<uint64_type, enumeration_type> {
  static auto can_cast(const uint64_type&, const enumeration_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const uint64_type&, uint64_t value,
                         const enumeration_type& enum_type) noexcept
    -> caf::expected<enumeration> {
    if (value > std::numeric_limits<uint32_t>::max())
      return caf::make_error(
        ec::convert_error, fmt::format("unable to convert {} into {}: "
                                       "the value is out of enum value range.",
                                       value, enum_type));
    auto field = enum_type.field(static_cast<uint32_t>(value));
    if (field.empty())
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value doesn't correspond to any "
                                         "enum state",
                                         value, enum_type));
    return static_cast<enumeration>(value);
  }

  static auto
  cast(const uint64_type&, std::shared_ptr<type_to_arrow_array_t<uint64_type>>,
       const enumeration_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<enumeration_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<int64_type, enumeration_type> {
  static auto can_cast(const int64_type&, const enumeration_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const int64_type&, int64_t value,
                         const enumeration_type& enum_type) noexcept
    -> caf::expected<enumeration> {
    if (value > std::numeric_limits<uint32_t>::max()
        or value < std::numeric_limits<uint32_t>::min())
      return caf::make_error(
        ec::convert_error, fmt::format("unable to convert {} into {}: "
                                       "the value is out of enum value range.",
                                       value, enum_type));
    auto field = enum_type.field(static_cast<uint32_t>(value));
    if (field.empty())
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value doesn't correspond to any "
                                         "enum state",
                                         value, enum_type));
    return static_cast<enumeration>(value);
  }

  static auto
  cast(const int64_type&, std::shared_ptr<type_to_arrow_array_t<int64_type>>,
       const enumeration_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<enumeration_type>> {
    die("unimplemented");
  }
};

template <>
struct cast_helper<double_type, enumeration_type> {
  static auto can_cast(const double_type&, const enumeration_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const double_type&, double value,
                         const enumeration_type& enum_type) noexcept
    -> caf::expected<enumeration> {
    auto maybe_uint = cast_helper<double_type, uint64_type>::cast_value(
      double_type{}, value, uint64_type{});
    if (not maybe_uint)
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value cannot be represented as "
                                         "unsigned integer: {}",
                                         value, enum_type, maybe_uint.error()));
    return cast_helper<uint64_type, enumeration_type>::cast_value(
      uint64_type{}, *maybe_uint, enum_type);
  }

  static auto
  cast(const double_type&, std::shared_ptr<type_to_arrow_array_t<double_type>>,
       const enumeration_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<enumeration_type>> {
    die("unimplemented");
  }
};

template <concrete_type FromType>
  requires(not std::same_as<FromType, string_type>)
struct cast_helper<FromType, string_type> {
  static auto can_cast(const FromType&, const string_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto
  cast_value(const FromType&, auto value, const string_type&) noexcept
    -> caf::expected<std::string> {
    return fmt::format("{}", data{value});
  }

  static auto cast_value(const enumeration_type& enum_type, enumeration value,
                         const string_type&) noexcept
    -> caf::expected<std::string> {
    auto field_name = enum_type.field(static_cast<uint32_t>(value));
    VAST_ASSERT(not field_name.empty());
    return std::string{field_name};
  }

  static auto
  cast(const FromType&, std::shared_ptr<type_to_arrow_array_t<FromType>>,
       const string_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<string_type>> {
    die("unimplemented");
  }
};

template <concrete_type ToType>
  requires(not std::same_as<ToType, string_type>)
struct cast_helper<string_type, ToType> {
  static auto from_str(std::string_view in, const time_type&)
    -> caf::expected<time> {
    if (auto ret = time{}; parsers::time(in, ret))
      return ret;
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into time", in));
  }

  static auto from_str(std::string_view in, const duration_type&)
    -> caf::expected<duration> {
    if (auto ret = duration{}; parsers::duration(in, ret))
      return ret;
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into duration", in));
  }

  static auto from_str(std::string_view in, const subnet_type&)
    -> caf::expected<subnet> {
    if (auto ret = subnet{}; parsers::net(in, ret))
      return ret;
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into subnet", in));
  }

  static auto from_str(std::string_view in, const ip_type&)
    -> caf::expected<ip> {
    if (auto ret = ip{}; parsers::ip(in, ret))
      return ret;
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into ip", in));
  }

  static auto from_str(std::string_view in, const bool_type&)
    -> caf::expected<bool> {
    if (auto ret = false; parsers::boolean(in, ret))
      return ret;
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a bool", in));
  }

  static auto from_str(std::string_view in, const uint64_type&)
    -> caf::expected<uint64_t> {
    if (auto ret = uint64_t{0}; parsers::count(in, ret))
      return ret;
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into an uint64",
                                       in));
  }

  static auto from_str(std::string_view in, const int64_type&)
    -> caf::expected<int64_t> {
    if (auto ret = int64_t{0}; parsers::integer(in, ret))
      return ret;
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into an int64", in));
  }

  static auto from_str(std::string_view in, const double_type&)
    -> caf::expected<double> {
    if (auto ret = double{0}; parsers::real(in, ret))
      return ret;
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into a double", in));
  }

  static auto from_str(std::string_view in, const enumeration_type& type)
    -> caf::expected<enumeration> {
    if (auto val = type.resolve(in))
      return detail::narrow_cast<enumeration>(*val);
    return caf::make_error(
      ec::convert_error,
      fmt::format("unable to convert {} into enumeration {}", in, type));
  }

  static auto from_str(std::string_view in, const record_type&)
    -> caf::expected<record> {
    if (auto ret = data{}; parsers::data(in, ret)) {
      VAST_ASSERT(caf::holds_alternative<record>(ret));
      return caf::get<record>(ret);
    }
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into a record", in));
  }

  static auto from_str(std::string_view in, const list_type&)
    -> caf::expected<list> {
    if (auto ret = data{}; parsers::data(in, ret)) {
      VAST_ASSERT(caf::holds_alternative<list>(ret));
      return caf::get<list>(ret);
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a list", in));
  }

  static auto from_str(std::string_view, const map_type&)
    -> caf::expected<void> {
    die("trying to cast string_type to map_type. Map_type is deprecated");
  }

  static auto can_cast(const string_type&, const ToType&) noexcept
    -> caf::expected<void> {
    return {};
  }

  static auto cast_value(const string_type&, std::string_view value,
                         const ToType& to) noexcept {
    return from_str(value, to);
  }

  static auto
  cast(const string_type&, std::shared_ptr<type_to_arrow_array_t<string_type>>,
       const ToType&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<ToType>> {
    die("unimplemented");
  }
};

template <class FromType>
  requires(std::same_as<FromType, int64_type>
           or std::same_as<FromType, uint64_type>
           or std::same_as<FromType, double_type>)
struct cast_helper<FromType, duration_type> {
  static auto can_cast(const FromType&, const duration_type&) noexcept
    -> caf::expected<void> {
    return {};
  }

  template <class ValType>
  static auto
  cast_value(const FromType& from, ValType value, const duration_type& to,
             std::string_view unit = "s") noexcept -> caf::expected<duration> {
    if (value < ValType{0})
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert negative numeric "
                                         "value {} into a duration type",
                                         value));
    auto str = cast_helper<FromType, string_type>::cast_value(from, value,
                                                              string_type{});
    if (not str)
      return str.error();
    if (str->front() == '+')
      str->erase(0, 1);
    *str += unit;
    return cast_helper<string_type, duration_type>::cast_value(string_type{},
                                                               *str, to);
  }

  static auto
  cast(const FromType&, std::shared_ptr<type_to_arrow_array_t<FromType>>,
       const duration_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<duration_type>> {
    die("unimplemented");
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

/// @brief Casts value from *from_type* to *to_type*.
/// @returns Nothing on success, or an error detailing why the cast is not
/// possible on failure.
template <type_or_concrete_type FromType, class ValueType,
          type_or_concrete_type ToType, class... Args>
auto cast_value(const FromType& from_type, ValueType&& value,
                const ToType& to_type, Args&&... args) noexcept {
  return detail::cast_helper<FromType, ToType>::cast_value(
    from_type, std::forward<ValueType>(value), to_type,
    std::forward<Args>(args)...);
}

/// Casts a table slice to another schema.
/// @param from_slice The table slice to cast.
/// @param to_schema The schema to cast towards.
/// @pre can_cast(from_slice.schema(), to_schema)
/// @post result.schema() == to_schema
/// @returns A slice that exactly matches *to_schema*.
auto cast(table_slice from_slice, const type& to_schema) noexcept
  -> table_slice;

template <class FromType, class ToType>
  requires(not std::same_as<FromType, ToType>)
static auto
cast_to_builder(const FromType& from_type,
                const std::shared_ptr<type_to_arrow_array_t<FromType>>& in,
                const ToType& to_type) noexcept
  -> caf::expected<std::shared_ptr<type_to_arrow_builder_t<ToType>>> {
  auto ret = to_type.make_arrow_builder(arrow::default_memory_pool());
  for (const auto& v : values(from_type, *in)) {
    if (not v) {
      auto status = ret->AppendNull();
      VAST_ASSERT(status.ok());
      continue;
    }
    auto converted = cast_value(from_type, *v, to_type);
    if (not converted)
      return converted.error();
    if constexpr (not std::is_same_v<decltype(converted), caf::expected<void>>) {
      auto status = append_builder(to_type, *ret, *converted);
      VAST_ASSERT(status.ok());
    }
  }

  return ret;
}

} // namespace vast
