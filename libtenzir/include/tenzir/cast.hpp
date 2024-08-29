//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/passthrough.hpp"
#include "tenzir/die.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/type.hpp"

#include <caf/expected.hpp>

namespace tenzir {

template <type_or_concrete_type FromType, class ValueType,
          type_or_concrete_type ToType, class... Args>
auto cast_value(const FromType& from_type, ValueType&& value,
                const ToType& to_type, Args&&... args) noexcept;

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

  static auto cast_value(const FromType& from_type, auto&&,
                         const ToType& to_type, auto&&...) noexcept
    -> caf::expected<type_to_data_t<ToType>> {
    return caf::make_error(ec::convert_error,
                           fmt::format("cannot cast from '{}' to '{}': not "
                                       "implemented",
                                       from_type, to_type));
  }
};

template <type_or_concrete_type FromType, type_or_concrete_type ToType>
  requires(!concrete_type<FromType> || !concrete_type<ToType>)
struct cast_helper<FromType, ToType> {
  template <type_or_concrete_type Type>
  static auto is_valid(const Type& type) noexcept {
    if constexpr (!concrete_type<Type>) {
      if (!type) {
        return false;
      }
    }
    return true;
  }

  static auto can_cast(const FromType& from_type,
                       const ToType& to_type) noexcept -> caf::expected<void> {
    if (!is_valid(from_type) || !is_valid(to_type)) {
      return caf::make_error(ec::logic_error,
                             fmt::format("cannot cast from '{}' to '{}': both "
                                         "types must be valid",
                                         from_type, to_type));
    }
    const auto f
      = [&]<concrete_type ConcreteFromType, concrete_type ConcreteToType>(
          const ConcreteFromType& from_type,
          const ConcreteToType& to_type) noexcept {
          return cast_helper<ConcreteFromType, ConcreteToType>::can_cast(
            from_type, to_type);
        };
    if constexpr (concrete_type<FromType>) {
      return caf::visit(f, detail::passthrough(from_type), to_type);
    } else if constexpr (concrete_type<ToType>) {
      return caf::visit(f, from_type, detail::passthrough(to_type));
    } else {
      return caf::visit(f, from_type, to_type);
    }
  }

  static auto
  cast(const FromType& from_type,
       const std::shared_ptr<type_to_arrow_array_t<type>>& from_array,
       const ToType& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<ToType>> {
    TENZIR_ASSERT_EXPENSIVE(can_cast(from_type, to_type));
    const auto f
      = [&]<concrete_type ConcreteFromType, concrete_type ConcreteToType>(
          const ConcreteFromType& from_type,
          const ConcreteToType& to_type) noexcept
      -> std::shared_ptr<type_to_arrow_array_t<type>> {
      if constexpr (concrete_type<FromType>) {
        return cast_helper<ConcreteFromType, ConcreteToType>::cast(
          from_type, from_array, to_type);
      } else {
        return cast_helper<ConcreteFromType, ConcreteToType>::cast(
          from_type,
          std::static_pointer_cast<type_to_arrow_array_t<ConcreteFromType>>(
            from_array),
          to_type);
      }
    };
    if constexpr (concrete_type<FromType>) {
      return caf::visit(f, detail::passthrough(from_type), to_type);
    } else if constexpr (concrete_type<ToType>) {
      return caf::visit(f, from_type, detail::passthrough(to_type));
    } else {
      return caf::visit(f, from_type, to_type);
    }
  }

  template <class InputType>
    requires(std::same_as<type_to_data_t<FromType>, InputType>
             || std::same_as<view<type_to_data_t<FromType>>, InputType>)
  static auto cast_value(const FromType& from_type, const InputType& data,
                         const ToType& to_type) noexcept {
    const auto f
      = [&]<concrete_type ConcreteFromType, concrete_type ConcreteToType>(
          const ConcreteFromType& from_type,
          const ConcreteToType& to_type) noexcept
      -> caf::expected<type_to_data_t<ToType>> {
      auto v = cast_helper<ConcreteFromType, ConcreteToType>::cast_value(
        from_type, get_underlying_data<ConcreteFromType>(data), to_type);
      if constexpr (concrete_type<ToType>) {
        return v;
      } else {
        if (not v) {
          return std::move(v.error());
        }
        return std::move(*v);
      }
    };
    if constexpr (concrete_type<FromType>) {
      return caf::visit(f, detail::passthrough(from_type), to_type);
    } else if constexpr (concrete_type<ToType>) {
      return caf::visit(f, from_type, detail::passthrough(to_type));
    } else {
      return caf::visit(f, from_type, to_type);
    }
  }

private:
  template <concrete_type To>
  static auto get_underlying_data(const data& d) {
    return caf::get<type_to_data_t<To>>(d);
  }

  template <concrete_type To>
  static auto get_underlying_data(const data_view& d) {
    return caf::get<view<type_to_data_t<To>>>(d);
  }

  template <concrete_type To>
  static auto get_underlying_data(const type_to_data_t<To>& d) {
    return d;
  }

  template <concrete_type To>
    requires(not std::same_as<view<type_to_data_t<To>>, type_to_data_t<To>>)
  static auto get_underlying_data(const view<type_to_data_t<To>>& d) {
    return d;
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

  static auto cast_value(const Type&, const type_to_data_t<Type>& value,
                         const Type&) noexcept
    -> caf::expected<type_to_data_t<Type>> {
    return value;
  }

  static auto cast_value(const string_type&, std::string_view view,
                         const string_type&) noexcept
    -> caf::expected<type_to_data_t<Type>>
    requires(std::same_as<string_type, Type>)
  {
    return materialize(view);
  }

  static auto
  cast_value(const blob_type&, view<blob> view, const blob_type&) noexcept
    -> caf::expected<type_to_data_t<Type>>
    requires(std::same_as<blob_type, Type>)
  {
    return materialize(view);
  }
};

template <complex_type Type>
struct cast_helper<Type, Type> {
  static auto can_cast(const Type& from_type, const Type& to_type) noexcept
    -> caf::expected<void> {
    if (from_type != to_type) {
      return caf::make_error(ec::convert_error,
                             fmt::format("cannot cast from '{}' to '{}': not "
                                         "implemented",
                                         from_type, to_type));
    }
    return {};
  }

  static auto cast(const Type& from_type,
                   std::shared_ptr<type_to_arrow_array_t<Type>> from_array,
                   const Type& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<Type>> {
    TENZIR_ASSERT_EXPENSIVE(can_cast(from_type, to_type));
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
    if (!can_cast_value_types) {
      return caf::make_error(ec::convert_error,
                             fmt::format("cannot cast from '{}' to '{}': {}",
                                         from_type, to_type,
                                         can_cast_value_types.error()));
    }
    return {};
  }

  static auto cast(const list_type& from_type,
                   std::shared_ptr<type_to_arrow_array_t<list_type>> from_array,
                   const list_type& to_type) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<list_type>> {
    TENZIR_ASSERT_EXPENSIVE(can_cast(from_type, to_type));
    if (from_type == to_type) {
      return from_array;
    }
    const auto offsets = from_array->offsets();
    const auto cast_values = cast_helper<type, type>::cast(
      from_type.value_type(), from_array->values(), to_type.value_type());
    return type_to_arrow_array_t<list_type>::FromArrays(
             *offsets, *cast_values, arrow::default_memory_pool(),
             from_array->null_bitmap(), from_array->null_count())
      .ValueOrDie();
  }

  template <class InputType>
    requires(
      std::same_as<std::remove_cvref_t<InputType>, type_to_data_t<list_type>>
      or std::same_as<std::remove_cvref_t<InputType>,
                      view<type_to_data_t<list_type>>>)
  static auto cast_value(const list_type& from_type, const InputType& value,
                         const list_type& to_type) noexcept
    -> caf::expected<type_to_data_t<list_type>> {
    if (from_type == to_type) {
      return on_same_input_and_output_types(value);
    }
    auto output = to_type.construct();
    output.reserve(value.size());
    for (const auto& val : value) {
      auto cast_val = cast_helper<type, type>::cast_value(
        from_type.value_type(), val, to_type.value_type());
      if (not cast_val) {
        return std::move(cast_val.error());
      }
      output.push_back(std::move(*cast_val));
    }
    return output;
  }

private:
  static auto
  on_same_input_and_output_types(const type_to_data_t<list_type>& in)
    -> type_to_data_t<list_type> {
    return in;
  }

  static auto on_same_input_and_output_types(view<type_to_data_t<list_type>> in)
    -> type_to_data_t<list_type> {
    return materialize(in);
  }
};

template <>
struct cast_helper<map_type, map_type> {
  static auto can_cast(const map_type&, const map_type&) noexcept
    -> caf::expected<void> {
    return caf::make_error(ec::convert_error,
                           "cast not supported for map types");
  }

  static auto
  cast(const map_type&, std::shared_ptr<type_to_arrow_array_t<map_type>>,
       const map_type&) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<map_type>> {
    die("map_type array cast not implemented");
  }

  static auto cast_value(const map_type&, const map&, const map_type&) noexcept
    -> caf::expected<type_to_data_t<map_type>> {
    return caf::make_error(ec::convert_error,
                           "cast not supported for map types");
  }

  static auto cast_value(const map_type&, view<map>, const map_type&) noexcept
    -> caf::expected<type_to_data_t<map_type>> {
    return caf::make_error(ec::convert_error,
                           "cast not supported for map types");
  }
};

template <>
struct cast_helper<record_type, record_type> {
  static auto
  can_cast(const record_type& from_type, const record_type& to_type) noexcept
    -> caf::expected<void> {
    if (from_type == to_type) {
      return {};
    }
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
    TENZIR_ASSERT_EXPENSIVE(can_cast(from_type, to_type));
    if (from_type == to_type) {
      return from_array;
    }
    auto fields = arrow::FieldVector{};
    fields.reserve(to_type.num_fields());
    auto children = arrow::ArrayVector{};
    children.reserve(to_type.num_fields());
    for (auto&& to_field : to_type.fields()) {
      auto from_field_array
        = from_array->GetFieldByName(std::string{to_field.name});
      fields.push_back(to_field.type.to_arrow_field(to_field.name));
      if (not from_field_array) {
        children.push_back(arrow::MakeArrayOfNull(to_field.type.to_arrow_type(),
                                                  from_array->length())
                             .ValueOrDie());
        continue;
      }
      // TODO: Wrong function!
      // TODO: Assert that it exists.
      auto from_field_type = type{};
      for (auto from_field : from_type.fields()) {
        if (from_field.name == to_field.name) {
          from_field_type = std::move(from_field.type);
          break;
        }
      }
      auto f = [](auto from_type, auto to_type) {};
      return std::visit(f, from_field_type, to_field.type);
      tenzir::cast_value(from_field_type, from_field_array, to_field.type);
    }
    return make_struct_array(from_array->length(), from_array->null_bitmap(),
                             fields, children);
  }

  template <class InputType>
    requires(
      std::same_as<std::remove_cvref_t<InputType>, type_to_data_t<record_type>>
      or std::same_as<std::remove_cvref_t<InputType>,
                      view<type_to_data_t<record_type>>>)
  static auto cast_value(const record_type& from_type, const InputType& in,
                         const record_type& to_type) noexcept
    -> caf::expected<type_to_data_t<record_type>> {
    if (from_type == to_type) {
      return on_same_input_and_output_types(in);
    }
    // NOLINTNEXTLINE
    auto impl
      = [&](const auto& impl, const record_type& to_type,
            std::string_view key_prefix) noexcept -> caf::expected<record> {
      auto ret = type_to_data_t<record_type>{};
      for (const auto& to_field : to_type.fields()) {
        const auto key = key_prefix.empty()
                           ? std::string{to_field.name}
                           : fmt::format("{}.{}", key_prefix, to_field.name);
        if (const auto* r = caf::get_if<record_type>(&to_field.type)) {
          auto maybe_nested_record = impl(impl, *r, key);
          if (not maybe_nested_record) {
            return std::move(maybe_nested_record.error());
          }
          ret[to_field.name] = std::move(*maybe_nested_record);
          continue;
        }
        const auto index = from_type.resolve_key(key);
        if (not index) {
          // The field does not exist, so we insert a null.
          ret[to_field.name] = caf::none;
          continue;
        }
        // The field exists, so we can insert the cast column.
        auto input_at_path = get_input_at_path(in, key);
        if (not input_at_path) {
          return std::move(input_at_path.error());
        }
        if (caf::holds_alternative<view<caf::none_t>>(*input_at_path)) {
          ret[to_field.name] = caf::none;
          continue;
        }
        auto maybe_new_value = cast_helper<type, type>::cast_value(
          from_type.field(*index).type, *input_at_path, to_field.type);
        if (not maybe_new_value) {
          return std::move(maybe_new_value.error());
        }
        ret[to_field.name] = std::move(*maybe_new_value);
      }
      return ret;
    };
    return impl(impl, to_type, "");
  }

private:
  static auto
  on_same_input_and_output_types(const type_to_data_t<record_type>& in)
    -> type_to_data_t<record_type> {
    return in;
  }

  static auto
  on_same_input_and_output_types(view<type_to_data_t<record_type>> in)
    -> type_to_data_t<record_type> {
    return materialize(in);
  }

  static auto
  get_input_at_path(const type_to_data_t<record_type>& in, std::string_view key)
    -> caf::expected<data_view> {
    auto input_at_path = descend(std::addressof(in), key);
    if (not input_at_path) {
      return std::move(input_at_path.error());
    }
    TENZIR_ASSERT(*input_at_path);
    return make_view(**input_at_path);
  }

  static auto
  get_input_at_path(view<type_to_data_t<record_type>> in, std::string_view key)
    -> caf::expected<data_view> {
    auto input_at_path = descend(in, key);
    if (not input_at_path) {
      return std::move(input_at_path.error());
    }
    return *input_at_path;
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
    if (value < 0) {
      return caf::make_error(
        ec::convert_error,
        fmt::format("unable to convert negative value {} into uint64", value));
    }
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
    if (value > std::numeric_limits<int64_t>::max()) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into int64: the "
                                         "value is above the int64_t limit",
                                         value));
    }
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
        or value > std::numeric_limits<bool>::max()) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into int64: "
                                         "only '0' and '1' are supported.",
                                         value));
    }
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
    if (value > std::numeric_limits<bool>::max()) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into uint64: "
                                         "only '0' and '1' are supported.",
                                         value));
    }
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
    if (value != 0.0 and value != 1.0) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into a double: "
                                         "only '0.0' and '1.0' are supported.",
                                         value));
    }
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
    if (value > std::numeric_limits<uint32_t>::max()) {
      return caf::make_error(
        ec::convert_error, fmt::format("unable to convert {} into {}: "
                                       "the value is out of enum value range.",
                                       value, enum_type));
    }
    auto field = enum_type.field(static_cast<uint32_t>(value));
    if (field.empty()) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value doesn't correspond to any "
                                         "enum state",
                                         value, enum_type));
    }
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
        or value < std::numeric_limits<uint32_t>::min()) {
      return caf::make_error(
        ec::convert_error, fmt::format("unable to convert {} into {}: "
                                       "the value is out of enum value range.",
                                       value, enum_type));
    }
    auto field = enum_type.field(static_cast<uint32_t>(value));
    if (field.empty()) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value doesn't correspond to any "
                                         "enum state",
                                         value, enum_type));
    }
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
    if (not maybe_uint) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert {} into {}: "
                                         "the value cannot be represented as "
                                         "unsigned integer: {}",
                                         value, enum_type, maybe_uint.error()));
    }
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

template <>
struct cast_helper<enumeration_type, enumeration_type> {
  static auto
  can_cast(const enumeration_type& from, const enumeration_type& to) noexcept
    -> caf::expected<void> {
    if (from == to) {
      return {};
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert from {} to {} : "
                                       "mismatching enumeration types",
                                       from, to));
  }

  static auto cast_value(const enumeration_type& from, enumeration value,
                         const enumeration_type& to) noexcept
    -> caf::expected<enumeration> {
    auto can = can_cast(from, to);
    if (can) {
      return value;
    }
    return can.error();
  }

  static auto
  cast(const enumeration_type& from,
       std::shared_ptr<type_to_arrow_array_t<enumeration_type>> array,
       const enumeration_type& to) noexcept
    -> std::shared_ptr<type_to_arrow_array_t<enumeration_type>> {
    TENZIR_ASSERT_EXPENSIVE(can_cast(from, to));
    return array;
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
  cast_value(const FromType&, const auto& value, const string_type&) noexcept
    -> caf::expected<std::string> {
    if constexpr (std::same_as<view<type_to_data_t<FromType>>,
                               std::remove_cvref_t<decltype(value)>>) {
      return fmt::format("{}", data{materialize(value)});
    } else {
      return fmt::format("{}", data{value});
    }
  }

  static auto cast_value(const enumeration_type& enum_type, enumeration value,
                         const string_type&) noexcept
    -> caf::expected<std::string> {
    auto field_name = enum_type.field(static_cast<uint32_t>(value));
    TENZIR_ASSERT(not field_name.empty());
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
    if (auto ret = time{}; parsers::time(in, ret)) {
      return ret;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into time", in));
  }

  static auto from_str(std::string_view in, const duration_type&)
    -> caf::expected<duration> {
    if (auto ret = duration{}; parsers::duration(in, ret)) {
      return ret;
    }
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into duration", in));
  }

  static auto from_str(std::string_view in, const subnet_type&)
    -> caf::expected<subnet> {
    if (auto ret = subnet{}; parsers::net(in, ret)) {
      return ret;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into subnet", in));
  }

  static auto from_str(std::string_view in, const ip_type&)
    -> caf::expected<ip> {
    if (auto ret = ip{}; parsers::ip(in, ret)) {
      return ret;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into ip", in));
  }

  static auto from_str(std::string_view in, const null_type&)
    -> caf::expected<caf::none_t> {
    if (parsers::lit{"null"}(in)) {
      return caf::none;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a null", in));
  }

  static auto from_str(std::string_view in, const bool_type&)
    -> caf::expected<bool> {
    if (auto ret = false; parsers::boolean(in, ret)) {
      return ret;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a bool", in));
  }

  static auto from_str(std::string_view in, const uint64_type&)
    -> caf::expected<uint64_t> {
    if (auto ret = uint64_t{0}; parsers::count(in, ret)) {
      return ret;
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into an uint64",
                                       in));
  }

  static auto from_str(std::string_view in, const int64_type&)
    -> caf::expected<int64_t> {
    if (auto ret = int64_t{0}; parsers::integer(in, ret)) {
      return ret;
    }
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into an int64", in));
  }

  static auto from_str(std::string_view in, const double_type&)
    -> caf::expected<double> {
    if (auto ret = double{0}; parsers::real(in, ret)) {
      return ret;
    }
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into a double", in));
  }

  static auto from_str(std::string_view in, const enumeration_type& type)
    -> caf::expected<enumeration> {
    if (auto val = type.resolve(in)) {
      return detail::narrow_cast<enumeration>(*val);
    }
    return caf::make_error(
      ec::convert_error,
      fmt::format("unable to convert {} into enumeration {}", in, type));
  }

  static auto from_str(std::string_view in, const record_type&)
    -> caf::expected<record> {
    if (auto ret = data{}; parsers::data(in, ret)) {
      TENZIR_ASSERT(caf::holds_alternative<record>(ret));
      return caf::get<record>(ret);
    }
    return caf::make_error(
      ec::convert_error, fmt::format("unable to convert {} into a record", in));
  }

  static auto from_str(std::string_view in, const list_type&)
    -> caf::expected<list> {
    if (auto ret = data{}; parsers::data(in, ret)) {
      TENZIR_ASSERT(caf::holds_alternative<list>(ret));
      return caf::get<list>(ret);
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a list", in));
  }

  static auto from_str(std::string_view in, const map_type&)
    -> caf::expected<map> {
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a map: "
                                       "map_type is deprecated",
                                       in));
  }

  static auto from_str(std::string_view in, const blob_type&)
    -> caf::expected<blob> {
    if (auto result = base64::try_decode<blob>(in)) {
      return std::move(*result);
    }
    return caf::make_error(ec::convert_error,
                           fmt::format("unable to convert {} into a blob", in));
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
    if (value < ValType{0}) {
      return caf::make_error(ec::convert_error,
                             fmt::format("unable to convert negative numeric "
                                         "value {} into a duration type",
                                         value));
    }
    auto str = cast_helper<FromType, string_type>::cast_value(from, value,
                                                              string_type{});
    if (not str) {
      return str.error();
    }
    if (str->front() == '+') {
      str->erase(0, 1);
    }
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
auto cast(const table_slice& from_slice, const type& to_schema) noexcept
  -> table_slice;

template <concrete_type FromType, concrete_type ToType>
static auto cast_to_builder(const FromType& from_type,
                            const type_to_arrow_array_t<FromType>& in,
                            const ToType& to_type) noexcept
  -> caf::expected<std::shared_ptr<type_to_arrow_builder_t<ToType>>> {
  auto ret = to_type.make_arrow_builder(arrow::default_memory_pool());
  for (const auto& v : values(from_type, in)) {
    if (not v) {
      auto status = ret->AppendNull();
      TENZIR_ASSERT(status.ok());
      continue;
    }
    auto converted = cast_value(from_type, *v, to_type);
    if (not converted) {
      return converted.error();
    }
    auto status = append_builder(to_type, *ret, make_view(*converted));
    TENZIR_ASSERT(status.ok());
  }

  return ret;
}

} // namespace tenzir
