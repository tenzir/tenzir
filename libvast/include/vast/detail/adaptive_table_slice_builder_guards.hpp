//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/series_builders.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

namespace vast::detail {

template <class T>
struct type_from_data
  : caf::detail::tl_at<
      concrete_types,
      caf::detail::tl_index_of<
        caf::detail::tl_map_t<concrete_types, type_to_data>, T>::value> {};

template <class T>
using type_from_data_t = typename type_from_data<T>::type;

class field_guard;

/// @brief A view of a list column created within adaptive_table_slice_builder.
/// Allows addition of new values into the list.
class list_guard {
private:
  class list_record_guard {
  public:
    list_record_guard(builder_provider builder_provider, list_guard& parent)
      : builder_provider_{std::move(builder_provider)}, parent_{parent} {
    }

    /// @brief Adds a field to a record nested inside a list.
    /// @param name Field name.
    /// @return Object used to append new values to a given field.
    auto push_field(std::string_view name) -> field_guard;

    ~list_record_guard() noexcept;

  private:
    builder_provider builder_provider_;
    list_guard& parent_;
  };

public:
  list_guard(builder_provider builder_provider, list_guard* parent_list_guard,
             type value_type)

    : builder_provider_{std::move(builder_provider)},
      parent{parent_list_guard},
      value_type{std::move(value_type)} {
  }

  /// @brief Adds a value to a list. Use push_record and push_list to add record
  /// and list respectively.
  /// @param view View of a value to be added.
  template <class ViewType>
    requires requires(ViewType x) { materialize(x); }
  auto add(ViewType view) -> void {
    using view_value_type = decltype(materialize(std::declval<ViewType>()));
    using vast_type = type_from_data_t<view_value_type>;
    add<vast_type>(view);
  }

  /// @brief Adds the underlying view to the list if it is of a supported type.
  auto add(const data_view& view) -> void;

  /// @brief Adds a new record as a value of the list. The parent list guard
  /// must outlive the return value of this method.
  // The record will be appended to the list when the returned guard is
  // destroyed.
  /// @return Object that enables manipulation of the created record.
  auto push_record() -> list_record_guard;

  /// @brief Adds a nested list to the current list. The parent list_guard must
  /// outlive the returned one from this method.
  /// @return Object that enables manipulation of the created list.
  auto push_list() -> list_guard;

private:
  auto propagate_type(type child_type) -> void;
  auto get_root_list_builder() -> concrete_series_builder<vast::list_type>&;

  template <concrete_type Type>
  auto add(auto view) -> void {
    if constexpr (std::is_same_v<Type, string_type>) {
      add_str(view);
      return;
    }
    add_impl<Type>(view);
  }

  auto add_str(std::string_view view) -> void {
    auto enum_type = caf::get_if<enumeration_type>(&value_type);
    if (not enum_type) {
      add_impl<string_type>(view);
      return;
    }
    auto& builder
      = get_root_list_builder()
          .get_child_builder<type_to_arrow_builder_t<enumeration_type>>(
            value_type);
    if (auto resolved = enum_type->resolve(view)) {
      const auto s = append_builder(*enum_type, builder, *resolved);
      VAST_ASSERT(s.ok());
      return;
    }
    const auto s = builder.AppendNull();
    VAST_ASSERT(s.ok());
  }

  template <concrete_type Type>
  auto add_impl(auto view) -> void {
    if (not value_type)
      propagate_type(vast::type{Type{}});
    // Casting not supported yet.
    VAST_ASSERT(caf::holds_alternative<Type>(value_type));
    const auto s = append_builder(
      caf::get<Type>(value_type),
      get_root_list_builder().get_child_builder<type_to_arrow_builder_t<Type>>(
        value_type),
      view);
    VAST_ASSERT(s.ok());
  }

  builder_provider builder_provider_;
  list_guard* parent = nullptr;
  type value_type;
  concrete_series_builder<vast::list_type>* list_builder_ = nullptr;
};

/// @brief A view of a record column created within adaptive_table_slice_builder.
/// Allows addition of new values into the individual fields of a record.
class record_guard {
public:
  record_guard(builder_provider builder_provider,
               arrow_length_type starting_fields_length)
    : builder_provider_{std::move(builder_provider)},
      starting_fields_length_{starting_fields_length} {
  }

  /// @brief Adds a field to a record.
  /// @param name Field name.
  /// @return Object that allows the caller to add new values to a given field.
  auto push_field(std::string_view name) -> field_guard;

private:
  builder_provider builder_provider_;
  arrow_length_type starting_fields_length_ = 0u;
};

/// @brief A view of field created within adaptive_table_slice_builder.
/// Allows addition of new values into the data column represented by the field.
class field_guard {
public:
  field_guard(builder_provider builder_provider,
              arrow_length_type starting_fields_length)
    : builder_provider_{std::move(builder_provider)},
      starting_fields_length_{starting_fields_length} {
  }

  /// @brief Adds a value to a field.
  /// @param view View of a value to be added.
  template <class ViewType>
    requires requires(ViewType x) { materialize(x); }
  auto add(ViewType view) -> void {
    using view_value_type = decltype(materialize(std::declval<ViewType>()));
    using vast_type = type_from_data_t<view_value_type>;
    builder_provider_.provide().add<vast_type>(view);
  }

  /// @brief Adds the underlying view to the field if it is of a supported type.
  auto add(const data_view& view) -> void;

  /// @brief Turns the field into a record_type if it was of unknown type.
  /// @return Object that enables manipulation of the record. The field_guard
  /// must outlive the return value of this method.
  auto push_record() -> record_guard;
  /// @brief Turns the field into a list_type if it was of unknown type.
  /// @return Object that enables manipulation of the list. The field_guard must
  /// outlive the return value of this method.
  auto push_list() -> list_guard;

  /// @brief The fields can exist in two scenarios. 1) The value was added to it
  /// by the add or push_list/push_record with it's add. 2) The
  /// adaptive_table_slice_builder was constructed with a known schema that
  /// already contained the field.
  /// @return true - field exist. false - doesn't exist.
  auto field_exists() const -> bool;

private:
  builder_provider builder_provider_;
  arrow_length_type starting_fields_length_ = 0u;
};

} // namespace vast::detail
