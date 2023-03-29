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

class field_guard;

/// @brief A view of a list column created within adaptive_table_slice_builder.
/// Allows addition of new values into the list.
class list_guard {
private:
  class list_record_guard {
  public:
    list_record_guard(concrete_series_builder<record_type>& builder,
                      list_guard& parent, length_type starting_fields_length)
      : builder_{builder},
        parent_{parent},
        starting_fields_length_{starting_fields_length} {
    }

    /// @brief Adds a field to a record nested inside a list.
    /// @param name Field name.
    /// @return Object used to append new values to a given field.
    auto push_field(std::string_view name) -> field_guard;

    ~list_record_guard() noexcept;

  private:
    concrete_series_builder<record_type>& builder_;
    list_guard& parent_;
    length_type starting_fields_length_ = 0u;
  };

public:
  list_guard(concrete_series_builder<list_type>& root_list_builder,
             list_guard* parent_list_guard, type value_type)

    : builder_{root_list_builder},
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
    using vast_type = typename view_trait<view_value_type>::vast_type;
    // materialize(const char*) results in bool_type. This is why we must pass
    // string_view in such cases.
    static_assert(std::is_same_v<ViewType, vast::view<view_value_type>>,
                  "Trying to add a type that can be converted to a vast::view. "
                  "Pass vast::view explicitly instead");
    if (not value_type)
      propagate_type(vast::type{vast_type{}});
    // Casting not supported yet.
    VAST_ASSERT(caf::holds_alternative<vast_type>(value_type));
    const auto s = append_builder(
      vast_type{},
      builder_.get_child_builder<type_to_arrow_builder_t<vast_type>>(
        value_type),
      view);
    VAST_ASSERT(s.ok());
  }

  /// @brief Adds a new record as a value of the list.
  /// @return Object that enables manipulation of the created record.
  auto push_record() -> list_record_guard;

  /// @brief Adds a nested list to the current list
  /// @return Object that enables manipulation of the created list.
  auto push_list() -> list_guard;

private:
  void propagate_type(type child_type);

  concrete_series_builder<list_type>& builder_;
  list_guard* parent = nullptr;
  type value_type;
};

/// @brief A view of a record column created within adaptive_table_slice_builder.
/// Allows addition of new values into the individual fields of a record.
class record_guard {
public:
  record_guard(concrete_series_builder<record_type>& builder,
               length_type starting_fields_length)
    : builder_{builder}, starting_fields_length_{starting_fields_length} {
  }

  /// @brief Adds a field to a record.
  /// @param name Field name.
  /// @return Object that allows the caller to add new values to a given field.
  auto push_field(std::string_view name) -> field_guard;
  ~record_guard() noexcept;

private:
  concrete_series_builder<record_type>& builder_;
  length_type starting_fields_length_ = 0u;
};

/// @brief A view of field created within adaptive_table_slice_builder.
/// Allows addition of new values into the data column represented by the field.
class field_guard {
public:
  explicit field_guard(series_builder& builder) : builder_{builder} {
  }

  /// @brief Adds a value to a field.
  /// @param view View of a value to be added.
  template <class ViewType>
    requires requires(ViewType x) { materialize(x); }
  auto add(ViewType view) -> void {
    using view_value_type = decltype(materialize(std::declval<ViewType>()));
    using vast_type = typename view_trait<view_value_type>::vast_type;
    // materialize(const char*) results in bool_type. This is why we must pass
    // string_view in such cases.
    static_assert(std::is_same_v<ViewType, vast::view<view_value_type>>,
                  "Trying to add a type that can be converted to a vast::view. "
                  "Pass vast::view explicitly instead");
    builder_.add<vast_type>(view);
  }

  /// @brief Turns the field into a record_type if it was of unknown type.
  /// @return Object that enables manipulation of the record.
  auto push_record() -> record_guard;
  /// @brief Turns the field into a list_type if it was of unknown type.
  /// @return Object that enables manipulation of the list.
  auto push_list() -> list_guard;

private:
  series_builder& builder_;
};

} // namespace vast::detail
