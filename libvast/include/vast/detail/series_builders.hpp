//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/stable_map.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"

#include <functional>

namespace vast::detail {

template <concrete_type T>
class concrete_series_builder;

template <class T>
struct make_concrete_series_builder {
  using type = concrete_series_builder<T>;
};

struct series_builder;

// Arrow uses int64_t for all lengths.
using arrow_length_type = int64_t;

class builder_provider {
public:
  using builder_provider_impl = std::function<series_builder&()>;

  explicit(false) builder_provider(
    std::variant<builder_provider_impl, std::reference_wrapper<series_builder>>
      data)
    : data_{std::move(data)} {
  }

  auto provide() -> series_builder&;
  auto type() -> type;
  auto is_builder_constructed() -> bool;

private:
  std::variant<builder_provider_impl, std::reference_wrapper<series_builder>>
    data_;
};

// Builder that represents a column of an unknown type.
class unknown_type_builder {
public:
  unknown_type_builder() = default;
  explicit unknown_type_builder(arrow_length_type null_count)
    : null_count_{null_count} {
  }

  arrow_length_type length() const {
    return null_count_;
  }

  auto type() const -> type {
    return {};
  }

  auto add_up_to_n_nulls(arrow_length_type max_null_count) -> void {
    null_count_ = std::max(null_count_, max_null_count);
  }

  auto get_arrow_builder() const -> std::shared_ptr<arrow::ArrayBuilder> {
    return nullptr;
  }

  auto remove_last_row() -> void {
    // remove_last_row can't be practically called when null_count is zero. No
    // need to guard against underflow
    --null_count_;
  }

private:
  arrow_length_type null_count_ = 0u;
};

template <concrete_type Type>
class concrete_series_builder {
public:
  concrete_series_builder()
    : type_{Type{}},
      builder_(Type{}.make_arrow_builder(arrow::default_memory_pool())) {
  }

  auto add(view<type_to_data_t<Type>> view) {
    const auto s = append_builder(caf::get<Type>(type_), *builder_, view);
    VAST_ASSERT(s.ok());
  }

  auto finish() && {
    return builder_->Finish().ValueOrDie();
  }

  auto get_arrow_builder() {
    return builder_;
  }

  auto type() const -> const vast::type& {
    return type_;
  }

  auto length() const -> arrow_length_type {
    return builder_->length();
  }

  auto add_up_to_n_nulls(arrow_length_type max_null_count) -> void {
    VAST_ASSERT(max_null_count >= length());
    const auto status = builder_->AppendNulls(max_null_count - length());
    VAST_ASSERT(status.ok());
  }

  auto remove_last_row() -> bool {
    auto array = builder_->Finish().ValueOrDie();
    auto new_array = array->Slice(0, array->length() - 1);
    const auto status
      = builder_->AppendArraySlice(*new_array->data(), 0u, new_array->length());
    VAST_ASSERT(status.ok());
    return builder_->null_count() == builder_->length();
  }

private:
  vast::type type_;
  std::shared_ptr<type_to_arrow_builder_t<Type>> builder_;
};

template <>
class concrete_series_builder<record_type> {
public:
  auto get_field_builder_provider(std::string_view field,
                                  arrow_length_type starting_fields_length)
    -> builder_provider;
  auto fill_nulls() -> void;
  auto add_up_to_n_nulls(arrow_length_type max_null_count) -> void;
  auto get_occupied_rows() -> arrow_length_type;
  auto finish() && -> std::shared_ptr<arrow::Array>;
  auto length() const -> arrow_length_type;
  auto type() const -> vast::type;
  auto get_arrow_builder() -> std::shared_ptr<arrow::StructBuilder>;
  auto append() -> void;
  auto remove_last_row() -> void;

private:
  auto is_type_known() -> bool;

  bool is_type_known_ = false;
  detail::stable_map<std::string, std::unique_ptr<series_builder>>
    field_builders_;
  std::shared_ptr<arrow::StructBuilder> arrow_builder_;
};

template <>
class concrete_series_builder<list_type> {
public:
  explicit concrete_series_builder(arrow_length_type nulls_to_prepend = 0u);

  auto finish() && -> std::shared_ptr<arrow::Array>;
  auto length() const -> arrow_length_type;
  auto type() const -> const vast::type&;
  auto add_up_to_n_nulls(arrow_length_type max_null_count) -> void;

  auto create_builder(const vast::type& value_type) -> void;

  template <class TypeToCastTo = arrow::ArrayBuilder>
  auto get_child_builder(const vast::type& t) -> TypeToCastTo& {
    VAST_ASSERT(child_builders_.contains(t));
    return static_cast<TypeToCastTo&>(*child_builders_[t]);
  }

  auto get_arrow_builder()
    -> std::shared_ptr<type_to_arrow_builder_t<list_type>>;

  // Only one record builder exists in list of records as the deeper nestings
  // are handled by the record builder itself.
  auto get_record_builder() -> series_builder&;
  auto remove_last_row() -> bool;

private:
  auto create_builder_impl(const vast::type& t)
    -> std::shared_ptr<arrow::ArrayBuilder>;

  std::shared_ptr<type_to_arrow_builder_t<list_type>> builder_;
  detail::stable_map<vast::type, arrow::ArrayBuilder*> child_builders_;
  std::unique_ptr<series_builder> record_builder_;
  arrow_length_type nulls_to_prepend_ = 0u;
  vast::type type_;
};

using series_builder_base = caf::detail::tl_apply_t<
  caf::detail::tl_push_front_t<
    caf::detail::tl_map_t<concrete_types, make_concrete_series_builder>,
    unknown_type_builder>,
  std::variant>;

struct series_builder : series_builder_base {
  using variant::variant;

  arrow_length_type length() const;

  std::shared_ptr<arrow::ArrayBuilder> get_arrow_builder();

  vast::type type() const;

  template <concrete_type Type>
  auto add(auto view) -> void {
    return std::visit(
      detail::overload{
        [view](concrete_series_builder<Type>& same_type_builder) {
          same_type_builder.add(view);
        },
        [this, view](unknown_type_builder& builder) {
          const auto nulls_to_prepend = builder.length();
          auto new_builder = concrete_series_builder<Type>{};
          new_builder.add_up_to_n_nulls(nulls_to_prepend);
          new_builder.add(view);
          *this = std::move(new_builder);
        },
        [](auto&) {
          die("cast not implemented");
        },
      },
      *this);
  }

  std::shared_ptr<arrow::Array> finish() &&;

  auto add_up_to_n_nulls(arrow_length_type max_null_count) -> void;
  auto remove_last_row() -> void;
};

} // namespace vast::detail
