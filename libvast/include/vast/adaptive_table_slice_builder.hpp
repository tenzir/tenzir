//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/stable_map.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <arrow/record_batch.h>

#include <memory>

namespace vast::internal {

template <concrete_type T>
class concrete_series_builder;

template <class T>
struct make_concrete_series_builder {
  using type = concrete_series_builder<T>;
};

struct series_builder;

// Arrow uses int64_t for all lengths.
using length_type = int64_t;

// Used when a field is created but type is not yet known
class unknown_type_builder {
public:
  length_type length() const {
    return null_count_;
  }

  auto type() const -> type {
    return {};
  }

  auto add_up_to_n_nulls(length_type max_null_count) -> void {
    null_count_ = std::max(null_count_, max_null_count);
  }

  auto get_arrow_builder() const -> std::shared_ptr<arrow::ArrayBuilder> {
    return nullptr;
  }

private:
  length_type null_count_ = 0u;
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

  auto length() const -> length_type {
    return builder_->length();
  }

  auto add_up_to_n_nulls(length_type max_null_count) -> void {
    VAST_ASSERT(max_null_count >= length());
    const auto status = builder_->AppendNulls(max_null_count - length());
    VAST_ASSERT(status.ok());
  }

private:
  vast::type type_;
  std::shared_ptr<type_to_arrow_builder_t<Type>> builder_;
};

struct list_guard {
private:
  class list_record_guard {
  public:
    list_record_guard(concrete_series_builder<record_type>& builder,
                      list_guard& parent, length_type starting_fields_length);

    auto push_field(std::string_view name);

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

  template <class ViewType>
    requires requires(ViewType x) { materialize(x); }
  auto add(ViewType view) -> void;

  auto push_record() -> list_record_guard;

  auto push_list() -> list_guard;

private:
  void propagate_type(type child_type);

  concrete_series_builder<list_type>& builder_;
  list_guard* parent = nullptr;
  type value_type;
};

class record_guard {
public:
  record_guard(concrete_series_builder<record_type>& builder,
               length_type starting_fields_length)
    : builder_{builder}, starting_fields_length_{starting_fields_length} {
  }

  auto push_field(std::string_view name);
  ~record_guard() noexcept;

private:
  concrete_series_builder<record_type>& builder_;
  length_type starting_fields_length_ = 0u;
};

class field_guard {
public:
  explicit field_guard(series_builder& builder) : builder_{builder} {
  }

  template <class ViewType>
    requires requires(ViewType x) { materialize(x); }
  auto add(ViewType view) -> void;

  auto push_record() -> record_guard;
  auto push_list() -> list_guard;

private:
  series_builder& builder_;
};

template <>
class concrete_series_builder<record_type> {
public:
  explicit concrete_series_builder(length_type nulls_to_prepend = 0u)
    : nulls_to_prepend_{nulls_to_prepend} {
  }

  auto push_field(std::string_view field, length_type starting_fields_length)
    -> field_guard;
  auto fill_nulls() -> void;
  auto add_up_to_n_nulls(length_type max_null_count) -> void;

  auto get_occupied_rows() -> length_type {
    // No type can happen when someone pushes an empty record. We reserve the
    // rows for the NULLs in case this record will have fields added in the next
    // rows.
    return is_type_known() ? length() : nulls_to_prepend_;
  }

  auto finish() && -> std::shared_ptr<arrow::Array>;
  auto length() const -> length_type;
  auto type() const -> vast::type;
  auto get_arrow_builder() -> std::shared_ptr<arrow::StructBuilder>;
  auto append() -> void;

private:
  auto is_type_known() -> bool;

  bool is_type_known_ = false;
  length_type nulls_to_prepend_ = 0u;
  detail::stable_map<std::string, std::unique_ptr<series_builder>>
    field_builders_;
  std::shared_ptr<arrow::StructBuilder> arrow_builder_;
};

template <>
class concrete_series_builder<list_type> {
public:
  explicit concrete_series_builder(length_type nulls_to_prepend = 0u)
    : nulls_to_prepend_{nulls_to_prepend} {
  }

  auto finish() && -> std::shared_ptr<arrow::Array>;
  auto length() const -> length_type;
  auto type() const -> const vast::type&;
  auto add_up_to_n_nulls(length_type max_null_count) -> void;

  auto create_builder(const vast::type& value_type) -> void {
    VAST_ASSERT(value_type);
    type_ = vast::type{list_type{value_type}};
    builder_
      = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                             create_builder_impl(value_type),
                                             type_.to_arrow_type());

    const auto s = builder_->AppendNulls(nulls_to_prepend_);
    VAST_ASSERT(s.ok());
    nulls_to_prepend_ = 0u;
  }

  template <class TypeToCastTo = arrow::ArrayBuilder>
  auto get_child_builder(const vast::type& t) -> TypeToCastTo& {
    VAST_ASSERT(child_builders_.contains(t));
    return static_cast<TypeToCastTo&>(*child_builders_[t]);
  }

  auto get_arrow_builder()
    -> std::shared_ptr<type_to_arrow_builder_t<list_type>> {
    return builder_;
  }

  // Only one record builder exists in list of records as the deeper nestings
  // are handled by the record builder itself.
  auto get_record_builder() -> concrete_series_builder<record_type>& {
    if (not record_builder_) [[unlikely]]
      return record_builder_.emplace();
    return *record_builder_;
  }

private:
  auto create_builder_impl(const vast::type& t)
    -> std::shared_ptr<arrow::ArrayBuilder> {
    return caf::visit(
      detail::overload{
        [this](const list_type& type) {
          auto value_builder = create_builder_impl(type.value_type());
          std::shared_ptr<arrow::ArrayBuilder> list_builder
            = std::make_shared<type_to_arrow_builder_t<list_type>>(
              arrow::default_memory_pool(), std::move(value_builder));
          child_builders_[vast::type{type}] = list_builder.get();
          return list_builder;
        },
        [this](const auto& basic) {
          std::shared_ptr<arrow::ArrayBuilder> value_builder
            = basic.make_arrow_builder(arrow::default_memory_pool());
          child_builders_[vast::type{basic}] = value_builder.get();
          return value_builder;
        },
        [this](const record_type& type) {
          VAST_ASSERT(record_builder_);
          auto ret = record_builder_->get_arrow_builder();
          child_builders_[vast::type{type}] = ret.get();
          return ret;
        },
      },
      t);
  }

  std::shared_ptr<type_to_arrow_builder_t<list_type>> builder_;
  detail::stable_map<vast::type, arrow::ArrayBuilder*> child_builders_;
  std::optional<concrete_series_builder<record_type>> record_builder_;
  length_type nulls_to_prepend_ = 0u;
  vast::type type_;
};

using series_builder_base = caf::detail::tl_apply_t<
  caf::detail::tl_push_front_t<
    caf::detail::tl_map_t<concrete_types, make_concrete_series_builder>,
    unknown_type_builder>,
  std::variant>;

struct series_builder : series_builder_base {
  using variant::variant;

  length_type length() const {
    return std::visit(
      [](const auto& actual) {
        return actual.length();
      },
      *this);
  }

  std::shared_ptr<arrow::ArrayBuilder> get_arrow_builder() {
    return std::visit(
      [](auto& actual) -> std::shared_ptr<arrow::ArrayBuilder> {
        return actual.get_arrow_builder();
      },
      *this);
  }

  vast::type type() const {
    return std::visit(
      [](const auto& actual) {
        return actual.type();
      },
      *this);
  }

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

  std::shared_ptr<arrow::Array> finish() && {
    return std::visit(detail::overload{
                        [](unknown_type_builder&) {
                          return std::shared_ptr<arrow::Array>{};
                        },
                        [](auto& builder) -> std::shared_ptr<arrow::Array> {
                          return std::move(builder).finish();
                        },
                      },
                      *this);
  }

  auto add_up_to_n_nulls(length_type max_null_count) -> void {
    std::visit(
      [max_null_count](auto& actual) {
        actual.add_up_to_n_nulls(max_null_count);
      },
      *this);
  }
};

auto record_guard::push_field(std::string_view name) {
  return builder_.push_field(name, starting_fields_length_);
}

record_guard::~record_guard() noexcept {
  builder_.fill_nulls();
}

list_guard::list_record_guard::list_record_guard(
  concrete_series_builder<record_type>& builder, list_guard& parent,
  length_type starting_fields_length)
  : builder_{builder},
    parent_{parent},
    starting_fields_length_{starting_fields_length} {
}

auto list_guard::list_record_guard::push_field(std::string_view name) {
  return builder_.push_field(name, starting_fields_length_);
}

list_guard::list_record_guard::~list_record_guard() noexcept {
  builder_.fill_nulls();
  if (not parent_.value_type)
    parent_.propagate_type(builder_.type());
  builder_.append();
}

auto list_guard::push_record() -> list_guard::list_record_guard {
  auto& record_builder = builder_.get_record_builder();
  return {record_builder, *this, record_builder.length()};
}

void list_guard::propagate_type(type child_type) {
  value_type = std::move(child_type);
  if (parent) {
    parent->propagate_type(vast::type{list_type{value_type}});
    const auto s = builder_
                     .get_child_builder<type_to_arrow_builder_t<list_type>>(
                       vast::type{list_type{value_type}})
                     .Append();
    VAST_ASSERT(s.ok());
  } else {
    builder_.create_builder(value_type);
    const auto s = builder_.get_arrow_builder()->Append();
    VAST_ASSERT(s.ok());
  }
}

auto list_guard::push_list() -> list_guard {
  auto child_value_type = type{};
  if (value_type) {
    child_value_type = caf::get<list_type>(value_type).value_type();
    const auto s
      = builder_
          .get_child_builder<type_to_arrow_builder_t<list_type>>(value_type)
          .Append();
    VAST_ASSERT(s.ok());
  }
  return list_guard{builder_, this, child_value_type};
}

template <class ViewType>
  requires requires(ViewType x) { materialize(x); }
auto list_guard::add(ViewType view) -> void {
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
    builder_.get_child_builder<type_to_arrow_builder_t<vast_type>>(value_type),
    view);
  VAST_ASSERT(s.ok());
}

template <class ViewType>
  requires requires(ViewType x) { materialize(x); }
auto field_guard::add(ViewType view) -> void {
  using view_value_type = decltype(materialize(std::declval<ViewType>()));
  using vast_type = typename view_trait<view_value_type>::vast_type;
  // materialize(const char*) results in bool_type. This is why we must pass
  // string_view in such cases.
  static_assert(std::is_same_v<ViewType, vast::view<view_value_type>>,
                "Trying to add a type that can be converted to a vast::view. "
                "Pass vast::view explicitly instead");
  builder_.add<vast_type>(view);
}

auto field_guard::push_record() -> record_guard {
  if (std::holds_alternative<unknown_type_builder>(builder_)) {
    auto nulls_to_prepend = builder_.length();
    builder_ = concrete_series_builder<record_type>{nulls_to_prepend};
  }
  auto& record_builder
    = std::get<concrete_series_builder<record_type>>(builder_);
  return record_guard{record_builder, record_builder.get_occupied_rows()};
}

auto field_guard::push_list() -> list_guard {
  if (std::holds_alternative<unknown_type_builder>(builder_)) {
    auto nulls_to_prepend = builder_.length();
    builder_ = concrete_series_builder<list_type>{nulls_to_prepend};
  }
  auto& list_builder = std::get<concrete_series_builder<list_type>>(builder_);
  auto list_value_type = type{};
  if (auto list_type = list_builder.type()) {
    list_value_type = caf::get<vast::list_type>(list_type).value_type();
    const auto s = list_builder.get_arrow_builder()->Append();
    VAST_ASSERT(s.ok());
  }
  return list_guard{list_builder, nullptr, list_value_type};
}

auto concrete_series_builder<record_type>::push_field(
  std::string_view field, length_type starting_fields_length) -> field_guard {
  if (auto it = field_builders_.find(std::string{field});
      it != field_builders_.end())
    return field_guard{*(it->second)};
  auto& new_builder
    = *(field_builders_.emplace(field, std::make_unique<series_builder>())
          .first->second);
  new_builder.add_up_to_n_nulls(starting_fields_length);
  return field_guard{new_builder};
}

auto concrete_series_builder<record_type>::fill_nulls() -> void {
  auto len = length();
  for (auto& [_, builder] : field_builders_)
    builder->add_up_to_n_nulls(len);
}

std::shared_ptr<arrow::StructBuilder>
concrete_series_builder<record_type>::get_arrow_builder() {
  if (arrow_builder_)
    return arrow_builder_;
  auto field_builders = std::vector<std::shared_ptr<arrow::ArrayBuilder>>{};
  field_builders.reserve(field_builders_.size());
  for (const auto& [_, builder] : field_builders_) {
    if (auto arrow_builder = builder->get_arrow_builder())
      field_builders.push_back(std::move(arrow_builder));
  }
  if (field_builders.empty())
    return nullptr;
  auto arrow_type = type().to_arrow_type();
  arrow_builder_ = std::make_shared<type_to_arrow_builder_t<record_type>>(
    std::move(arrow_type), arrow::default_memory_pool(),
    std::move(field_builders));
  return arrow_builder_;
}

length_type concrete_series_builder<record_type>::length() const {
  auto len = length_type{0u};
  for (const auto& [_, builder] : field_builders_) {
    len = std::max(len, builder->length());
  }
  return len;
}

vast::type concrete_series_builder<record_type>::type() const {
  if (field_builders_.empty())
    return vast::type{};
  std::vector<record_type::field_view> fields;
  fields.reserve(field_builders_.size());
  for (const auto& [name, builder] : field_builders_) {
    if (auto type = builder->type())
      fields.emplace_back(name, std::move(type));
  }
  if (fields.empty())
    return {};
  return vast::type{record_type{std::move(fields)}};
}

auto concrete_series_builder<record_type>::append() -> void {
  VAST_ASSERT(arrow_builder_);
  for (const auto& [_, builder] : field_builders_) {
    if (caf::holds_alternative<vast::record_type>(builder->type()))
      std::get<concrete_series_builder<record_type>>(*builder).append();
  }
  const auto status = arrow_builder_->Append();
  VAST_ASSERT(status.ok());
}

auto concrete_series_builder<record_type>::is_type_known() -> bool {
  if (is_type_known_) [[likely]]
    return true;
  is_type_known_ = std::any_of(field_builders_.begin(), field_builders_.end(),
                               [](const auto& field_and_builder) {
                                 return field_and_builder.second->type();
                               });
  return is_type_known_;
}

std::shared_ptr<arrow::Array> concrete_series_builder<list_type>::finish() && {
  if (not builder_)
    return nullptr;
  return builder_->Finish().ValueOrDie();
}

auto concrete_series_builder<list_type>::add_up_to_n_nulls(
  length_type max_null_count) -> void {
  if (builder_) {
    VAST_ASSERT(max_null_count >= length());
    const auto status = builder_->AppendNulls(max_null_count - length());
    VAST_ASSERT(status.ok());
    return;
  }
  nulls_to_prepend_ = std::max(max_null_count, nulls_to_prepend_);
}

auto concrete_series_builder<list_type>::type() const -> const vast::type& {
  return type_;
}

length_type concrete_series_builder<list_type>::length() const {
  if (not builder_)
    return 0u;
  return builder_->length();
}

auto concrete_series_builder<record_type>::add_up_to_n_nulls(
  length_type max_null_count) -> void {
  if (not is_type_known()) {
    nulls_to_prepend_ = std::max(max_null_count, nulls_to_prepend_);
    return;
  }
  for (auto& [name, builder] : field_builders_) {
    std::visit(detail::overload{
                 [max_null_count](auto& b) {
                   b.add_up_to_n_nulls(max_null_count);
                 },
                 [](concrete_series_builder<record_type>&) {
                   // nop. Nested records handle nulls before parents. No need
                   // to repeadetly call it again for every nesting level.
                 },
               },
               *builder);
  }
}

std::shared_ptr<arrow::Array>
concrete_series_builder<record_type>::finish() && {
  auto arrays = arrow::ArrayVector{};
  auto field_names = std::vector<std::string>{};
  arrays.reserve(field_builders_.size());
  field_names.reserve(field_builders_.size());
  for (auto& [name, builder] : field_builders_) {
    if (auto arr = std::move(*builder).finish()) {
      arrays.push_back(std::move(arr));
      field_names.push_back(name);
    }
  }
  if (arrays.empty())
    return nullptr;
  return arrow::StructArray::Make(arrays, field_names).ValueOrDie();
}

} // namespace vast::internal

namespace vast {

class adaptive_table_slice_builder {
public:
  /// @brief Inserts a row to the output table slice.
  /// @return An object used to manipulate fields of an inserted row. The
  /// returned object must be destroyed beforore calling this method again.
  auto push_row() -> internal::record_guard {
    return {root_builder_, root_builder_.length()};
  }

  /// @brief Returns the resulting table slice.
  /// @return Finalized table slice.
  auto finish() && -> table_slice {
    auto final_array = std::move(root_builder_).finish();
    if (not final_array)
      return table_slice{};
    auto schema = root_builder_.type();
    auto schema_name = schema.make_fingerprint();
    auto slice_schema = vast::type{std::move(schema_name), std::move(schema)};
    const auto& struct_array
      = static_cast<const arrow::StructArray&>(*final_array);
    auto batch
      = arrow::RecordBatch::Make(slice_schema.to_arrow_schema(),
                                 struct_array.length(), struct_array.fields());
    VAST_ASSERT(batch);
    return table_slice{batch, std::move(slice_schema)};
  }

private:
  internal::concrete_series_builder<record_type> root_builder_;
};
} // namespace vast
