//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/series_builders.hpp"

namespace vast::detail {

auto builder_provider::provide() -> series_builder& {
  return std::visit(
    detail::overload{
      [this](const builder_provider_impl& provider) -> series_builder& {
        auto& ret = provider();
        data_ = std::ref(ret);
        return ret;
      },
      [](std::reference_wrapper<series_builder> ref) -> series_builder& {
        return ref.get();
      }},
    data_);
}

auto builder_provider::type() const -> vast::type {
  return std::visit(
    detail::overload{[](const builder_provider_impl&) {
                       return vast::type{};
                     },
                     [](std::reference_wrapper<series_builder> ref) {
                       return ref.get().type();
                     }},
    data_);
}

auto builder_provider::is_builder_constructed() const -> bool {
  return std::holds_alternative<std::reference_wrapper<series_builder>>(data_);
}

concrete_series_builder<record_type>::concrete_series_builder(
  const record_type& type) {
  for (auto view : type.fields()) {
    field_builders_[std::string{view.name}]
      = std::make_unique<series_builder>(std::move(view.type));
  }
}

auto concrete_series_builder<record_type>::get_field_builder_provider(
  std::string_view field, arrow_length_type starting_fields_length)
  -> builder_provider {
  if (auto it = field_builders_.find(std::string{field});
      it != field_builders_.end())
    return {std::ref(*(it->second))};
  return {[field, starting_fields_length,
           &builders = field_builders_]() -> series_builder& {
    auto& new_builder = *(
      builders.emplace(field, std::make_unique<series_builder>()).first->second);
    new_builder.add_up_to_n_nulls(starting_fields_length);
    return new_builder;
  }};
}

auto concrete_series_builder<record_type>::get_arrow_builder()
  -> std::shared_ptr<arrow::StructBuilder> {
  return record_series_builder_base::get_arrow_builder(type());
}

auto record_series_builder_base::fill_nulls() -> void {
  auto len = length();
  for (auto& [_, builder] : field_builders_)
    builder->add_up_to_n_nulls(len);
}

auto record_series_builder_base::add_up_to_n_nulls(
  arrow_length_type max_null_count) -> void {
  for (auto& [name, builder] : field_builders_) {
    builder->add_up_to_n_nulls(max_null_count);
  }
}

auto record_series_builder_base::get_arrow_builder(const type& t)
  -> std::shared_ptr<arrow::StructBuilder> {
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
  auto arrow_type = t.to_arrow_type();
  arrow_builder_ = std::make_shared<type_to_arrow_builder_t<record_type>>(
    std::move(arrow_type), arrow::default_memory_pool(),
    std::move(field_builders));
  return arrow_builder_;
}

auto record_series_builder_base::length() const -> arrow_length_type {
  auto len = arrow_length_type{0u};
  for (const auto& [_, builder] : field_builders_) {
    len = std::max(len, builder->length());
  }
  return len;
}

auto concrete_series_builder<record_type>::type() const -> vast::type {
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

auto record_series_builder_base::append() -> void {
  VAST_ASSERT(arrow_builder_);
  for (auto& [_, builder] : field_builders_) {
    std::visit(
      []<class Builder>(Builder& b) {
        if constexpr (std::is_base_of_v<record_series_builder_base, Builder>) {
          b.append();
        }
      },
      *builder);
  }
  const auto status = arrow_builder_->Append();
  VAST_ASSERT(status.ok());
}

auto record_series_builder_base::remove_last_row() -> void {
  for (const auto& [_, builder] : field_builders_) {
    builder->remove_last_row();
  }
}

concrete_series_builder<list_type>::concrete_series_builder(
  arrow_length_type nulls_to_prepend)
  : nulls_to_prepend_{nulls_to_prepend} {
}

concrete_series_builder<list_type>::concrete_series_builder(
  const list_type& type, bool are_fields_fixed)
  : are_fields_fixed_{are_fields_fixed} {
  create_builder(type.value_type());
}

std::shared_ptr<arrow::Array> concrete_series_builder<list_type>::finish() {
  if (not builder_)
    return nullptr;
  return builder_->Finish().ValueOrDie();
}

auto concrete_series_builder<list_type>::add_up_to_n_nulls(
  arrow_length_type max_null_count) -> void {
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

auto concrete_series_builder<list_type>::length() const -> arrow_length_type {
  if (not builder_)
    return 0u;
  return builder_->length();
}

auto concrete_series_builder<list_type>::create_builder(
  const vast::type& value_type) -> void {
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
auto concrete_series_builder<list_type>::get_arrow_builder()
  -> std::shared_ptr<type_to_arrow_builder_t<list_type>> {
  return builder_;
}

// Only one record builder exists in list of records as the deeper nestings
// are handled by the record builder itself.
auto concrete_series_builder<list_type>::get_record_builder()
  -> series_builder& {
  if (not record_builder_) [[unlikely]] {
    record_builder_ = std::make_unique<series_builder>(
      concrete_series_builder<record_type>{});
  }
  return *record_builder_;
}

auto concrete_series_builder<list_type>::remove_last_row() -> bool {
  if (not builder_) [[unlikely]]
    return true;

  auto array = builder_->Finish().ValueOrDie();
  auto new_array = array->Slice(0, array->length() - 1);
  const auto status
    = builder_->AppendArraySlice(*new_array->data(), 0u, new_array->length());
  VAST_ASSERT(status.ok());
  if (are_fields_fixed_)
    return false;
  return builder_->null_count() == builder_->length();
}

fixed_fields_record_builder::fixed_fields_record_builder(record_type type)
  : type_{std::move(type)} {
  constexpr auto fixed_fields = true;
  for (auto view : caf::get<record_type>(type_).fields()) {
    field_builders_[std::string{view.name}]
      = std::make_unique<series_builder>(std::move(view.type), fixed_fields);
  }
}

auto fixed_fields_record_builder::get_field_builder_provider(
  std::string_view field_name) -> builder_provider {
  if (auto it = field_builders_.find(std::string{field_name});
      it != field_builders_.end())
    return {std::ref(*(it->second))};
  return {[field_name]() -> series_builder& {
    die(
      fmt::format("trying to add a value to a non existent field: {}. The "
                  "parent adaptive_table_slice_builder was forbidden to infer "
                  "unknown fields. Construct it with allow_fields_discovery "
                  "set to true if you want the fields to be discovered",
                  field_name));
  }};
}

auto fixed_fields_record_builder::type() const -> const vast::type& {
  return type_;
}

auto fixed_fields_record_builder::get_arrow_builder()
  -> std::shared_ptr<arrow::StructBuilder> {
  return record_series_builder_base::get_arrow_builder(type_);
}

series_builder::series_builder(const vast::type& type, bool are_fields_fixed) {
  caf::visit(
    detail::overload{
      [this, &type]<class Type>(const Type&) {
        *this = concrete_series_builder<Type>{type};
      },
      [this, are_fields_fixed](const record_type& t) {
        if (are_fields_fixed) {
          *this = fixed_fields_record_builder{t};
        } else {
          *this = concrete_series_builder<record_type>{t};
        }
      },
      [this, are_fields_fixed](const list_type& t) {
        *this = concrete_series_builder<list_type>{t, are_fields_fixed};
      },
      [](const map_type&) {
        die("unsupported map_type in construction of series builder");
        // TODO: remove with map type removal.
      },
    },
    type);
}

arrow_length_type series_builder::length() const {
  return std::visit(
    [](const auto& actual) {
      return actual.length();
    },
    *this);
}

std::shared_ptr<arrow::ArrayBuilder> series_builder::get_arrow_builder() {
  return std::visit(
    [](auto& actual) -> std::shared_ptr<arrow::ArrayBuilder> {
      return actual.get_arrow_builder();
    },
    *this);
}

vast::type series_builder::type() const {
  return std::visit(
    [](const auto& actual) {
        return actual.type();
    },
    *this);
}

std::shared_ptr<arrow::Array> series_builder::finish() {
  return std::visit(detail::overload{
                      [](unknown_type_builder&) {
                        return std::shared_ptr<arrow::Array>{};
                      },
                      [](auto& builder) -> std::shared_ptr<arrow::Array> {
                        return builder.finish();
                      },
                    },
                    *this);
}

auto series_builder::add_up_to_n_nulls(arrow_length_type max_null_count)
  -> void {
  std::visit(
    [max_null_count](auto& actual) {
      actual.add_up_to_n_nulls(max_null_count);
    },
    *this);
}

auto series_builder::remove_last_row() -> void {
  std::visit(
    [this](auto& actual) {
      if constexpr (std::is_same_v<bool, decltype(actual.remove_last_row())>) {
        if (auto all_values_are_null = actual.remove_last_row();
            all_values_are_null) {
          *this = unknown_type_builder(actual.length());
        }
      } else {
        actual.remove_last_row();
      }
    },
    *this);
}

std::shared_ptr<arrow::Array> record_series_builder_base::finish() {
  auto arrays = arrow::ArrayVector{};
  auto field_names = std::vector<std::string>{};
  arrays.reserve(field_builders_.size());
  field_names.reserve(field_builders_.size());
  for (auto& [name, builder] : field_builders_) {
    if (auto arr = builder->finish()) {
      arrays.push_back(std::move(arr));
      field_names.push_back(name);
    }
  }
  if (arrays.empty())
    return nullptr;
  return arrow::StructArray::Make(arrays, field_names).ValueOrDie();
}

auto concrete_series_builder<list_type>::create_builder_impl(const vast::type& t)
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
        if (not record_builder_) {
          if (are_fields_fixed_) {
            record_builder_ = std::make_unique<series_builder>(
              fixed_fields_record_builder{type});
          } else {
            record_builder_ = std::make_unique<series_builder>(
              concrete_series_builder<record_type>{type});
          }
        }
        auto ret = record_builder_->get_arrow_builder();
        child_builders_[vast::type{type}] = ret.get();
        return ret;
      },
    },
    t);
}

} // namespace vast::detail
