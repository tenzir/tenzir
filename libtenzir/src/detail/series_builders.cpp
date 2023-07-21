//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/series_builders.hpp"

#include "caf/detail/scope_guard.hpp"

namespace tenzir::detail {

namespace {
template <class T>
constexpr auto is_caf_expected = false;

template <class T>
constexpr auto is_caf_expected<caf::expected<T>> = true;

template <class T>
concept allowed_common_type
  = concrete_type<T>
    and not tl_contains_v<type_list<map_type, list_type, record_type>, T>;

template <concrete_type T1, concrete_type T2>
consteval auto get_common_type_list() {
  // Remove T1 as casting from T1 to T1 makes no sense in this context. We'd
  // never try a common type cast in such case. Remove enumeration_type as it
  // has no field informations to be cast into.
  using common_types = caf::detail::tl_filter_not_type_t<
    caf::detail::tl_filter_not_type_t<
      detail::tl_common_types_t<typename supported_casts<T1>::types,
                                typename supported_casts<T2>::types>,
      T1>,
    enumeration_type>;
  return common_types{};
}

template <concrete_type InputType, class... CommonTypes>
auto get_common_types_impl(const InputType& input_type, const data& new_input,
                           type_list<CommonTypes...>)
  -> std::vector<std::pair<type, data>> {
  auto ret = std::vector<std::pair<type, data>>{};
  auto push_type = [&](const auto& current_common_type) {
    const auto& actual_data = caf::get<type_to_data_t<InputType>>(new_input);
    if (auto cast_val
        = cast_value(input_type, actual_data, current_common_type)) {
      ret.push_back({type{current_common_type}, std::move(*cast_val)});
    }
  };
  (push_type(CommonTypes{}), ...);
  return ret;
}

auto get_common_types(const data& new_input, const type& new_input_type,
                      const type& current_builder_type)
  -> std::vector<std::pair<type, data>> {
  return caf::visit(
    detail::overload{
      [](const auto&, const auto&) -> std::vector<std::pair<type, data>> {
        return {};
      },
      []<class T>(const T&, const T&) -> std::vector<std::pair<type, data>> {
        return {};
      },
      [&new_input]<allowed_common_type InputType, allowed_common_type CurrType>(
        const InputType& input_type,
        const CurrType&) -> std::vector<std::pair<type, data>> {
        return get_common_types_impl(
          input_type, new_input, get_common_type_list<CurrType, InputType>());
      },
    },
    new_input_type, current_builder_type);
}

auto append_no_null_values(
  const data& data, arrow::ArrayBuilder& builder, const type& type,
  concrete_series_builder<record_type>& cast_field_parent_record) -> bool {
  return caf::visit(
    detail::overload{
      []<class FieldType>(const FieldType& field_type,
                          arrow::ArrayBuilder& builder,
                          const tenzir::data& basic_data) -> bool {
        auto status
          = append_builder(field_type, builder, make_view(basic_data));
        TENZIR_ASSERT(status.ok());
        return true;
      },
      [&cast_field_parent_record](const list_type& type,
                                  arrow::ArrayBuilder& builder,
                                  const tenzir::data& list_data) -> bool {
        TENZIR_ASSERT(caf::holds_alternative<list>(list_data));
        TENZIR_ASSERT(
          caf::holds_alternative<type_to_arrow_builder_t<list_type>>(builder));
        auto& list_builder
          = static_cast<type_to_arrow_builder_t<list_type>&>(builder);
        auto status = list_builder.Append();
        TENZIR_ASSERT(status.ok());
        auto should_append_builder = true;
        for (const auto& v : caf::get<list>(list_data)) {
          if (v == caf::none) {
            continue;
          }
          auto should_append
            = append_no_null_values(v, *list_builder.value_builder(),
                                    type.value_type(),
                                    cast_field_parent_record);
          should_append_builder = should_append ? should_append_builder : false;
        }
        return should_append_builder;
      },
      [&cast_field_parent_record](const record_type& type,
                                  arrow::ArrayBuilder& builder,
                                  const tenzir::data& record_data) -> bool {
        TENZIR_ASSERT(caf::holds_alternative<record>(record_data));
        TENZIR_ASSERT(
          caf::holds_alternative<type_to_arrow_builder_t<record_type>>(
            builder));
        auto& record_builder
          = static_cast<type_to_arrow_builder_t<record_type>&>(builder);
        auto should_append_builder
          = std::addressof(record_builder)
            != cast_field_parent_record.get_arrow_builder().get();
        const auto& record = caf::get<tenzir::record>(record_data);
        for (auto field_no = 0u; const auto& field : type.fields()) {
          const auto& data = record.at(field.name);
          // Only field of a record that started common type casting should have
          // null here. We ignore them as we had to add them in order to finish
          // the struct array.
          if (data == caf::none) {
            ++field_no;
            continue;
          }
          auto should_append
            = append_no_null_values(data,
                                    *record_builder.field_builder(field_no),
                                    field.type, cast_field_parent_record);
          should_append_builder = should_append ? should_append_builder : false;
          ++field_no;
        }
        if (should_append_builder) {
          auto status = record_builder.Append();
          TENZIR_ASSERT(status.ok());
        }
        return should_append_builder;
      },
    },
    type, detail::passthrough(builder), detail::passthrough(data));
}

auto add_new_value(const data& data, const type& type_of_new_data,
                   series_builder& cast_builder) -> caf::error {
  return caf::visit(
    detail::overload{
      [&data,
       &cast_builder]<allowed_common_type CommonType>(const CommonType&) {
        auto new_val = caf::get_if<type_to_data_t<CommonType>>(&data);
        TENZIR_ASSERT(new_val);
        return cast_builder.add<CommonType>(*new_val);
      },
      [](const auto&) {
        return caf::make_error(ec::convert_error, "invalid common type chosen");
      },
    },
    type_of_new_data);
}

auto change_child_builder(std::vector<std::pair<type, data>> possible_new_types,
                          series_builder& builder_that_needs_type_change)
  -> bool {
  auto array = builder_that_needs_type_change.finish();
  for (auto& [type, data] : possible_new_types) {
    if (auto err = builder_that_needs_type_change.change_type(type, *array)) {
      continue;
    }
    auto err = add_new_value(data, type, builder_that_needs_type_change);
    return not err;
  }
  return false;
}

type get_list_type_with_new_non_list_value_type(const type& list_type,
                                                const type& new_value_type) {
  return caf::visit(detail::overload{
                      [&new_value_type](const tenzir::list_type& type) {
                        return tenzir::type{tenzir::list_type{
                          get_list_type_with_new_non_list_value_type(
                            type.value_type(), new_value_type)}};
                      },
                      [&new_value_type](const auto&) {
                        return new_value_type;
                      },
                    },
                    list_type);
}

type get_first_non_list_type(const type& type) {
  return caf::visit(detail::overload{
                      [](const list_type& type) {
                        return get_first_non_list_type(type.value_type());
                      },
                      [](const auto& t) {
                        return tenzir::type{t};
                      },
                    },
                    type);
}

const record* get_last_record(const data& data) {
  return caf::visit(detail::overload{
                      [](const list& list) {
                        const record* last_record = nullptr;
                        for (auto it = list.rbegin(); it != list.rend(); ++it) {
                          if (auto last = get_last_record(*it)) {
                            return last;
                          }
                        }
                        return last_record;
                      },
                      [&](const record& rec) {
                        return std::addressof(rec);
                      },
                      [](const auto&) -> const record* {
                        return nullptr;
                      },
                    },
                    data);
}

// Copies all the data to the new list builder but the last record.
// The last record must be copied without null values as they were appended just
// so that the child struct array could have been finished.
auto add_last_cast_list_data(
  const record* last_record,
  concrete_series_builder<record_type>& cast_field_parent_record,
  const data& last_data, arrow::ArrayBuilder& root_builder) -> void {
  caf::visit(
    detail::overload{
      [last_record, &cast_field_parent_record](const list& list,
                                               arrow::ArrayBuilder& builder) {
        TENZIR_ASSERT(
          caf::holds_alternative<type_to_arrow_builder_t<list_type>>(builder));
        auto& list_builder
          = static_cast<type_to_arrow_builder_t<list_type>&>(builder);
        auto status = list_builder.Append();
        TENZIR_ASSERT(status.ok());
        for (const auto& v : list) {
          add_last_cast_list_data(last_record, cast_field_parent_record, v,
                                  *list_builder.value_builder());
        }
      },
      [last_record, &cast_field_parent_record](const record& rec,
                                               arrow::ArrayBuilder& builder) {
        TENZIR_ASSERT(
          caf::holds_alternative<type_to_arrow_builder_t<record_type>>(
            builder));
        auto& record_builder
          = static_cast<type_to_arrow_builder_t<record_type>&>(builder);
        auto type = type::from_arrow(*builder.type());
        if (last_record == std::addressof(rec)) {
          append_no_null_values(rec, builder, type, cast_field_parent_record);
          return;
        }
        auto status = append_builder(caf::get<record_type>(type),
                                     record_builder, make_view(rec));
        TENZIR_ASSERT(status.ok());
      },
      [](auto&&...) {
        // nop
      },
    },
    last_data, detail::passthrough(root_builder));
}

} // namespace

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

auto builder_provider::type() const -> tenzir::type {
  return std::visit(
    detail::overload{[](const builder_provider_impl&) {
                       return tenzir::type{};
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
      = std::make_unique<series_builder>(std::move(view.type), this);
  }
}

auto concrete_series_builder<record_type>::get_field_builder_provider(
  std::string_view field, arrow_length_type starting_fields_length)
  -> builder_provider {
  if (auto it = field_builders_.find(std::string{field});
      it != field_builders_.end())
    return {std::ref(*(it->second))};
  return {[field, starting_fields_length, &builders = field_builders_,
           this]() -> series_builder& {
    auto& new_builder
      = *(builders
            .emplace(field, std::make_unique<series_builder>(
                              this, std::in_place_type<unknown_type_builder>))
            .first->second);
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

auto concrete_series_builder<record_type>::type() const -> tenzir::type {
  if (field_builders_.empty())
    return tenzir::type{};
  std::vector<record_type::field_view> fields;
  fields.reserve(field_builders_.size());
  for (const auto& [name, builder] : field_builders_) {
    if (auto type = builder->type())
      fields.emplace_back(name, std::move(type));
  }
  if (fields.empty())
    return {};
  return tenzir::type{record_type{std::move(fields)}};
}

auto concrete_series_builder<record_type>::is_part_of_a_list() const -> bool {
  return std::visit(detail::overload{
                      [](std::monostate) {
                        return false;
                      },
                      [](concrete_series_builder<record_type>* obs) {
                        return obs ? obs->is_part_of_a_list() : false;
                      },
                      [](concrete_series_builder<list_type>* obs) {
                        return static_cast<bool>(obs);
                      },
                    },
                    type_change_observer_);
}

auto concrete_series_builder<record_type>::on_field_type_change(
  series_builder& builder_that_needs_type_change,
  const tenzir::type& type_of_new_input, const data& new_input) -> caf::error {
  auto possible_new_field_types = get_common_types(
    new_input, type_of_new_input, builder_that_needs_type_change.type());
  if (possible_new_field_types.empty()) {
    return caf::make_error(
      ec::convert_error,
      "cannot add value {} : unable to find a common type between {} and {}",
      new_input, builder_that_needs_type_change.type(), type_of_new_input);
  }
  if (not is_part_of_a_list()) {
    if (change_child_builder(possible_new_field_types,
                             builder_that_needs_type_change)) {
      return {};
    }
    return caf::make_error(
      ec::convert_error,
      "cannot add value {} : unable to find a common type between {} and {}",
      new_input, builder_that_needs_type_change.type(), type_of_new_input);
  }
  changing_builder_ = std::addressof(builder_that_needs_type_change);
  auto changing_builder_clearer = caf::detail::make_scope_guard([this] {
    changing_builder_ = nullptr;
  });
  auto cast_infos = get_cast_infos(possible_new_field_types);
  // At least one of the field must have value added so that we can finish the
  // struct array. We add a null here so that we don't need to check if any
  // value was added to this record before a common type cast was started.
  auto status = changing_builder_->get_arrow_builder()->AppendNull();
  TENZIR_ASSERT(status.ok());
  if (auto err = propagate_type_change_to_observer(cast_infos))
    return err;
  auto new_common_type = builder_that_needs_type_change.type();
  auto common_type_cast_info
    = std::find_if(cast_infos.begin(), cast_infos.end(),
                   [&new_common_type](const auto& cast_info) {
                     return cast_info.type_of_value_to_add_after_cast
                            == new_common_type;
                   });
  TENZIR_ASSERT(common_type_cast_info != cast_infos.end());
  return add_new_value(common_type_cast_info->value_to_add_after_cast,
                       new_common_type, builder_that_needs_type_change);
}

auto concrete_series_builder<record_type>::on_child_record_type_change(
  concrete_series_builder<record_type>& child,
  std::vector<common_type_cast_info> child_cast_infos) -> caf::error {
  auto child_builder_it = std::find_if(
    field_builders_.begin(), field_builders_.end(),
    [&child](const auto& field_name_and_builder) {
      auto& builder = field_name_and_builder.second;
      auto maybe_rec_builder
        = std::get_if<concrete_series_builder<record_type>>(builder.get());
      return maybe_rec_builder == std::addressof(child);
    });
  if (child_builder_it == field_builders_.end()) {
    return caf::make_error(ec::convert_error,
                           "unable to change a type of a field in the list "
                           "of nested records: child builder not found");
  }
  auto cast_infos_for_current_record
    = get_cast_infos(std::move(child_cast_infos), *child_builder_it->second);
  return propagate_type_change_to_observer(
    std::move(cast_infos_for_current_record));
}

auto concrete_series_builder<record_type>::get_cast_infos(
  std::vector<std::pair<tenzir::type, data>> possible_new_field_types)
  -> std::vector<common_type_cast_info> {
  auto ret = std::vector<common_type_cast_info>{};
  for (auto& [type, val] : possible_new_field_types) {
    std::vector<record_type::field_view> fields;
    fields.reserve(field_builders_.size());
    for (const auto& [name, builder] : field_builders_) {
      if (builder.get() == changing_builder_)
        fields.emplace_back(name, type);
      else if (auto type = builder->type())
        fields.emplace_back(name, std::move(type));
    }
    ret.push_back(
      {tenzir::type{record_type{std::move(fields)}}, val, type, this});
  }
  return ret;
}

auto concrete_series_builder<record_type>::propagate_type_change_to_observer(
  const std::vector<common_type_cast_info>& cast_infos) -> caf::error {
  return std::visit(
    detail::overload{
      [](std::monostate) {
        return caf::make_error(ec::convert_error,
                               "unexpected monostate when trying to cast to a "
                               "common type");
      },
      [this, &cast_infos](concrete_series_builder<record_type>* obs) {
        TENZIR_ASSERT(obs);
        return obs->on_child_record_type_change(*this, cast_infos);
      },
      [this, &cast_infos](concrete_series_builder<list_type>* obs) {
        TENZIR_ASSERT(obs);
        fill_nulls();
        append();
        return obs->on_record_type_change(cast_infos);
      },
    },
    type_change_observer_);
}

auto concrete_series_builder<record_type>::get_cast_infos(
  std::vector<common_type_cast_info> child_cast_infos,
  const series_builder& child_builder) -> std::vector<common_type_cast_info> {
  std::vector<common_type_cast_info> ret;
  ret.reserve(child_cast_infos.size());
  for (auto& info : child_cast_infos) {
    std::vector<record_type::field_view> fields;
    fields.reserve(field_builders_.size());
    for (const auto& [field_name, builder] : field_builders_) {
      if (builder.get() == std::addressof(child_builder)) {
        fields.emplace_back(field_name, std::move(info.new_type_candidate));
      } else {
        fields.emplace_back(field_name, builder->type());
      }
    }
    ret.push_back({tenzir::type{record_type{std::move(fields)}},
                   std::move(info.value_to_add_after_cast),
                   std::move(info.type_of_value_to_add_after_cast),
                   info.cast_field_parent_record});
  }
  return ret;
}

auto concrete_series_builder<record_type>::on_child_list_change(
  concrete_series_builder<list_type>* child,
  std::vector<common_type_cast_info> child_cast_infos) -> caf::error {
  auto child_builder_it = std::find_if(
    field_builders_.begin(), field_builders_.end(),
    [child](const auto& field_name_and_builder) {
      auto& builder = field_name_and_builder.second;
      auto maybe_list_builder
        = std::get_if<concrete_series_builder<list_type>>(builder.get());
      return maybe_list_builder == child;
    });
  TENZIR_ASSERT(child_builder_it != field_builders_.end());
  auto cast_infos_for_current_record
    = get_cast_infos(std::move(child_cast_infos), *child_builder_it->second);
  return propagate_type_change_to_observer(
    std::move(cast_infos_for_current_record));
}

auto concrete_series_builder<record_type>::reset(
  tenzir::type chosen_type_of_changing_field,
  const tenzir::type& new_builder_type) -> void {
  TENZIR_ASSERT(caf::holds_alternative<record_type>(new_builder_type));
  auto& new_record_type = caf::get<record_type>(new_builder_type);
  for (auto field_index = 0u; auto& [field_name, builder] : field_builders_) {
    if (auto record_builder
        = std::get_if<concrete_series_builder<record_type>>(builder.get())) {
      record_builder->reset(chosen_type_of_changing_field,
                            new_record_type.field(field_index).type);
    } else if (auto list_builder
               = std::get_if<concrete_series_builder<list_type>>(
                 builder.get())) {
      list_builder->reset(chosen_type_of_changing_field,
                          new_record_type.field(field_index).type);
    }
    ++field_index;
  }
  if (changing_builder_) {
    changing_builder_->emplace_with_type(chosen_type_of_changing_field, this);
  }
  if (arrow_builder_)
    arrow_builder_.reset();
}

auto concrete_series_builder<record_type>::set_type_change_observer(
  type_change_observer obs) -> void {
  type_change_observer_ = std::move(obs);
}

auto record_series_builder_base::append() -> void {
  TENZIR_ASSERT(arrow_builder_);
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
  TENZIR_ASSERT(status.ok());
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
    TENZIR_ASSERT(max_null_count >= length());
    const auto status = builder_->AppendNulls(max_null_count - length());
    TENZIR_ASSERT(status.ok());
    return;
  }
  nulls_to_prepend_ = std::max(max_null_count, nulls_to_prepend_);
}

auto concrete_series_builder<list_type>::type() const -> const tenzir::type& {
  return type_;
}

auto concrete_series_builder<list_type>::length() const -> arrow_length_type {
  return builder_ ? builder_->length() : 0u;
}

auto concrete_series_builder<list_type>::create_builder(
  const tenzir::type& value_type) -> void {
  TENZIR_ASSERT(value_type);
  type_ = tenzir::type{list_type{value_type}};
  builder_
    = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(),
                                           create_builder_impl(value_type),
                                           type_.to_arrow_type());
  const auto s = builder_->AppendNulls(nulls_to_prepend_);
  TENZIR_ASSERT(s.ok());
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
      nullptr, std::in_place_type<concrete_series_builder<record_type>>);
    std::get<concrete_series_builder<record_type>>(*record_builder_)
      .set_type_change_observer(this);
  }
  return *record_builder_;
}

auto concrete_series_builder<list_type>::remove_last_row() -> bool {
  if (not builder_) [[unlikely]]
    return true;

  auto array = builder_->Finish().ValueOrDie();
  auto new_array = array->Slice(0, array->length() - 1);
  for (auto view : values(type_, *new_array)) {
    auto status = append_builder(
      type_, static_cast<arrow::ArrayBuilder&>(*builder_), view);
    TENZIR_ASSERT(status.ok());
  }
  if (are_fields_fixed_)
    return false;
  return builder_->null_count() == builder_->length();
}

fixed_fields_record_builder::fixed_fields_record_builder(record_type type)
  : type_{std::move(type)} {
  constexpr auto fixed_fields = true;
  for (auto view : caf::get<record_type>(type_).fields()) {
    field_builders_[std::string{view.name}] = std::make_unique<series_builder>(
      std::move(view.type), nullptr, fixed_fields);
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

auto fixed_fields_record_builder::type() const -> const tenzir::type& {
  return type_;
}

auto fixed_fields_record_builder::get_arrow_builder()
  -> std::shared_ptr<arrow::StructBuilder> {
  return record_series_builder_base::get_arrow_builder(type_);
}

series_builder::series_builder(
  const tenzir::type& type, concrete_series_builder<record_type>* parent_record,
  bool are_fields_fixed) {
  emplace_with_type(type, parent_record, are_fields_fixed);
}

void series_builder::emplace_with_type(
  const tenzir::type& type, concrete_series_builder<record_type>* parent_record,
  bool are_fields_fixed) {
  caf::visit(
    detail::overload{
      [this, &type, parent_record]<class Type>(const Type&) {
        emplace_with_builder<concrete_series_builder<Type>>(parent_record,
                                                            type);
      },
      [this, are_fields_fixed, parent_record](const record_type& t) {
        if (are_fields_fixed) {
          emplace_with_builder<fixed_fields_record_builder>(parent_record, t);
        } else {
          emplace_with_builder<concrete_series_builder<record_type>>(
            parent_record, t)
            .set_type_change_observer(parent_record);
        }
      },
      [this, are_fields_fixed, parent_record](const list_type& t) {
        emplace_with_builder<concrete_series_builder<list_type>>(
          parent_record, t, are_fields_fixed)
          .set_record_type_change_observer(parent_record);
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

tenzir::type series_builder::type() const {
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
            all_values_are_null and field_type_change_handler_) {
          emplace_with_builder<unknown_type_builder>(field_type_change_handler_,
                                                     actual.length());
        }
      } else {
        actual.remove_last_row();
      }
    },
    *this);
}

auto series_builder::change_type(const tenzir::type& new_type,
                                 const arrow::Array& array) -> caf::error {
  return caf::visit(
    detail::overload{
      [](const auto& current_type, const auto& concrete_new_type) {
        return caf::make_error(ec::convert_error,
                               fmt::format("unsupported common type: {} for a "
                                           "field of type: {}",
                                           concrete_new_type, current_type));
      },
      [this, &array,
       &new_type]<allowed_common_type CurrentType, allowed_common_type NewType>(
        const CurrentType& current_type, const NewType& concrete_new_type) {
        auto cast_builder = cast_to_builder(
          current_type,
          static_cast<const type_to_arrow_array_t<CurrentType>&>(array),
          concrete_new_type);
        if (not cast_builder)
          return std::move(cast_builder.error());
        emplace_with_builder<concrete_series_builder<NewType>>(
          field_type_change_handler_, new_type, std::move(*cast_builder));
        return caf::error{};
      },
    },
    type(), new_type);
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

auto concrete_series_builder<list_type>::on_record_type_change(
  std::vector<common_type_cast_info> cast_infos) -> caf::error {
  if (record_type_change_observer_
      and record_type_change_observer_->is_part_of_a_list()) {
    auto new_cast_infos = std::vector<common_type_cast_info>{};
    for (auto& info : cast_infos) {
      new_cast_infos.push_back({get_list_type_with_new_non_list_value_type(
                                  type_, std::move(info.new_type_candidate)),
                                std::move(info.value_to_add_after_cast),
                                std::move(info.type_of_value_to_add_after_cast),
                                info.cast_field_parent_record});
    }
    return record_type_change_observer_->on_child_list_change(
      this, std::move(new_cast_infos));
  }
  auto arr = finish();
  auto new_list_builder
    = std::shared_ptr<type_to_arrow_builder_t<list_type>>{nullptr};
  auto new_list_builder_type = tenzir::type{};
  const common_type_cast_info* successful_cast_info = nullptr;
  for (const auto& cast_info : cast_infos) {
    auto new_type = get_list_type_with_new_non_list_value_type(
      type_, cast_info.new_type_candidate);
    auto cast = cast_to_builder(
      caf::get<list_type>(type_),
      static_cast<const type_to_arrow_array_t<list_type>&>(*arr),
      caf::get<list_type>(new_type));
    if (cast) {
      new_list_builder = std::move(*cast);
      successful_cast_info = std::addressof(cast_info);
      new_list_builder_type = std::move(new_type);
      break;
    }
  }
  if (not new_list_builder) {
    return caf::make_error(ec::convert_error, "unable to find appropriate list "
                                              "type for common type cast");
  }
  const auto& [new_record_type, new_data, new_data_type,
               cast_field_parent_record]
    = *successful_cast_info;
  reset(new_data_type, new_list_builder_type);
  // The idea here is to copy every single data from the cast_builder to the new
  // builder, but the last record. The last record is inflated with a manually
  // appened nulls that should be skipped. They were appended just so that the
  // finished list array didn't have invalid length.
  auto new_list_arr = new_list_builder->Finish().ValueOrDie();
  std::vector<data_view> current_builder_data;
  for (auto v : values(new_list_builder_type, *new_list_arr)) {
    current_builder_data.push_back(v);
  }
  auto last_data = materialize(current_builder_data.back());
  current_builder_data.pop_back();
  auto& new_list_type = caf::get<list_type>(new_list_builder_type);
  for (auto& d : current_builder_data) {
    if (caf::holds_alternative<view<caf::none_t>>(d)) {
      const auto status = builder_->AppendNull();
      TENZIR_ASSERT(status.ok());
      continue;
    }
    TENZIR_ASSERT(caf::holds_alternative<view<list>>(d));
    const auto status
      = append_builder(new_list_type, *builder_, caf::get<view<list>>(d));
    TENZIR_ASSERT(status.ok());
  }
  auto last_record = get_last_record(last_data);
  add_last_cast_list_data(last_record, *cast_field_parent_record, last_data,
                          *builder_);
  return {};
}

auto concrete_series_builder<list_type>::set_record_type_change_observer(
  concrete_series_builder<record_type>* obs) -> void {
  record_type_change_observer_ = obs;
}

auto concrete_series_builder<list_type>::reset(
  tenzir::type chosen_type_of_changing_field,
  const tenzir::type& new_builder_type) -> void {
  if (record_builder_) {
    auto rec = std::get_if<concrete_series_builder<record_type>>(
      record_builder_.get());
    TENZIR_ASSERT(rec);
    rec->reset(chosen_type_of_changing_field,
               get_first_non_list_type(new_builder_type));
  }
  child_builders_.clear();
  create_builder(caf::get<list_type>(new_builder_type).value_type());
}

auto concrete_series_builder<list_type>::create_builder_impl(
  const tenzir::type& t) -> std::shared_ptr<arrow::ArrayBuilder> {
  return caf::visit(
    detail::overload{
      [this](const list_type& type) {
        auto value_builder = create_builder_impl(type.value_type());
        std::shared_ptr<arrow::ArrayBuilder> list_builder
          = std::make_shared<type_to_arrow_builder_t<list_type>>(
            arrow::default_memory_pool(), std::move(value_builder));
        child_builders_[tenzir::type{type}] = list_builder.get();
        return list_builder;
      },
      [this](const auto& basic) {
        std::shared_ptr<arrow::ArrayBuilder> value_builder
          = basic.make_arrow_builder(arrow::default_memory_pool());
        child_builders_[tenzir::type{basic}] = value_builder.get();
        return value_builder;
      },
      [this](const record_type& type) {
        if (not record_builder_) {
          if (are_fields_fixed_) {
            record_builder_ = std::make_unique<series_builder>(
              nullptr, std::in_place_type<fixed_fields_record_builder>, type);
          } else {
            record_builder_ = std::make_unique<series_builder>(
              nullptr, std::in_place_type<concrete_series_builder<record_type>>,
              type);
            std::get<concrete_series_builder<record_type>>(*record_builder_)
              .set_type_change_observer(this);
          }
        }
        auto ret = record_builder_->get_arrow_builder();
        child_builders_[tenzir::type{type}] = ret.get();
        return ret;
      },
    },
    t);
}

auto concrete_series_builder<list_type>::change_type(
  tenzir::type list_value_type, tenzir::type new_value_type, data value_to_add)
  -> caf::expected<tenzir::type> {
  if (are_fields_fixed_) {
    return caf::make_error(ec::parse_error,
                           fmt::format("unable to add {} to the list: casting "
                                       "required but "
                                       "type inference is disabled",
                                       value_to_add));
  }
  auto common_types_candidates
    = get_common_types(value_to_add, new_value_type, list_value_type);
  if (record_type_change_observer_
      and record_type_change_observer_->is_part_of_a_list()) {
    return change_type_with_nested_list_parent(common_types_candidates);
  }
  auto arr = finish();
  for (auto [common_type, new_val_to_add] : common_types_candidates) {
    auto new_type
      = get_list_type_with_new_non_list_value_type(type_, common_type);
    auto cast = cast_to_builder(
      caf::get<list_type>(type_),
      static_cast<const type_to_arrow_array_t<list_type>&>(*arr),
      caf::get<list_type>(new_type));
    if (not cast) {
      continue;
    }
    builder_ = std::move(*cast);
    // The builder_ has a new type. The type_ and child_builders_ must be
    // adjusted to the new builder_.
    type_ = new_type;
    child_builders_.clear();
    auto update_builders = detail::overload{
      [this](auto&& recursive_update, const list_type& type,
             arrow::ArrayBuilder& builder) {
        TENZIR_ASSERT(
          caf::holds_alternative<type_to_arrow_builder_t<list_type>>(builder));
        child_builders_[tenzir::type{type}] = std::addressof(builder);
        recursive_update(
          recursive_update, type.value_type(),
          *(static_cast<type_to_arrow_builder_t<list_type>&>(builder)
              .value_builder()));
      },
      [this](auto&&, const auto& actual_type, arrow::ArrayBuilder& builder) {
        child_builders_[tenzir::type{actual_type}] = std::addressof(builder);
      },
    };
    auto update_builders_visit
      = [&update_builders](auto&& update, const tenzir::type& t,
                           arrow::ArrayBuilder& b) {
          caf::visit(update_builders, detail::passthrough(update), t,
                     detail::passthrough(b));
        };
    update_builders_visit(update_builders_visit,
                          caf::get<list_type>(type_).value_type(),
                          *builder_->value_builder());
    auto status = append_builder(common_type, *child_builders_[common_type],
                                 make_view(new_val_to_add));
    TENZIR_ASSERT(status.ok());
    return common_type;
  }
  return caf::make_error(ec::parse_error,
                         fmt::format("unable to add {} to the list: common "
                                     "type not found",
                                     value_to_add));
}

auto concrete_series_builder<list_type>::change_type_with_nested_list_parent(
  const std::vector<std::pair<tenzir::type, data>>& common_type_candidates)
  -> caf::expected<tenzir::type> {
  auto cast_infos = std::vector<common_type_cast_info>{};
  cast_infos.reserve(common_type_candidates.size());
  for (auto& [new_type, data] : common_type_candidates) {
    cast_infos.push_back(
      {get_list_type_with_new_non_list_value_type(type_, new_type), data,
       new_type, record_type_change_observer_});
  }
  auto err = record_type_change_observer_->on_child_list_change(
    this, std::move(cast_infos));
  if (err) {
    return err;
  }
  auto new_value_type = get_first_non_list_type(type_);
  for (const auto& [type, data] : common_type_candidates) {
    if (new_value_type == type) {
      auto status = append_builder(
        new_value_type, *child_builders_[new_value_type], make_view(data));
      TENZIR_ASSERT(status.ok());
      return new_value_type;
    }
  }
  return caf::make_error(ec::parse_error, fmt::format("nested list common "
                                                      "type casting failed"));
}

} // namespace tenzir::detail
