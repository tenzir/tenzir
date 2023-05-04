//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/adaptive_table_slice_builder_guards.hpp"

#include <optional>

namespace vast::detail {

namespace {

auto add_data_view(auto& guard, const data_view& view) {
  caf::visit(detail::overload{
               [&guard](const auto& v) {
                 guard.add(make_view(v));
               },
               [](const caf::none_t&) {
                 // nop
               },
               [](const map_view_handle&) {
                 die("adding view<map> is not supported");
               },
               [](const list_view_handle&) {
                 die("adding view<list> is not supported");
               },
               [](const record_view_handle&) {
                 die("adding view<record> is not supported");
               },
               [](const pattern_view&) {
                 die("adding patterns is not supported");
               },
             },
             view);
}

auto try_create_field_builder_for_fixed_builder(
  builder_provider& record_builder_provider, std::string_view field_name,
  arrow_length_type starting_fields_length) -> std::optional<field_guard> {
  if (not record_builder_provider.is_builder_constructed())
    return {};
  auto fixed_builder = std::get_if<fixed_fields_record_builder>(
    &record_builder_provider.provide());
  if (not fixed_builder)
    return {};
  return field_guard{fixed_builder->get_field_builder_provider(field_name),
                     starting_fields_length};
}

} // namespace

auto record_guard::push_field(std::string_view name) -> field_guard {
  if (auto guard = try_create_field_builder_for_fixed_builder(
        builder_provider_, name, starting_fields_length_))
    return std::move(*guard);
  auto provider = [name, this]() -> series_builder& {
    auto& b = builder_provider_.provide();
    if (std::holds_alternative<unknown_type_builder>(b)) {
      b = concrete_series_builder<record_type>();
      b.add_up_to_n_nulls(starting_fields_length_);
    }
    auto& record_builder = std::get<concrete_series_builder<record_type>>(b);
    return record_builder
      .get_field_builder_provider(name, starting_fields_length_)
      .provide();
  };
  return {{std::move(provider)}, starting_fields_length_};
}

auto list_guard::list_record_guard::push_field(std::string_view name)
  -> field_guard {
  // The columnar growth of list type is handled by arrow::ListBuilder. From the
  // perspective of a record value of a list we start with a field of 0 length
  // which is later appended into the list builder.
  constexpr auto list_fields_start_length = arrow_length_type{0};
  if (auto guard = try_create_field_builder_for_fixed_builder(
        builder_provider_, name, list_fields_start_length))
    return std::move(*guard);
  if (builder_provider_.is_builder_constructed()) {
    auto& b = std::get<concrete_series_builder<record_type>>(
      builder_provider_.provide());
    return field_guard{b.get_field_builder_provider(name,
                                                    list_fields_start_length),
                       list_fields_start_length};
  }
  auto provider = [this, name]() -> series_builder& {
    auto& builder = std::get<concrete_series_builder<record_type>>(
      builder_provider_.provide());
    return builder.get_field_builder_provider(name, list_fields_start_length)
      .provide();
  };
  return field_guard{{std::move(provider)}, list_fields_start_length};
}

list_guard::list_record_guard::~list_record_guard() noexcept {
  if (builder_provider_.is_builder_constructed()) {
    if (not parent_.value_type)
      parent_.propagate_type(builder_provider_.type());
    std::visit(
      []<class Builder>(Builder& b) {
        if constexpr (std::is_base_of_v<record_series_builder_base, Builder>) {
          b.fill_nulls();
          b.append();
        }
      },
      builder_provider_.provide());
  }
}

auto list_guard::add(const data_view& view) -> void {
  add_data_view(*this, view);
}

auto list_guard::push_record() -> list_guard::list_record_guard {
  if (builder_provider_.is_builder_constructed()) {
    auto& builder = std::get<concrete_series_builder<list_type>>(
      builder_provider_.provide());
    return {{std::ref(builder.get_record_builder())}, *this};
  }
  auto provider = [this]() -> series_builder& {
    auto& b = builder_provider_.provide();
    VAST_ASSERT(std::holds_alternative<concrete_series_builder<list_type>>(b));
    auto& builder = std::get<concrete_series_builder<list_type>>(b);
    return builder.get_record_builder();
  };
  return {{std::move(provider)}, *this};
}

auto list_guard::propagate_type(type child_type) -> void {
  value_type = std::move(child_type);
  if (parent) {
    parent->propagate_type(vast::type{list_type{value_type}});
    const auto s = get_root_list_builder()
                     .get_child_builder<type_to_arrow_builder_t<list_type>>(
                       vast::type{list_type{value_type}})
                     .Append();
    VAST_ASSERT(s.ok());
  } else {
    auto& builder = get_root_list_builder();
    builder.create_builder(value_type);
    const auto s = builder.get_arrow_builder()->Append();
    VAST_ASSERT(s.ok());
  }
}

auto list_guard::get_root_list_builder()
  -> concrete_series_builder<vast::list_type>& {
  if (list_builder_)
    return *list_builder_;
  auto& series_builder = builder_provider_.provide();
  list_builder_
    = std::get_if<concrete_series_builder<list_type>>(&series_builder);
  VAST_ASSERT(list_builder_);
  return *list_builder_;
}

auto list_guard::push_list() -> list_guard {
  auto child_value_type = type{};
  if (value_type) {
    child_value_type = caf::get<list_type>(value_type).value_type();
    const auto s
      = get_root_list_builder()
          .get_child_builder<type_to_arrow_builder_t<list_type>>(value_type)
          .Append();
    VAST_ASSERT(s.ok());
  }
  return list_guard{builder_provider_, this, child_value_type};
}

auto field_guard::add(const data_view& view) -> void {
  add_data_view(*this, view);
}

auto field_guard::push_record() -> record_guard {
  return {builder_provider_, starting_fields_length_};
}

auto field_guard::push_list() -> list_guard {
  if (auto type = builder_provider_.type()) {
    auto value_type = caf::get<vast::list_type>(type).value_type();
    const auto s = std::get<concrete_series_builder<list_type>>(
                     builder_provider_.provide())
                     .get_arrow_builder()
                     ->Append();
    VAST_ASSERT(s.ok());
    return list_guard{std::move(builder_provider_), nullptr,
                      std::move(value_type)};
  }
  auto provider = [this]() mutable -> series_builder& {
    auto& builder = builder_provider_.provide();
    if (std::holds_alternative<unknown_type_builder>(builder)) {
      builder = concrete_series_builder<list_type>(starting_fields_length_);
    }
    return builder;
  };

  return list_guard{{std::move(provider)}, nullptr, type{}};
}

auto field_guard::field_exists() const -> bool {
  return builder_provider_.is_builder_constructed();
}

} // namespace vast::detail
