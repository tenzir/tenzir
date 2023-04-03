//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/adaptive_table_slice_builder_guards.hpp"

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

} // namespace

auto record_guard::push_field(std::string_view name) -> field_guard {
  return field_guard{builder_.get_field_builder(name, starting_fields_length_)};
}

record_guard::~record_guard() noexcept {
  builder_.fill_nulls();
}

auto list_guard::list_record_guard::push_field(std::string_view name)
  -> field_guard {
  return field_guard{builder_.get_field_builder(name, starting_fields_length_)};
}

list_guard::list_record_guard::~list_record_guard() noexcept {
  builder_.fill_nulls();
  if (not parent_.value_type)
    parent_.propagate_type(builder_.type());
  builder_.append();
}

auto list_guard::add(const data_view& view) -> void {
  add_data_view(*this, view);
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

auto field_guard::add(const data_view& view) -> void {
  add_data_view(*this, view);
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

} // namespace vast::detail
