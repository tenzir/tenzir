//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/record_builder.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/concept/parseable/string/string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/type.hpp"

#include <arrow/compute/expression.h>
#include <caf/detail/type_list.hpp>
#include <caf/error.hpp>
#include <caf/none.hpp>
#include <caf/sum_type.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace tenzir {

auto record_builder::record() -> detail::record_builder::node_record* {
  root_.mark_this_alive();
  return &root_;
}

auto record_builder::reseed(std::optional<tenzir::type> seed) -> void {
  if (seed) {
    root_.reseed(caf::get<record_type>(*seed));
  }
}

auto record_builder::find_field_raw(std::string_view key)
  -> detail::record_builder::node_field* {
  return root_.at(key);
}

auto record_builder::clear() -> void {
  root_.clear();
}

auto record_builder::free() -> void {
  root_.data_.clear();
  root_.data_.shrink_to_fit();
  root_.lookup_.clear();
  root_.lookup_.shrink_to_fit();
}

auto record_builder::commit_to(series_builder& builder,
                               bool mark_dead) -> void {
  root_.commit_to(builder.record(), mark_dead);
}

auto record_builder::materialize(bool mark_dead) -> tenzir::record {
  tenzir::record res;
  root_.commit_to(res, mark_dead);

  return res;
}

namespace {
template <typename T>
struct type_to_parser;

template <>
struct type_to_parser<null_type> : std::type_identity<decltype(parsers::null)> {
};
template <>
struct type_to_parser<bool_type>
  : std::type_identity<decltype(parsers::boolean)> {};
template <>
struct type_to_parser<int64_type>
  : std::type_identity<decltype(parsers::integer)> {};
template <>
struct type_to_parser<uint64_type>
  : std::type_identity<decltype(parsers::count)> {};
template <>
struct type_to_parser<double_type>
  : std::type_identity<decltype(parsers::real)> {};
template <>
struct type_to_parser<duration_type>
  : std::type_identity<decltype(parsers::duration)> {};
template <>
struct type_to_parser<time_type> : std::type_identity<decltype(parsers::time)> {
};
template <>
struct type_to_parser<string_type> : std::type_identity<parsers::str> {};
template <>
struct type_to_parser<ip_type> : std::type_identity<decltype(parsers::ip)> {};
template <>
struct type_to_parser<subnet_type>
  : std::type_identity<decltype(parsers::net)> {};

template <typename T>
concept has_parser = caf::detail::is_complete<type_to_parser<T>>;
static_assert(has_parser<time_type>);

auto parse_enumeration(std::string_view s, const enumeration_type& e)
  -> detail::record_builder::data_parsing_result {
  s = detail::trim(s);
  if (auto opt = e.resolve(s)) {
    return tenzir::data{*opt};
  }
  uint32_t v;
  const auto [ptr, errc] = std::from_chars(s.begin(), s.end(), v);
  if (errc == std::errc{}) {
    if (not e.field(v).empty()) {
      return tenzir::data{static_cast<enumeration>(v)};
    }
  }
  return diagnostic::warning("failed to parse enumeration value")
    .note("value was \"{}\"", s)
    .done();
}

template <typename T>
auto try_parse_as(std::string_view s)
  -> detail::record_builder::data_parsing_result {
  T res;
  auto parser = make_parser<T>{};
  if (parser(s, res)) {
    return {res};
  }
  return {};
}

template <typename... T>
auto sequential_parsing(std::string_view s)
  -> detail::record_builder::data_parsing_result {
  detail::record_builder::data_parsing_result res;
  //FIXME check that fold is guaranteed to short circuit
  ((res = try_parse_as<T>(s), res.data.has_value()) || ...);

  return res;
}
} // namespace

auto record_builder::basic_seeded_parser(std::string_view s,
                                         const tenzir::type& seed)
  -> detail::record_builder::data_parsing_result {
  const auto visitor = detail::overload{
    [&s]<has_parser T>(
      const T& t) -> detail::record_builder::data_parsing_result {
      type_to_data_t<T> res;
      using parser = typename type_to_parser<T>::type;
      if (parser{}(s, res)) {
        return tenzir::data{std::move(res)};
      } else {
        return diagnostic::warning("failed to parse value as requested type")
          .hint("value was `{}`; type was `{}`", t, typeid(T).name())
          .done();
      }
    },
    [](const string_type&) -> detail::record_builder::data_parsing_result {
      return {};
    },
    [](const record_type&) -> tenzir::diagnostic {
      TENZIR_ERROR("`basic_parser` does not support structural "
                   "types. It cannot parsed something as a record");
      return diagnostic::error("`record` seed for basic parser is unsupported")
        .done();
    },
    [](const list_type&) -> tenzir::diagnostic {
      TENZIR_ERROR("`basic_parser` basic parser does not support "
                   "structural types. It cannot parse something as a list");
      return diagnostic::error("`list` seed for basic parser is unsupported")
        .done();
    },
    [&s](const enumeration_type& e) {
      return parse_enumeration(s, e);
    },
    []<typename T>(const T&) -> tenzir::diagnostic {
      TENZIR_ERROR("`basic parser` does not "
                   "support type `{}`",
                   typeid(T).name());
      return diagnostic::error("`unsupported type in record")
        .hint("type was `{}`", typeid(T).name())
        .done();
    },
  }; // namespace tenzir
  return caf::visit(visitor, seed);
}

auto record_builder::basic_parser(std::string_view s, const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result {
  if (seed) {
    return basic_seeded_parser(s, *seed);
  }
  // tenzir::data result;
  // if ((parsers::data - parsers::pattern)(s, result)) {
  //   return result;
  // } else {
  //   return {};
  // }

  return sequential_parsing<int64_t, uint64_t, double, duration, time, ip,
                            subnet, enumeration>(s);
}

auto record_builder::non_number_parser(std::string_view s,
                                       const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result {
  if (seed) {
    return record_builder::basic_seeded_parser(s, *seed);
  }
  // tenzir::data result;
  // constexpr static auto p
  //   = (parsers::data - parsers::number - parsers::pattern);
  //   // FIXME this can def be faster.
  // if (p(s, result)) {
  //   return result;
  // } else {
  //   return {};
  // }

  return sequential_parsing<duration, time, ip,
                            subnet, enumeration>(s);
}

namespace detail::record_builder {
namespace {

template <typename Field, typename Tenzir_Type>
struct index_regression_tester {
  constexpr static auto index_in_field
    = caf::detail::tl_index_of<field_type_list, Field>::value;
  constexpr static auto tenzir_type_index = Tenzir_Type::type_index;

  static_assert(index_in_field == tenzir_type_index);

  using type = void;
};

[[maybe_unused]] consteval void test() {
  (void)index_regression_tester<caf::none_t, null_type>{};
  (void)index_regression_tester<int64_t, int64_type>{};
  (void)index_regression_tester<uint64_t, uint64_type>{};
  (void)index_regression_tester<double, double_type>{};
  (void)index_regression_tester<duration, duration_type>{};
  (void)index_regression_tester<time, time_type>{};
  (void)index_regression_tester<std::string, string_type>{};
  (void)index_regression_tester<ip, ip_type>{};
  (void)index_regression_tester<subnet, subnet_type>{};
  (void)index_regression_tester<enumeration, enumeration_type>{};
  (void)index_regression_tester<node_list, list_type>{};
  (void)index_regression_tester<map_dummy, map_type>{};
  (void)index_regression_tester<node_record, record_type>{};
  (void)index_regression_tester<blob, blob_type>{};
}
} // namespace

auto node_base::is_dead() const -> bool {
  return state_ == state::dead;
}
auto node_base::is_alive() const -> bool {
  return state_ == state::alive;
}
auto node_base::affects_signature() const -> bool {
  switch (state_) {
    case state::alive:
    case state::sentinel:
      return true;
    case state::dead:
      return false;
  }
  TENZIR_UNREACHABLE();
}

auto node_record::try_field(std::string_view name) -> node_field* {
  auto [it, inserted] = lookup_.try_emplace(name, data_.size());
  if (not inserted) {
    return &data_[it->second].value;
  }
  TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on record size reached.");
  return &data_.emplace_back(it->first).value;
}

auto node_record::reserve(size_t N) -> void {
  lookup_.reserve(N);
  data_.reserve(N);
}

auto node_record::field(std::string_view name) -> node_field* {
  mark_this_alive();
  auto* f = try_field(name);
  f->mark_this_alive();
  return f;
}

auto node_record::at(std::string_view key) -> node_field* {
  for (const auto& [field_name, index] : lookup_) {
    const auto [field_name_mismatch, key_mismatch]
      = std::ranges::mismatch(field_name, key);
    if (field_name_mismatch == field_name.end()) {
      if (key_mismatch == key.end()) {
        return &data_[index].value;
      }
      if (*key_mismatch == '.') {
        continue;
      }
      if (auto* r = data_[index].value.get_if<node_record>()) {
        if (auto* result = r->at(key.substr(1 + key_mismatch - key.begin()))) {
          return result;
        } else {
          continue;
        }
      }
    }
  }
  return nullptr;
}

auto node_record::commit_to(record_ref r, bool mark_dead) -> void {
  if (mark_dead) {
    mark_this_dead();
  }
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    v.commit_to(r.field(k), mark_dead);
  }
}

auto node_record::commit_to(tenzir::record& r, bool mark_dead) -> void {
  if (mark_dead) {
    mark_this_dead();
  }
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    const auto [it, success] = r.try_emplace(k);
    v.commit_to(it->second, mark_dead);
  }
}

auto node_record::clear() -> void {
  node_base::mark_this_dead();
  for (auto& [k, v] : data_) {
    v.clear();
  }
}

auto node_record::reseed(const tenzir::record_type& type) -> void {
  if (is_dead()) {
    state_ = state::sentinel;
  }
  for (const auto& [k, v] : type.fields()) {
    try_field(k)->reseed(v);
  }
}

auto node_field::null() -> void {
  mark_this_alive();
  is_unparsed_ = false;
  data_ = caf::none;
}

auto node_field::data(tenzir::data d) -> void {
  mark_this_alive();
  is_unparsed_ = false;
  const auto visitor = detail::overload{
    [this](non_structured_data_type auto& x) {
      data(std::move(x));
    },
    [this](tenzir::list& x) {
      auto* l = list();
      for (auto& e : x) {
        l->data(std::move(e));
      }
    },
    [this](tenzir::record& x) {
      auto* r = record();
      for (auto& [k, v] : x) {
        r->field(k)->data(std::move(v));
      }
    },
    []<unsupported_types T>(T&) {
      TENZIR_ASSERT(false, fmt::format("Unexpected type \"{}\" in "
                                       "`record_builder::data`",
                                       typeid(T).name()));
    },
  };

  return caf::visit(visitor, d);
}

auto node_field::data_unparsed(std::string_view text) -> void {
  mark_this_alive();
  is_unparsed_ = true;
  data_.emplace<std::string>(text);
}

auto node_field::record() -> node_record* {
  mark_this_alive();
  if (auto* p = get_if<node_record>()) {
    return p;
  }
  return &data_.emplace<node_record>();
}

auto node_field::list() -> node_list* {
  mark_this_alive();
  if (auto* p = get_if<node_list>()) {
    return p;
  }
  return &data_.emplace<node_list>();
}

auto node_field::reseed(const tenzir::type& seed) -> void {
  if (is_dead()) {
    state_ = state::sentinel;
  }
  const auto visitor = detail::overload{
    [&]<non_structured_type_type T>(const T&) {
      data(T::construct());
    },
    [&](const enumeration_type&) {
      data(enumeration{});
    },
    [&](const record_type&) {
      auto r = record();
      r->reseed(caf::get<record_type>(seed));
    },
    [&](const list_type&) {
      auto l = list();
      l->reseed(caf::get<list_type>(seed));
    },
    [](const tenzir::null_type&) {

    },
    []<unsupported_types T>(const T&) {
      TENZIR_UNREACHABLE();
    },
  };
  caf::visit(visitor, seed);
}

auto node_field::commit_to(builder_ref r, bool mark_dead) -> void {
  if (mark_dead) {
    mark_this_dead();
  }
  const auto visitor = detail::overload{
    [&r, mark_dead](node_list& v) {
      if (v.is_alive()) {
        v.commit_to(r.list(), mark_dead);
      }
    },
    [&r, mark_dead](node_record& v) {
      if (v.is_alive()) {
        v.commit_to(r.record(), mark_dead);
      }
    },
    [&r]<non_structured_data_type T>(T& v) {
      r.try_data(v);
      // r.data(v);
    },
    [](auto&) {
      TENZIR_UNREACHABLE();
    },
  };
  std::visit(visitor, data_);
}

auto node_field::commit_to(tenzir::data& r, bool mark_dead) -> void {
  if (mark_dead) {
    mark_this_dead();
  }
  const auto visitor = detail::overload{
    [&r, mark_dead](node_list& v) {
      if (v.is_alive()) {
        r = tenzir::list{};
        v.commit_to(caf::get<tenzir::list>(r), mark_dead);
      }
    },
    [&r, mark_dead](node_record& v) {
      if (v.is_alive()) {
        r = tenzir::record{};
        v.commit_to(caf::get<tenzir::record>(r), mark_dead);
      }
    },
    [&r, mark_dead]<non_structured_data_type T>(T& v) {
      if (mark_dead) {
        r = std::move(v);
      } else {
        r = v;
      }
    },
    [](auto&) {
      TENZIR_UNREACHABLE();
    },
  };
  std::visit(visitor, data_);
}

auto node_field::clear() -> void {
  node_base::mark_this_dead();
  const auto visitor = detail::overload{
    [](node_list& v) {
      v.clear();
    },
    [](node_record& v) {
      v.clear();
    },
    [](auto&) { /* no-op */ },
  };
  std::visit(visitor, data_);
}

auto node_list::find_free() -> node_field* {
  for (auto& value : data_) {
    if (not value.is_alive()) {
      return &value;
    }
  }
  return nullptr;
}

auto node_list::back() -> node_field& {
  for (size_t i = 0; i < data_.size(); ++i) {
    if (not data_[i].is_alive()) {
      return data_[i - 1];
    }
  }
  TENZIR_UNREACHABLE();
  return data_.back();
}

auto node_list::reserve(size_t N) -> void {
  data_.reserve(N);
}

auto node_list::data(tenzir::data d) -> void {
  mark_this_alive();
  const auto visitor = detail::overload{
    [this](non_structured_data_type auto& x) {
      data(std::move(x));
    },
    [this](tenzir::list& x) {
      auto* l = list();
      for (auto& e : x) {
        l->data(std::move(e));
      }
    },
    [this](tenzir::record& x) {
      auto* r = record();
      for (auto& [k, v] : x) {
        r->field(k)->data(std::move(v));
      }
    },
    [](auto&) {
      TENZIR_UNREACHABLE();
    },
  };

  return caf::visit(visitor, d);
}

auto node_list::data_unparsed(std::string_view text) -> void {
  mark_this_alive();
  type_index_ = type_index_generic_mismatch;
  if (auto* free = find_free()) {
    free->data_unparsed(text);
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on list size reached.");
    data_.emplace_back().data_unparsed(text);
  }
}

auto node_list::null() -> void {
  return data(caf::none);
}

void node_list::update_new_structural_signature() {
  if (current_structural_signature_.empty()) {
    current_structural_signature_ = std::move(new_structural_signature_);
  } else if (new_structural_signature_ != current_structural_signature_) {
    type_index_ = type_index_generic_mismatch;
  }
}

auto node_list::record() -> node_record* {
  mark_this_alive();
  update_type_index(type_index_, type_index_record);
  if (type_index_ != type_index_empty
      and type_index_ != type_index_generic_mismatch) {
    update_new_structural_signature();
  }
  if (auto* free = find_free()) {
    if (auto* r = free->get_if<node_record>()) {
      return r;
    } else {
      return &free->data_.emplace<node_record>();
    }
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000,
                  "Upper limit on record size reached.");
    return data_.emplace_back().record();
  }
}

auto node_list::list() -> node_list* {
  mark_this_alive();
  update_type_index(type_index_, type_index_list);
  if (auto* free = find_free()) {
    if (auto* r = free->get_if<node_list>()) {
      return r;
    } else {
      return &free->data_.emplace<node_list>();
    }
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on list size reached.");
    return data_.emplace_back().list();
  }
}

auto node_list::reseed(const tenzir::list_type& seed) -> void {
  const auto& value_type = seed.value_type();
  const auto visitor = detail::overload{
    [&]<basic_type T>(const T&) {
      update_type_index(
        type_index_, caf::detail::tl_index_of<field_type_list,
                                              tenzir::type_to_data<T>>::value);
    },
    [&](const enumeration_type&) {
      update_type_index(
        type_index_,
        caf::detail::tl_index_of<field_type_list, tenzir::enumeration>::value);
    },
    [&](const tenzir::record_type&) {
      update_type_index(
        type_index_,
        caf::detail::tl_index_of<field_type_list, node_record>::value);
    },
    [&](const tenzir::list_type&) {
      update_type_index(
        type_index_,
        caf::detail::tl_index_of<field_type_list, node_list>::value);
    },
    [&](const auto&) {
      TENZIR_UNREACHABLE();
    },
  };
  caf::visit(visitor, value_type);
}

auto node_list::commit_to(builder_ref r, bool mark_dead) -> void {
  if (mark_dead) {
    type_index_ = type_index_empty;
    mark_this_dead();
  }
  for (auto& v : data_) {
    if (v.is_alive()) {
      v.commit_to(r, mark_dead);
    } else {
      break;
    }
  }
}
auto node_list::commit_to(tenzir::list& r, bool mark_dead) -> void {
  if (mark_dead) {
    type_index_ = type_index_empty;
    mark_this_dead();
  }
  for (auto& v : data_) {
    if (v.is_alive()) {
      auto& d = r.emplace_back();
      v.commit_to(d, mark_dead);
    } else {
      break;
    }
  }
}

auto node_list::clear() -> void {
  node_base::mark_this_dead();
  type_index_ = type_index_empty;
  for (auto& v : data_) {
    v.clear();
  }
}

} // namespace detail::record_builder
} // namespace tenzir
