//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data_builder.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/cast.hpp"
#include "tenzir/concept/parseable/string/string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/enumerate.hpp"
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
#include <fmt/core.h>

#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace tenzir {

namespace {
constexpr std::size_t structured_element_limit = 20'000;
}

data_builder::data_builder(data_parsing_function parser, diagnostic_handler* dh)
  : data_builder{{}, parser, dh} {
}

data_builder::data_builder(settings s, data_parsing_function parser,
                           diagnostic_handler* dh)
  : root_{s.building_settings},
    dh_{dh},
    settings_{s},
    parser_{std::move(parser)} {
  root_.mark_this_dead();
}

auto data_builder::record() -> detail::data_builder::node_record* {
  return root_.record();
}

auto data_builder::list() -> detail::data_builder::node_list* {
  return root_.list();
}

auto data_builder::data_unparsed(std::string text) -> void {
  return root_.data_unparsed(std::move(text));
}

auto data_builder::find_field_raw(std::string_view key)
  -> detail::data_builder::node_object* {
  if (auto r = root_.get_if<detail::data_builder::node_record>()) {
    return r->at(key);
  }
  return nullptr;
}

auto data_builder::clear() -> void {
  root_.clear();
}

auto data_builder::free() -> void {
  root_.data(caf::none);
}

auto data_builder::commit_to(series_builder& builder, bool mark_dead,
                             const tenzir::type* seed) -> void {
  root_.commit_to(builder_ref{builder}, *this, seed, value_path{}, mark_dead);
}

auto data_builder::materialize(bool mark_dead, const tenzir::type* seed)
  -> tenzir::data {
  tenzir::data res;
  root_.commit_to(res, *this, seed, value_path{}, mark_dead);
  return res;
}

auto data_builder::lookup_record_fields(
  const tenzir::record_type* r, detail::data_builder::node_record* apply)
  -> const detail::data_builder::field_type_lookup_map* {
  if (not r) {
    return nullptr;
  }
  auto seed_it = schema_type_lookup_.find(*r);
  if (seed_it == schema_type_lookup_.end()) {
    bool success = false;
    std::tie(seed_it, success) = schema_type_lookup_.try_emplace(*r);
    TENZIR_ASSERT(success);
    for (auto v : r->fields()) {
      // ensure the field exists
      if (apply) {
        auto* ptr = apply->try_field(v.name);
        ptr->mark_this_relevant_for_signature();
      }
      // add its type back to the lookup map
      const auto [_, field_success]
        = seed_it->second.try_emplace(std::string{v.name}, std::move(v.type));
      TENZIR_ASSERT(field_success);
    }
  } else {
    for (const auto& [k, t] : seed_it->second) {
      // ensure the field exists
      if (apply) {
        auto* ptr = apply->try_field(k);
        ptr->mark_this_relevant_for_signature();
      }
    }
  }
  return std::addressof(seed_it->second);
}

auto data_builder::append_signature_to(signature_type& sig,
                                       const tenzir::type* seed) -> void {
  root_.append_to_signature(sig, *this, seed, value_path{});
}

auto data_builder::emit_or_throw(tenzir::diagnostic&& diag) -> void {
  if (dh_) {
    dh_->emit(std::move(diag));
  } else {
    throw std::move(diag);
  }
}

auto data_builder::emit_or_throw(tenzir::diagnostic_builder&& builder) -> void {
  if (dh_) {
    std::move(builder).emit(*dh_);
  } else {
    std::move(builder).throw_();
  }
}

auto data_builder::emit_mismatch_warning(const type_kind& value_type,
                                         const type& seed_type,
                                         const value_path& path) -> void {
  emit_or_throw(diagnostic::warning("parsed field `{}` contains `{}`, but the "
                                    "schema expects `{}`",
                                    path, value_type, seed_type.kind()));
}

namespace {
template <typename Tenzir_Type>
struct parser_for;

template <typename Tenzir_Type>
  requires caf::detail::is_complete<parser_registry<type_to_data_t<Tenzir_Type>>>
struct parser_for<Tenzir_Type> : parser_registry<type_to_data_t<Tenzir_Type>> {
};

// the parser registry for boolean is only for "0 1" ...
template <>
struct parser_for<bool_type> : std::type_identity<decltype(parsers::boolean)> {
};

template <typename T>
concept has_parser = caf::detail::is_complete<parser_for<T>>;
static_assert(has_parser<time_type>);

template <has_parser... Types>
auto parse_as_data(std::string_view s, tenzir::data& d) -> bool {
  constexpr auto l = []<typename Type>(std::string_view s, tenzir::data& d) {
    using T = type_to_data_t<Type>;
    auto res = T{};
    auto p = typename parser_for<Type>::type{};
    if (p(s, res)) {
      d = std::move(res);
      return true;
    }
    return false;
  };
  return (l.template operator()<Types>(s, d) or ...);
}

auto parse_enumeration(std::string_view s, const enumeration_type& e)
  -> detail::data_builder::data_parsing_result {
  s = detail::trim(s);
  enumeration v;
  const auto [ptr, errc] = std::from_chars(s.begin(), s.end(), v);
  if (errc == std::errc{}) {
    if (not e.field(v).empty()) {
      return tenzir::data{v};
    }
  }
  if (auto opt = e.resolve(s)) {
    return tenzir::data{static_cast<enumeration>(*opt)};
  }
  return diagnostic::warning("failed to parse enumeration value")
    .note("value was \"{}\"", s)
    .done();
}

auto parse_duration(std::string_view s, const type& seed)
  -> detail::data_builder::data_parsing_result {
  auto cast_res = cast_value(string_type{}, s, duration_type{});
  if (cast_res) {
    return {*cast_res};
  }
  auto unit = seed.attribute("unit").value_or("s");
  cast_res
    = cast_value(string_type{}, fmt::format("{} {}", s, unit), duration_type{});
  if (not cast_res) {
    return diagnostic::warning("failed to parse value as requested type")
      .hint("value was `{}`, desired type was `{}`", s, seed)
      .done();
  }
  return {*cast_res};
}
auto parse_time(std::string_view s, const type& seed)
  -> detail::data_builder::data_parsing_result {
  auto cast_res = cast_value(string_type{}, s, time_type{});
  if (cast_res) {
    return {*cast_res};
  }
  auto unit = seed.attribute("unit");
  if (not unit) {
    return diagnostic::warning("failed to parse value as requested type")
      .hint("value was `{}`, desired type was `{}`", s, seed)
      .done();
  }
  int64_t value = 0;
  auto [ptr, ec] = std::from_chars(s.begin(), s.end(), value);
  if (ec != std::errc{} or ptr != s.end()) {
    return diagnostic::warning("failed to parse value as requested type")
      .hint("value was `{}`, desired type was `{}`", s, seed)
      .done();
  }
  auto since_epoch = cast_value(int64_type{}, value, duration_type{}, *unit);
  if (not since_epoch) {
    return diagnostic::warning("failed to parse value as requested type")
      .hint("value was `{}`, desired type was `{}`", s, seed)
      .note("{}", since_epoch.error())
      .done();
  }
  return {time{} + *since_epoch};
}
} // namespace

namespace detail::data_builder {

auto best_effort_parser(std::string_view s) -> std::optional<data> {
  tenzir::data res;
  if (parse_as_data<bool_type, int64_type, uint64_type, double_type, time_type,
                    duration_type, subnet_type, ip_type>(s, res)) {
    return res;
  }
  return std::nullopt;
}

auto basic_seeded_parser(std::string_view s, const tenzir::type& seed,
                         const value_path& path)
  -> detail::data_builder::data_parsing_result {
  const auto visitor = detail::overload{
    [&s, &path]<has_parser Type>(
      const Type& t) -> detail::data_builder::data_parsing_result {
      using T = type_to_data_t<Type>;
      auto res = T{};
      auto p = typename parser_for<Type>::type{};
      if (p(s, res)) {
        return tenzir::data{std::move(res)};
      } else {
        return diagnostic::warning("failed to parse value as requested type")
          .hint("value was `{}`, desired type was `{}`", s, t)
          .hint("field `{}`", path)
          .done();
      }
    },
    [](const string_type&) -> detail::data_builder::data_parsing_result {
      return {};
    },
    [&s,
     &seed](const duration_type&) -> detail::data_builder::data_parsing_result {
      return parse_duration(s, seed);
    },
    [&s, &seed](const time_type&) -> detail::data_builder::data_parsing_result {
      return parse_time(s, seed);
    },
    [&s](
      const enumeration_type& e) -> detail::data_builder::data_parsing_result {
      return parse_enumeration(s, e);
    },
    [&s](const blob_type&) -> detail::data_builder::data_parsing_result {
      auto dec = detail::base64::try_decode<tenzir::blob>(s);
      if (dec) {
        return {std::move(*dec)};
      } else {
        return diagnostic::warning("base64 decode failure").done();
      }
    },
    [&path]<typename T>(
      const T& t) -> detail::data_builder::data_parsing_result {
      return diagnostic::warning("schema expected `{}`, but the input "
                                 "contained a string",
                                 tenzir::type{t}.kind())
        .hint("field `{}`", path)
        .done();
    },
  };
  return match(seed, visitor);
}

auto basic_parser(std::string_view s, const tenzir::type* seed,
                  const value_path& path)
  -> detail::data_builder::data_parsing_result {
  if (seed) {
    return basic_seeded_parser(s, *seed, path);
  }
  if (s.empty()) {
    return {};
  }
  auto res = best_effort_parser(s);
  if (res) {
    return std::move(*res);
  } else {
    return {};
  }
}

auto non_number_parser(std::string_view s, const tenzir::type* seed,
                       const value_path& path)
  -> detail::data_builder::data_parsing_result {
  if (seed) {
    return data_builder::basic_seeded_parser(s, *seed, path);
  }
  if (s.empty()) {
    return tenzir::data{std::string{}};
  }
  auto res = tenzir::data{};
  if (parse_as_data<bool_type, time_type, duration_type, subnet_type, ip_type>(
        s, res)) {
    return res;
  }
  return {};
}

namespace {

template <typename Field, typename Tenzir_Type>
struct index_regression_tester {
  constexpr static auto index_in_field
    = detail::tl_index_of<field_type_list, Field>::value;
  constexpr static auto tenzir_type_index = Tenzir_Type::type_index;

  static_assert(index_in_field == tenzir_type_index);

  using type = void;
};

static_assert(detail::tl_size<field_type_list>::value
                == detail::tl_size<data::types>::value + 1,
              "The could should match up, apart from the lack of `enriched` in "
              "`tenzir::data`");

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

auto node_record::try_field(std::string_view name) -> node_object* {
  auto [it, inserted] = lookup_.try_emplace(name, data_.size());
  if (not inserted) {
    return &data_[it->second].value;
  }
  return &data_.emplace_back(it->first, settings_).value;
}

auto node_record::reserve(size_t N) -> void {
  lookup_.reserve(N);
  data_.reserve(N);
}

auto node_record::field(std::string_view name) -> node_object* {
  mark_this_alive();
  auto* f = try_field(name);
  switch (settings_.duplicate_keys) {
    using enum duplicate_keys;
    case overwrite: {
      break;
    }
    case to_list:
    case merge_structural: {
      if (f->is_alive()
          and f->value_state_ == node_object::value_state_type::has_value) {
        f->is_repeat_key_list = true;
      }
    }
  };
  f->mark_this_alive();
  return f;
}

auto node_record::at(std::string_view key) -> node_object* {
  for (const auto& [field_name, index] : lookup_) {
    if (not data_[index].value.is_alive()) {
      continue;
    }
    const auto [field_name_mismatch, key_mismatch]
      = std::ranges::mismatch(field_name, key);
    if (field_name_mismatch == field_name.end()) {
      if (key_mismatch == key.end()) {
        return &data_[index].value;
      }
      if (*key_mismatch != '.') {
        continue;
      }
      if (auto* r = data_[index].value.get_if<node_record>()) {
        if (auto* result = r->at(key.substr(1 + key_mismatch - key.begin()))) {
          return result;
        }
      }
    }
  }
  return nullptr;
}

auto node_record::append_to_signature(signature_type& sig,
                                      class data_builder& rb,
                                      const tenzir::record_type* seed,
                                      value_path path) -> void {
  sig.push_back(record_start_marker);
  // if we have a seed, we need too ensure that all fields exist first
  const field_type_lookup_map* seed_map = rb.lookup_record_fields(seed, this);
  // we are intentionally traversing `lookup_` here, because that is sorted by
  // name. this ensures that the signature computation will be the same
  for (const auto& [k, index] : lookup_) {
    auto& field = data_[index].value;
    if (not field.affects_signature()) {
      continue;
    }
    if (seed) {
      TENZIR_ASSERT(seed_map);
      const auto field_it = seed_map->find(k);
      if (field_it == seed_map->end()) {
        if (rb.settings_.schema_only) {
          field.mark_this_dead();
          continue;
        }
      } else {
        const auto key_bytes = as_bytes(k);
        sig.insert(sig.end(), key_bytes.begin(), key_bytes.end());

        field.append_to_signature(sig, rb, &(field_it->second), path.field(k));
        continue;
      }
      // break;
      // TODO this buggy break gives about 2.5x performance for suricata
      // However, this de-facto relies on the suricata input data adhering to
      // the format and having the fields in a consistent order.
      // It will also result in larger batches since in practice most fields
      // will be ignore for the signature computation. There may be a heuristic
      // that doesnt consider *every* field for the signature in order to speed
      // up computation.
      //
      // The question is what the cost would be for the resulting
      // batch/event quality
      // If performance of the signature compute is ever needed, one may
      // investigate only doing a heuristic compute of the byte signature. Maybe
      // just every 2rd/3rd field another alternative would be to not accurately
      // compute the signatures of nested structural entries.
    }
    const auto key_bytes = as_bytes(k);
    sig.insert(sig.end(), key_bytes.begin(), key_bytes.end());
    field.append_to_signature(sig, rb, nullptr, path.field(k));
  }
  sig.push_back(record_end_marker);
}

auto node_record::commit_to(tenzir::record_ref r, class data_builder& rb,
                            const tenzir::record_type* seed, value_path path,
                            bool mark_dead) -> void {
  auto field_map = rb.lookup_record_fields(seed, this);
  TENZIR_ASSERT(static_cast<bool>(field_map) == static_cast<bool>(seed));
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    if (seed) {
      auto it = field_map->find(k);
      // If the field is in the seed
      if (it != field_map->end()) {
        v.commit_to(r.field(k), rb, &(it->second), path.field(k), mark_dead);
        // No more work has to be done
        continue;
      }
      // If the field was not in the seed, but we are on schema-only
      if (rb.settings_.schema_only) {
        if (mark_dead) {
          // Explicitly call `node_object::clear` here, in order to also affect
          // nested structural types. Calling just `v.mark_this_dead()` would
          // not clear nested records/lists
          v.clear();
        }
        continue;
      }
    }
    v.commit_to(r.field(k), rb, nullptr, path.field(k), mark_dead);
  }
  if (mark_dead) {
    prune();
    mark_this_dead();
  }
}

auto node_record::commit_to(tenzir::record& r, class data_builder& rb,
                            const tenzir::record_type* seed, value_path path,
                            bool mark_dead) -> void {
  auto field_map = rb.lookup_record_fields(seed, this);
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    const auto [entry_it, success] = r.try_emplace(k);
    if (seed) {
      auto it = field_map->find(k);
      // If the field is in the seed
      if (it != field_map->end()) {
        v.commit_to(entry_it->second, rb, &(it->second), path.field(k),
                    mark_dead);
        // No more work has to be done
        continue;
      }
      // If the field was not in the seed, but we are on schema-only
      if (rb.settings_.schema_only) {
        if (mark_dead) {
          // Explicitly call `node_object::clear` here, in order to also affect
          // nested structural types. Calling just `v.mark_this_dead()` would
          // not clear nested records/lists
          v.clear();
        }
        continue;
      }
    }
    v.commit_to(entry_it->second, rb, nullptr, path.field(k), mark_dead);
  }
  if (mark_dead) {
    prune();
    mark_this_dead();
  }
}

auto node_record::prune() -> void {
  constexpr static auto pruned_size = structured_element_limit / 2;
  if (data_.size() > structured_element_limit) {
    data_.resize(pruned_size);
    data_.shrink_to_fit();
    const auto it
      = std::remove_if(lookup_.begin(), lookup_.end(), [](const auto& kvp) {
          return kvp.second > pruned_size;
        });
    lookup_.erase(it, lookup_.end());
    TENZIR_ASSERT(data_.size() == lookup_.size());
  }
}

auto node_record::clear() -> void {
  node_base::mark_this_dead();
  for (auto& [k, v] : data_) {
    v.clear();
  }
}

auto node_object::null(bool overwrite) -> void {
  if (overwrite or settings_.duplicate_keys == duplicate_keys::overwrite
      or not is_repeat_key_list) {
    mark_this_alive();
    value_state_ = value_state_type::has_value;
    data_ = caf::none;
    return;
  }
  TENZIR_ASSERT(is_alive());
  TENZIR_ASSERT(is_repeat_key_list);
  TENZIR_ASSERT(value_state_ == value_state_type::has_value);
  /// The node could already have been upgraded to a list.If it is, we can
  /// just append to it
  if (auto* l = try_as<node_list>(data_)) {
    value_state_ = value_state_type::has_value;
    return l->null();
  }
  /// If the node isnt already upgraded ot a list, we need to do it
  auto previous_value = std::move(data_);
  auto* l = list();
  l->data(std::move(previous_value));
  return l->null();
}

auto node_object::data(tenzir::data d, bool overwrite) -> void {
  const auto visitor = detail::overload{
    [this, overwrite](non_structured_data_type auto& x) {
      data(std::move(x), overwrite);
    },
    [this, overwrite](tenzir::list& x) {
      auto* l = list(overwrite);
      for (auto& e : x) {
        l->data(std::move(e));
      }
    },
    [this, overwrite](tenzir::record& x) {
      auto* r = record(overwrite);
      for (auto& [k, v] : x) {
        r->field(k)->data(std::move(v), overwrite);
      }
    },
    []<unsupported_type T>(T&) {
      TENZIR_ASSERT(false, fmt::format("Unexpected type \"{}\" in "
                                       "`data_builder::data`",
                                       typeid(T).name()));
    },
  };

  match(d, visitor);
  mark_this_alive();
  value_state_ = value_state_type::has_value;
}

auto node_object::data(object_variant_type&& v) -> void {
  mark_this_alive();
  data_ = std::move(v);
  value_state_ = value_state_type::has_value;
}

auto node_object::data_unparsed(std::string text) -> void {
  mark_this_alive();
  value_state_ = value_state_type::unparsed;
  data_.emplace<std::string>(std::move(text));
}

auto node_object::record(bool overwrite) -> node_record* {
  const auto direct
    = overwrite or settings_.duplicate_keys == duplicate_keys::overwrite
      or not is_repeat_key_list
      or (get_if<node_record>()
          and settings_.duplicate_keys == duplicate_keys::merge_structural);
  if (direct) {
    mark_this_alive();
    value_state_ = value_state_type::has_value;
    if (auto* p = get_if<node_record>()) {
      p->mark_this_alive();
      return p;
    }
    return &data_.emplace<node_record>(settings_);
  }
  TENZIR_ASSERT(is_alive());
  TENZIR_ASSERT(is_repeat_key_list);
  TENZIR_ASSERT(value_state_ == value_state_type::has_value);
  /// Special case handling to turn data + record -> record
  if (settings_.duplicate_keys == duplicate_keys::merge_structural
      and not is_structural(data_.index())) {
    auto previous_value = std::move(data_);
    is_repeat_key_list = false;
    auto& r = data_.emplace<node_record>(settings_);
    r.reserve(2);
    r.field(settings_.top_key_name_for_records)->data(std::move(previous_value));
    return &r;
  }
  /// The node could already have been upgraded to a list.If it is, we can just
  /// append to it
  if (auto* l = try_as<node_list>(data_)) {
    value_state_ = value_state_type::has_value;
    return l->record();
  }
  /// If the node isnt already upgraded ot a list, we need to do it
  auto* l = list();
  return l->record();
}

auto node_object::list(bool overwrite) -> node_list* {
  const auto direct
    = overwrite or settings_.duplicate_keys == duplicate_keys::overwrite
      or not is_repeat_key_list
      or (get_if<node_list>()
          and settings_.duplicate_keys == duplicate_keys::merge_structural);
  if (direct) {
    mark_this_alive();
    value_state_ = value_state_type::has_value;
    if (auto* p = get_if<node_list>()) {
      p->mark_this_alive();
      return p;
    }
    return &data_.emplace<node_list>(settings_);
  }
  TENZIR_ASSERT(is_alive());
  TENZIR_ASSERT(is_repeat_key_list);
  TENZIR_ASSERT(value_state_ == value_state_type::has_value);
  /// The node could already have been upgraded to a list.If it is, we can just
  /// append to it
  if (auto* l = try_as<node_list>(data_)) {
    value_state_ = value_state_type::has_value;
    return l->list();
  }
  /// If the node isnt already upgraded ot a list, we need to do it
  auto previous_value = std::move(data_);
  auto& l = data_.emplace<node_list>(settings_);
  l.data(std::move(previous_value));
  return &l;
}

auto node_object::parse(class data_builder& rb, const tenzir::type* seed,
                        const value_path& path) -> void {
  if (value_state_ != value_state_type::unparsed) {
    return;
  }
  if (not is_alive()) {
    return;
  }
  if (not seed and rb.settings_.parse_schema_fields_only) {
    return;
  }
  TENZIR_ASSERT(std::holds_alternative<std::string>(data_));
  std::string_view raw_data = std::get<std::string>(data_);
  auto parse_result = rb.parser_(raw_data, seed, path);
  auto& [value, diag] = parse_result;
  if (diag) {
    rb.emit_or_throw(std::move(*diag));
  }
  if (value) {
    data(std::move(value));
    return;
  }
}

auto node_object::try_resolve_nonstructural_field_mismatch(
  class data_builder& rb, const tenzir::type* seed, const value_path& path)
  -> void {
  if (not seed) {
    return;
  }
  if (not is_alive()) {
    return;
  }
  // This is matching on (current_value, desired_tenzir_type)
  const auto visitor2 = detail::overload{
    [&rb, seed, &path]<non_structured_data_type T, typename S>(const T&,
                                                               const S&) {
      if constexpr (not std::same_as<data_to_type_t<T>, S>) {
        rb.emit_mismatch_warning(tag_v<data_to_type_t<T>>, *seed, path);
      }
    },
    // generic fallback
    [](const auto&, const auto&) {
      // noop: Mismatches involving structured types are handled in `commit_to`
      // to avoid duplication of this warning
    },
    // null -> anything
    [](const caf::none_t&, const auto&) {
      // noop: Null promotion happens in `commit_to`
    },
    // numeric -> double
    [this]<concepts::one_of<int64_t, uint64_t> T>(const T& value,
                                                  const double_type&) {
      data(static_cast<double>(value));
    },
    // int -> uint
    [this](const int64_t value, const uint64_type&) {
      if (value < 0) {
        null();
      } else {
        data(static_cast<uint64_t>(value));
      }
    },
    // uint -> int
    [this](const uint64_t& value, const int64_type&) {
      if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        null();
      } else {
        data(static_cast<int64_t>(value));
      }
    },
    // integral -> enumeration
    [this, &rb, &path]<concepts::one_of<uint64_t, int64_t> T>(
      const T& value, const enumeration_type& t) {
      if (value < 0
          or value > static_cast<T>(std::numeric_limits<enumeration>::max())) {
        rb.emit_or_throw(diagnostic::warning("value is out of range "
                                             "for expected type")
                           .note("value `{}` does not fit into `{}`", value,
                                 type_kind::of<enumeration_type>)
                           .note("field `{}`", path));
        null();
        return;
      }
      if (t.field(static_cast<uint32_t>(value)).empty()) {
        null();
        rb.emit_or_throw(
          diagnostic::warning("unknown integral enumeration value")
            .note("value `{}` is not defined for `{}`", value, t)
            .note("field `{}`", path));
      }
      data(static_cast<enumeration>(value));
    },
    // numeric -> duration
    [this, seed]<numeric_data_type T>(const T& value, const duration_type&) {
      auto unit = seed->attribute("unit").value_or("s");
      auto res = cast_value(data_to_type_t<T>{}, value, duration_type{}, unit);
      if (res) {
        data(*res);
        return;
      }
    },
    // numeric -> time
    [this, seed, &rb, &path]<numeric_data_type T>(const T& value,
                                                  const time_type&) {
      auto unit = seed->attribute("unit");
      if (not unit) {
        rb.emit_or_throw(
          diagnostic::warning("could not parse value as `{}`", time_type{})
            .note("the read value as a number, but the schema does not "
                  "specify a unit")
            .note("field `{}`", path));
        return;
      }
      auto res = cast_value(data_to_type_t<T>{}, value, duration_type{}, *unit);
      if (res) {
        data(time{} + *res);
        return;
      }
    },
    // anything -> string
    [this]<typename T>(const T& value, const string_type&)
      requires(fmt::is_formattable<T>::value)
    {
      data(fmt::format("{}", value));
    },
    };
  match(std::forward_as_tuple(data_, *seed), visitor2);
}

auto node_object::append_to_signature(signature_type& sig,
                                      class data_builder& rb,
                                      const tenzir::type* seed, value_path path)
  -> void {
  if (state_ == state::sentinel) {
    if (not seed) {
      return;
    }
    const auto seed_idx = seed->type_index();
    if (not is_structural(seed_idx)) {
      sig.push_back(static_cast<std::byte>(seed_idx));
      return;
    }
    // Sentinel structural types get handled by the regular visit below
  }
  parse(rb, seed, path);
  try_resolve_nonstructural_field_mismatch(rb, seed, path);
  // This lambda handles the case where the node is either null, or its stored
  // type mismatches the given seed
  // In case of a mismatch, the node is nulled out and the visit is rerun.
  // Rerunning the visit then runs into the `caf::none_t` case, which correctly
  // re-creates the nodes contents according to the seed.
  const auto visitor = detail::overload{
    [&sig, &rb, seed, this, &path](node_list& v) {
      const auto* ls = try_as<list_type>(seed);
      if (seed and not ls) {
        rb.emit_mismatch_warning(tag_v<list_type>, *seed, path);
        if (rb.settings_.schema_only) {
          null(true);
          return false;
        }
      }
      if (v.affects_signature() or ls) {
        v.append_to_signature(sig, rb, ls, path);
      }
      return true;
    },
    [&sig, &rb, seed, this, &path](node_record& v) {
      const auto* rs = try_as<record_type>(seed);
      if (seed and not rs) {
        rb.emit_mismatch_warning(tag_v<record_type>, *seed, path);
        if (rb.settings_.schema_only) {
          null(true);
          return false;
        }
      }
      if (v.affects_signature() or rs) {
        v.append_to_signature(sig, rb, rs, path);
      }
      return true;
    },
    [&sig, &rb, seed, this, &path](caf::none_t&) {
      // none could be the result of pre-seeding or being built with a true
      // null via the API for the first case we need to ensure we continue
      // doing seeding if we have a seed
      if (seed) {
        if (auto sr = try_as<tenzir::record_type>(seed)) {
          auto r = record(true);
          r->append_to_signature(sig, rb, sr, path);
          r->state_ = state::sentinel;
          this->value_state_ = value_state_type::null;
          return true;
        }
        if (auto sl = try_as<tenzir::list_type>(seed)) {
          auto* l = list(true);
          l->append_to_signature(sig, rb, sl, path);
          l->state_ = state::sentinel;
          this->value_state_ = value_state_type::null;
          return true;
        }
        sig.push_back(static_cast<std::byte>(seed->type_index()));
        return true;
      } else {
        constexpr static auto type_idx
          = detail::tl_index_of<field_type_list, caf::none_t>::value;
        sig.push_back(static_cast<std::byte>(type_idx));
        return true;
      }
    },
    [&sig, seed, this]<non_structured_data_type T>(T&) {
      constexpr static auto type_idx
        = detail::tl_index_of<field_type_list, T>::value;
      if (seed) {
        const auto seed_idx = seed->type_index();
        if (seed_idx != type_idx) {
          null(true);
          return false;
        }
      }
      sig.push_back(static_cast<std::byte>(type_idx));
      return true;
    },
    []<typename T>(T&) {
      TENZIR_ASSERT_ALWAYS(false, fmt::format("No `{}` should ever be stored",
                                              typeid(T).name()));
      return true;
    },
  };
  if (std::visit(visitor, data_)) {
    return;
  }
  // This should never fail a second time.
  TENZIR_ASSERT(std::visit(visitor, data_));
}

auto node_object::commit_to(tenzir::builder_ref builder, class data_builder& rb,
                            const tenzir::type* seed, value_path path,
                            bool mark_dead) -> void {
  if (mark_dead) {
    is_repeat_key_list = false;
  }
  if (rb.settings_.schema_only and not seed) {
    if (mark_dead) {
      mark_this_dead();
      value_state_ = value_state_type::null;
    }
    return;
  }
  if (value_state_ == value_state_type::null) {
    builder.null();
    if (mark_dead) {
      mark_this_dead();
    }
    return;
  }
  parse(rb, seed, path);
  try_resolve_nonstructural_field_mismatch(rb, seed, path);
  const auto visitor = detail::overload{
    [&builder, &rb, seed, mark_dead, &path](node_list& v) {
      if (v.is_dead()) {
        return;
      }
      const auto ls = try_as<tenzir::list_type>(seed);
      if (not ls and seed) {
        rb.emit_mismatch_warning(tag_v<list_type>, *seed, path);
        builder.null();
        v.mark_this_dead();
        return;
      }
      // Because we ensured that the seed matches above, we can now safely
      // call `builder.list()`
      auto l = builder.list();
      v.commit_to(std::move(l), rb, ls, path, mark_dead);
    },
    [&builder, &rb, seed, mark_dead, &path](node_record& v) {
      if (v.is_dead()) {
        return;
      }
      const auto rs = try_as<tenzir::record_type>(seed);
      if (not rs and seed) {
        rb.emit_mismatch_warning(tag_v<record_type>, *seed, path);
        builder.null();
        v.mark_this_dead();
        return;
      }
      // Because we ensured that the seed matches above, we can now safely
      // call `builder.record()`
      auto r = builder.record();
      v.commit_to(std::move(r), rb, rs, path, mark_dead);
    },
    [&builder, seed, &rb, &path]<non_structured_data_type T>(T& v) {
      auto res = builder.try_data(v);
      if (not res) {
        const auto& err = res.error();
        if (tenzir::ec{err.code()} == ec::type_clash) {
          rb.emit_mismatch_warning(tag_v<data_to_type_t<T>>, *seed, path);
        } else {
          rb.emit_or_throw(
            diagnostic::warning("issue writing data into builder")
              .note("{}", err)
              .note("field `{}`", path));
        }
      }
    },
    [](auto&) {
      TENZIR_UNREACHABLE();
    },
  };
  std::visit(visitor, data_);
  if (mark_dead) {
    mark_this_dead();
    value_state_ = value_state_type::null;
  }
}

auto node_object::commit_to(tenzir::data& r, class data_builder& rb,
                            const tenzir::type* seed, value_path path,
                            bool mark_dead) -> void {
  if (mark_dead) {
    is_repeat_key_list = false;
  }
  if (rb.settings_.schema_only and not seed) {
    if (mark_dead) {
      mark_this_dead();
      value_state_ = value_state_type::null;
    }
    return;
  }
  if (value_state_ == value_state_type::null) {
    r = caf::none;
    if (mark_dead) {
      mark_this_dead();
    }
    return;
  }
  parse(rb, seed, path);
  try_resolve_nonstructural_field_mismatch(rb, seed, path);
  const auto visitor = detail::overload{
    [&r, &rb, seed, mark_dead, &path](node_list& v) {
      if (v.is_dead()) {
        return;
      }
      const auto ls = try_as<tenzir::list_type>(seed);
      if (not ls and seed) {
        rb.emit_mismatch_warning(tag_v<list_type>, *seed, path);
        r = caf::none;
        v.mark_this_dead();
        return;
      }
      r = tenzir::list{};
      v.commit_to(as<tenzir::list>(r), rb, ls, path, mark_dead);
    },
    [&r, &rb, seed, mark_dead, &path](node_record& v) {
      if (v.is_dead()) {
        return;
      }
      const auto rs = try_as<tenzir::record_type>(seed);
      if (not rs and seed) {
        rb.emit_mismatch_warning(tag_v<record_type>, *seed, path);
        r = caf::none;
        v.mark_this_dead();
        return;
      }
      r = tenzir::record{};
      v.commit_to(as<tenzir::record>(r), rb, rs, path, mark_dead);
    },
    [&r, mark_dead]<non_structured_data_type T>(T& v) {
      if (mark_dead) {
        r = std::move(v);
      } else {
        r = v;
      }
    },
    []<unsupported_type T>(T&) {
      TENZIR_UNREACHABLE();
    },
  };
  std::visit(visitor, data_);
  if (mark_dead) {
    mark_this_dead();
    value_state_ = value_state_type::null;
  }
}

auto node_object::clear() -> void {
  is_repeat_key_list = false;
  value_state_ = value_state_type::null;
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

auto node_list::push_back_node() -> node_object* {
  TENZIR_ASSERT(first_dead_idx_ <= data_.size());
  if (first_dead_idx_ < data_.size()) {
    auto& value = data_[first_dead_idx_];
    TENZIR_ASSERT(not value.is_alive());
    ++first_dead_idx_;
    return &value;
  }
  TENZIR_ASSERT_EXPENSIVE(std::ranges::all_of(data_, [](const auto& o) {
    return o.state_ == state::alive;
  }));
  ++first_dead_idx_;
  return &data_.emplace_back(settings_);
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
  return match(d, visitor);
}

auto node_list::data(object_variant_type&& v) -> void {
  mark_this_alive();
  push_back_node()->data(std::move(v));
  update_type_index(type_index_, data_.back().current_index());
}

auto node_list::data_unparsed(std::string text) -> void {
  mark_this_alive();
  type_index_ = type_index_generic_mismatch;
  return push_back_node()->data_unparsed(std::move(text));
}

auto node_list::null() -> void {
  return data(caf::none);
}

auto node_list::record() -> node_record* {
  mark_this_alive();
  update_type_index(type_index_, type_index_record);
  return push_back_node()->record();
}

auto node_list::list() -> node_list* {
  mark_this_alive();
  update_type_index(type_index_, type_index_list);
  return push_back_node()->list();
}

auto node_list::append_to_signature(signature_type& sig, class data_builder& rb,
                                    const tenzir::list_type* seed,
                                    value_path path) -> void {
  sig.push_back(list_start_marker);
  // This has to be a local value because `list_type::value_type` returns a
  // value.
  tenzir::type seed_type;
  tenzir::type* seed_type_ptr = nullptr;
  auto seed_index = static_cast<size_t>(-1);
  if (seed) {
    seed_type = seed->value_type();
    seed_type_ptr = &seed_type;
    seed_index = seed_type.type_index();
  }
  if (type_index_ == seed_index and not is_structural(type_index_)) {
    sig.push_back(static_cast<std::byte>(type_index_));
  } // This also isn't pretty, but aligns with the `series_builder` very well
  else if (seed) {
    node_object sentinel{settings_};
    sentinel.state_ = state::sentinel;
    sentinel.append_to_signature(sig, rb, seed_type_ptr, path.list());
  } else if (not is_structural(type_index_)
             and type_index_ < type_index_empty) {
    sig.push_back(static_cast<std::byte>(type_index_));
  } else if (not seed and type_index_ == type_index_numeric_mismatch) {
    auto negative = size_t{0};
    auto large_positive = size_t{0};
    auto floating = size_t{0};
    constexpr static auto idx_int
      = detail::tl_index_of<field_type_list, int64_t>::value;
    constexpr static auto idx_uint
      = detail::tl_index_of<field_type_list, uint64_t>::value;
    constexpr static auto idx_double
      = detail::tl_index_of<field_type_list, double>::value;
    {
      const auto visitor = detail::overload{
        [](const auto&) {
          TENZIR_UNREACHABLE();
        },
        [&](const int64_t& value) {
          if (value < 0) {
            ++negative;
          }
        },
        [&](const uint64_t& value) {
          if (value > std::numeric_limits<int64_t>::max()) {
            ++large_positive;
          }
        },
        [&](const double&) {
          ++floating;
        },
      };
      for (const auto& e : alive_elements()) {
        std::visit(visitor, e.data_);
      }
    }
    auto final_index = size_t{0};
    if (floating > 0) {
      final_index = idx_double;
    } else if (negative and large_positive) {
      final_index = idx_double;
    } else if (large_positive) {
      final_index = idx_uint;
    } else {
      final_index = idx_int;
    }
    for (auto& e : alive_elements()) {
      const auto visitor = detail::overload{
        [](const auto&) {
          TENZIR_UNREACHABLE();
        },
        [&]<numeric_data_type T>(const T& value) {
          switch (final_index) {
            case idx_int: {
              e.data_.emplace<int64_t>(static_cast<int64_t>(value));
              break;
            }
            case idx_uint: {
              e.data_.emplace<uint64_t>(static_cast<uint64_t>(value));
              break;
            }
            case idx_double: {
              e.data_.emplace<double>(static_cast<double>(value));
              break;
            }
          }
        },
      };
      std::visit(visitor, e.data_);
    }
    sig.push_back(static_cast<std::byte>(final_index));
  } else {
    ///////////
    /// TODO this entire generic path is a best effort trying to match
    /// the behaviour the of series builder.
    /// This  case also applies when there is any unparsed fields.
    /// In the generic "mismatch" handling case, we need to iterate every
    /// element of the list.
    /// Originally this function computed the signature of every element, and
    /// compared it to the previous elements signature. However, that does not
    /// address cases where the series builder would e.g. merge records
    /// Currently this function simply ignores structural types, as the
    /// as the `series_builder` will potentially resolve some conflicts.
    /// Parsing of unparsed non-structural elements still happens, but the
    /// contents of structural elements will only be parsed during a `commit_to`
    /// call.
    /// The downside of this is lists containing records with different fields
    /// will have the same signature, causing events that dont formally have the
    /// same schema to be merged. However, that seems acceptable for now.
    ///////////
    auto initial_sig_size = sig.size();
    auto last_sig_start_index = std::size_t{0};
    auto non_matching_signatures = false;
    auto has_list = false;
    auto has_record = false;
    for (auto [i, v] : detail::enumerate(alive_elements())) {
      auto curr_sig_start_index = sig.size();
      if (v.current_index() == type_index_list) {
        sig.push_back(list_start_marker);
        sig.push_back(list_end_marker);
      } else if (v.current_index() == type_index_record) {
        sig.push_back(record_start_marker);
        sig.push_back(record_end_marker);
      } else if (v.current_index() == type_index_null) {
        continue;
      } else {
        v.append_to_signature(sig, rb, seed_type_ptr, path.index(i));
      }
      if (last_sig_start_index == 0) {
        last_sig_start_index = curr_sig_start_index;
        continue;
      }
      TENZIR_ASSERT(curr_sig_start_index >= last_sig_start_index);
      auto prev_sig = std::span{
        sig.begin() + last_sig_start_index,
        sig.begin() + curr_sig_start_index,
      };
      auto curr_sig = std::span{
        sig.begin() + curr_sig_start_index,
        sig.end(),
      };
      TENZIR_ASSERT(curr_sig.size() == 1 or curr_sig.size() == 2);
      const auto prev_matches_current = std::ranges::equal(prev_sig, curr_sig);
      if (prev_matches_current) {
        // drop the last appended signature
        sig.erase(sig.begin() + curr_sig_start_index, sig.end());
      } else {
        non_matching_signatures = true;
        last_sig_start_index = curr_sig_start_index;
      }
    }
    non_matching_signatures |= (has_record and has_list);
    non_matching_signatures
      |= (has_record or has_list) and sig.size() > initial_sig_size + 2;
    if (non_matching_signatures) {
      rb.emit_or_throw(
        diagnostic::warning("type mismatch between list elements")
          .note("field `{}`", path));
    }
  }
  sig.push_back(list_end_marker);
}

auto node_list::commit_to(builder_ref r, class data_builder& rb,
                          const tenzir::list_type* seed, value_path path,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto [i, v] : detail::enumerate(alive_elements())) {
    v.commit_to(r, rb, seed ? &field_seed : nullptr, path.index(i), mark_dead);
  }
  TENZIR_ASSERT_EXPENSIVE(
    not mark_dead
    or std::ranges::all_of(data_.begin() + first_dead_idx_, data_.end(),
                           [](const auto& o) {
                             return o.state_ == state::dead;
                           }));
  if (mark_dead) {
    type_index_ = type_index_empty;
    first_dead_idx_ = 0;
    prune();
    mark_this_dead();
  }
}

auto node_list::commit_to(tenzir::list& l, class data_builder& rb,
                          const tenzir::list_type* seed, value_path path,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto [i, v] : detail::enumerate(alive_elements())) {
    auto& d = l.emplace_back();
    v.commit_to(d, rb, seed ? &field_seed : nullptr, path.index(i), mark_dead);
  }
  TENZIR_ASSERT_EXPENSIVE(
    not mark_dead
    or std::ranges::all_of(data_.begin() + first_dead_idx_, data_.end(),
                           [](const auto& o) {
                             return o.state_ == state::dead;
                           }));
  if (mark_dead) {
    type_index_ = type_index_empty;
    first_dead_idx_ = 0;
    prune();
    mark_this_dead();
  }
}

auto node_list::alive_elements() -> std::span<node_object> {
  return {data_.begin(), data_.begin() + first_dead_idx_};
}

auto node_list::prune() -> void {
  if (data_.size() > structured_element_limit) {
    data_.resize(structured_element_limit, {{settings_}});
    data_.shrink_to_fit();
  }
}

auto node_list::clear() -> void {
  node_base::mark_this_dead();
  type_index_ = type_index_empty;
  first_dead_idx_ = 0;
  for (auto& v : data_) {
    v.clear();
  }
}

node_list::~node_list() {
  for (const auto& e : data_) {
    TENZIR_ASSERT(e.state_ != state::sentinel);
  }
}

} // namespace detail::data_builder
} // namespace tenzir
