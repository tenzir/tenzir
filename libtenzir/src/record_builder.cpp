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

auto record_builder::commit_to(series_builder& builder, bool mark_dead,
                               const tenzir::type* seed) -> void {
  const tenzir::record_type* rs = caf::get_if<tenzir::record_type>(seed);
  if (seed and not rs) {
    emit_or_throw(diagnostic::warning("selected type is not a record")
                    .note("type was `{}`", seed->name()));
  }
  root_.commit_to(builder.record(), *this, rs, mark_dead);
}

auto record_builder::materialize(bool mark_dead,
                                 const tenzir::type* seed) -> tenzir::record {
  const tenzir::record_type* rs = caf::get_if<tenzir::record_type>(seed);
  if (seed and not rs) {
    emit_or_throw(diagnostic::warning("selected type is not a record")
                    .note("type was `{}`", seed->name()));
  }
  tenzir::record res;
  root_.commit_to(res, *this, rs, mark_dead);
  return res;
}

auto record_builder::lookup_record_fields(
  const tenzir::record_type* r, detail::record_builder::node_record* apply)
  -> const detail::record_builder::field_type_lookup_map* {
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
        ptr->mark_this_relevant();
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
        ptr->mark_this_relevant();
      }
    }
  }
  return std::addressof(seed_it->second);
}

auto record_builder::append_signature_to(signature_type& sig,
                                         const tenzir::type* seed) -> void {
  auto* seed_as_record_type = caf::get_if<tenzir::record_type>(&*seed);
  if (seed) {
    if (seed_as_record_type) {
      return root_.append_to_signature(sig, *this, seed_as_record_type);
    } else {
      emit_or_throw(
        diagnostic::warning("selected schema is not a record and will be "
                            "ignored"));
    }
  }
  root_.append_to_signature(sig, *this, nullptr);
}

auto record_builder::emit_or_throw(tenzir::diagnostic&& diag) -> void {
  if (dh_) {
    dh_->emit(std::move(diag));
  } else {
    throw std::move(diag);
  }
}

auto record_builder::emit_or_throw(tenzir::diagnostic_builder&& builder)
  -> void {
  if (dh_) {
    std::move(builder).emit(*dh_);
  } else {
    std::move(builder).throw_();
  }
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
} // namespace

namespace detail::record_builder {

auto basic_seeded_parser(std::string_view s, const tenzir::type& seed)
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
    [&s](const blob_type&) -> detail::record_builder::data_parsing_result {
      // TODO this doesnt necessarily need to copy the same bytes into a blob,
      // but the record builder has no notion of storing the type outside of the
      // variant alternative
      auto bytes_data = as_bytes(s);
      return {tenzir::blob{bytes_data.begin(), bytes_data.end()}};
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
  };
  return caf::visit(visitor, seed);
}

auto basic_parser(std::string_view s, const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result {
  if (seed) {
    return basic_seeded_parser(s, *seed);
  }
  auto p = parsers::boolean | parsers::integer | parsers::count | parsers::real | parsers::time
           | parsers::duration | parsers::net | parsers::ip;
  auto res = tenzir::data{};
  if (p(s, res)) {
    return res;
  }
  return {};
}

auto non_number_parser(std::string_view s, const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result {
  if (seed) {
    return record_builder::basic_seeded_parser(s, *seed);
  }
  auto p = parsers::boolean | parsers::time | parsers::duration | parsers::net | parsers::ip;
  auto res = tenzir::data{};
  if (p(s, res)) {
    return res;
  }
  return {};
}

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
    if (not data_[index].value.is_alive()) {
      continue;
    }
    const auto [field_name_mismatch, key_mismatch]
      = std::ranges::mismatch(field_name, key);
    if (field_name_mismatch == field_name.end()) {
      if (key_mismatch == key.end()) {
        return &data_[index].value;
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
                                      class record_builder& rb,
                                      const tenzir::record_type* seed) -> void {
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
        if (rb.schema_only_) {
          field.mark_this_dead();
          continue;
        }
      } else {
        const auto key_bytes = as_bytes(k);
        sig.insert(sig.end(), key_bytes.begin(), key_bytes.end());

        field.append_to_signature(sig, rb, &(field_it->second));
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
      // event quality
    }
    const auto key_bytes = as_bytes(k);
    sig.insert(sig.end(), key_bytes.begin(), key_bytes.end());
    field.append_to_signature(sig, rb, nullptr);
  }
  sig.push_back(record_end_marker);
}

auto node_record::commit_to(tenzir::record_ref r, class record_builder& rb,
                            const tenzir::record_type* seed,
                            bool mark_dead) -> void {
  auto field_map = rb.lookup_record_fields(seed, this);
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    if (seed) {
      auto it = field_map->find(k);
      if (it != field_map->end()) {
        v.commit_to(r.field(k), rb, &(it->second), mark_dead);
        continue;
      }
      if (rb.schema_only_) {
        continue;
      }
    }
    v.commit_to(r.field(k), rb, nullptr, mark_dead);
  }
  if (mark_dead) {
    mark_this_dead();
  }
}

auto node_record::commit_to(tenzir::record& r, class record_builder& rb,
                            const tenzir::record_type* seed,
                            bool mark_dead) -> void {
  auto field_map = rb.lookup_record_fields(seed, this);
  for (auto& [k, v] : data_) {
    if (not v.is_alive()) {
      continue;
    }
    const auto [entry_it, success] = r.try_emplace(k);
    if (seed) {
      auto it = field_map->find(k);
      if (it != field_map->end()) {
        v.commit_to(entry_it->second, rb, &(it->second), mark_dead);
        continue;
      }
      if (rb.schema_only_) {
        continue;
      }
    }
    v.commit_to(entry_it->second, rb, nullptr, mark_dead);
  }
  if (mark_dead) {
    mark_this_dead();
  }
}

auto node_record::clear() -> void {
  node_base::mark_this_dead();
  for (auto& [k, v] : data_) {
    v.clear();
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

auto node_field::parse(class record_builder& rb,
                       const tenzir::type* seed) -> void {
  if (not is_unparsed_) {
    return;
  }
  if (not is_alive()) {
    return;
  }
  is_unparsed_ = false;
  if (not seed and rb.parse_schema_fields_only_) {
    return;
  }
  TENZIR_ASSERT(std::holds_alternative<std::string>(data_));
  std::string_view raw_data = std::get<std::string>(data_);
  auto parse_result = rb.parser_(raw_data, seed);
  auto& [value, diag] = parse_result;
  if (diag) {
    rb.emit_or_throw(std::move(*diag));
  }
  if (value) {
    if (rb.schema_only_ and seed
        and value->get_data().index() != seed->type_index()) {
      // if schema only is enabled, and the parsed field does not match the
      // schema, we discard its value
      rb.emit_or_throw(diagnostic::warning("parsed field type does not "
                                           "match the provided schema. This "
                                           "is a shortcoming of the parser.")
                         .note("string `{}` parsed as type index `{}`, but the "
                               "schema expected `{}`",
                               raw_data, value->get_data().index(),
                               seed->type_index()));
    }
    data(std::move(value));
    return;
  }
}

auto node_field::append_to_signature(signature_type& sig,
                                     class record_builder& rb,
                                     const tenzir::type* seed) -> void {
  if (is_unparsed_) {
    parse(rb, seed);
  }
  if (state_ == state::sentinel) {
    if (not seed) {
      return;
    }
    const auto seed_idx = seed->type_index();
    if (not is_structural(seed_idx)) {
      sig.push_back(static_cast<std::byte>(seed_idx));

      return;
      ;
    }
    // sentinel structural types get handled by the regular visit below
  }
  const auto visitor = detail::overload{
    [&sig, &rb, seed](node_list& v) {
      const auto* ls = caf::get_if<list_type>(seed);
      if (seed and not ls) {
        rb.emit_or_throw(diagnostic::warning("event field is a list, but the "
                                             "schema does not expect a list"));
      }
      if (v.affects_signature() or ls) {
        v.append_to_signature(sig, rb, ls);
      }
    },
    [&sig, &rb, seed](node_record& v) {
      const auto* rs = caf::get_if<record_type>(seed);
      if (seed and not rs) {
        rb.emit_or_throw(
          diagnostic::warning("event field is a record, but the "
                              "schema does not expect a record"));
      }
      if (v.affects_signature() or rs) {
        v.append_to_signature(sig, rb, rs);
      }
    },
    [&sig, &rb, seed, this](caf::none_t&) {
      // none could be the result of pre-seeding or being built with a true null
      // via the API for the first case we need to ensure we continue doing
      // seeding if we have a seed
      if (seed) {
        if (auto sr = caf::get_if<tenzir::record_type>(seed)) {
          return record()->append_to_signature(sig, rb, sr);
        }
        if (auto sl = caf::get_if<tenzir::list_type>(seed)) {
          return list()->append_to_signature(sig, rb, sl);
        }
        sig.push_back(static_cast<std::byte>(seed->type_index()));
      } else {
        constexpr static auto type_idx
          = caf::detail::tl_index_of<field_type_list, caf::none_t>::value;
        sig.push_back(static_cast<std::byte>(type_idx));
      }
    },
    [&sig, &rb, seed, this]<non_structured_data_type T>(T& v) {
      constexpr static auto type_idx
        = caf::detail::tl_index_of<field_type_list, T>::value;
      auto result_index = type_idx;
      if (seed and type_idx != seed->type_index()) {
        const auto seed_idx = seed->type_index();
        if constexpr (is_numeric(type_idx)) {
          // numeric conversion if possible
          if (is_numeric(seed_idx)) {
            switch (seed_idx) {
              case caf::detail::tl_index_of<field_type_list, int64_t>::value:
                data(static_cast<int64_t>(v));
                result_index
                  = caf::detail::tl_index_of<field_type_list, int64_t>::value;
                goto done;
              case caf::detail::tl_index_of<field_type_list, uint64_t>::value:
                data(static_cast<uint64_t>(v));
                result_index
                  = caf::detail::tl_index_of<field_type_list, uint64_t>::value;
                goto done;
              case caf::detail::tl_index_of<field_type_list, double>::value:
                data(static_cast<double>(v));
                result_index
                  = caf::detail::tl_index_of<field_type_list, double>::value;
                goto done;
              case caf::detail::tl_index_of<field_type_list, enumeration>::value:
                data(static_cast<enumeration>(v));
                result_index = caf::detail::tl_index_of<field_type_list,
                                                        enumeration>::value;
                goto done;
              default:
                TENZIR_UNREACHABLE();
            }
          }
        }
        if (seed_idx == type_index_string) {
          // stringify if possible
          if constexpr (fmt::is_formattable<T>{}) {
            data(fmt::format("{}", v));
            result_index = type_index_string;
            rb.emit_or_throw(diagnostic::warning(
              "The provided schema requested a string, but the "
              "parsed field "
              "was typed data"
              "This is most likely a shortcoming of the parser"));
          }
        }
        rb.emit_or_throw(
          diagnostic::warning("parsed field type (id: {}), does not match the "
                              "type from the schema (id: {}). This is most "
                              "likely a shortcoming of the parser",
                              type_idx, seed_idx));
      }
    [[maybe_unused]] done:
      sig.push_back(static_cast<std::byte>(result_index));
    },
    [&rb](auto&) {
      TENZIR_UNREACHABLE();
      rb.emit_or_throw(diagnostic::error("node_field::append_to_signature "
                                         "UNREACHABLE"));
    },
  };
  return std::visit(visitor, data_);
}

auto node_field::commit_to(tenzir::builder_ref builder,
                           class record_builder& rb, const tenzir::type* seed,
                           bool mark_dead) -> void {
  if (std::holds_alternative<std::string>(data_)) {
    parse(rb, seed);
  }
  const auto visitor = detail::overload{
    [&builder, &rb, seed, mark_dead](node_list& v) {
      const auto ls = caf::get_if<tenzir::list_type>(seed);
      auto l = builder.list();
      if (v.is_alive() or ls) {
        v.commit_to(std::move(l), rb, ls, mark_dead);
      }
    },
    [&builder, &rb, seed, mark_dead](node_record& v) {
      const auto rs = caf::get_if<tenzir::record_type>(seed);
      auto r = builder.record();
      if (v.is_alive() or rs) {
        v.commit_to(std::move(r), rb, rs, mark_dead);
      }
    },
    [&builder, &rb]<non_structured_data_type T>(T& v) {
      auto res = builder.try_data(v);
      if (auto& e = res.error()) {
        rb.emit_or_throw(diagnostic::warning(
          "unexpected error in `record_builder::commit_to(builder_ref)`: {}",
          std::move(e)));
      }
    },
    [](auto&) {
      TENZIR_UNREACHABLE();
    },
  };
  std::visit(visitor, data_);
  if (mark_dead) {
    mark_this_dead();
  }
}

auto node_field::commit_to(tenzir::data& r, class record_builder& rb,
                           const tenzir::type* seed, bool mark_dead) -> void {
  if (std::holds_alternative<std::string>(data_)) {
    parse(rb, seed);
  }
  const auto visitor = detail::overload{
    [&r, &rb, seed, mark_dead](node_list& v) {
      auto ls = caf::get_if<tenzir::list_type>(seed);
      r = tenzir::list{};
      if (v.is_alive() or ls) {
        v.commit_to(caf::get<tenzir::list>(r), rb, ls, mark_dead);
      }
    },
    [&r, &rb, seed, mark_dead](node_record& v) {
      auto rs = caf::get_if<tenzir::record_type>(seed);
      r = tenzir::record{};
      if (v.is_alive() or rs) {
        v.commit_to(caf::get<tenzir::record>(r), rb, rs, mark_dead);
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
  if (mark_dead) {
    mark_this_dead();
  }
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

auto node_list::append_to_signature(signature_type& sig,
                                    class record_builder& rb,
                                    const tenzir::list_type* seed) -> void {
  sig.push_back(list_start_marker);
  if (is_numeric(type_index_) or type_index_ == type_index_numeric_mismatch) {
    // first, we handle the case where the current index is numeric
    // this has special handling, as it will try to convert to the seed type or
    // to double.
    size_t result_index = type_index_;
    if (seed) {
      auto seed_idx = seed->value_type().type_index();
      if (seed_idx != type_index_) {
        if (seed_idx == type_index_double) {
          rb.emit_or_throw(
            diagnostic::warning("numeric type mismatch between list elements "
                                "and the selected schema. A conversion to "
                                "'double' will be performed"));
          goto numeric_mismatch_handling;
        } else {
          goto generic_mismatch_handling;
        }
      }
    } else if (type_index_ == type_index_numeric_mismatch) {
    numeric_mismatch_handling:
      for (auto& e : data_) {
        e.cast_to<double>();
      }
      result_index = type_index_double;
    }
    sig.push_back(static_cast<std::byte>(result_index));
  } else if (is_structural(type_index_)
             or type_index_ == type_index_generic_mismatch) {
  generic_mismatch_handling:
    // this  case also applies when there is any unparsed fields
    // in the generic "mismatch" handling, we need to iterate every element of
    // the list we first append an elements signature, and then check if its
    // identical to the last one we already appended this ensures that if all
    // elements truly have the same signature, the element count of the list no
    // longer matters.
    // Since we need to iterate all elements in the structural case anyways, its
    // equivalent to the generic mismatch case.
    tenzir::type seed_type;
    tenzir::type* seed_type_ptr = nullptr;
    if (seed) {
      seed_type = seed->value_type();
      seed_type_ptr = &seed_type;
    }
    auto last_sig_index = 0;
    if (seed and data_.empty()) {
      node_field sentinel;
      sentinel.state_ = state::sentinel;
      return sentinel.append_to_signature(sig, rb, seed_type_ptr);
    }
    for (auto& v : data_) {
      auto next_sig_index = sig.size();
      v.append_to_signature(sig, rb, seed_type_ptr);
      if (last_sig_index != 0) {
        const auto last_signatures_match = std::ranges::equal(
          std::span{sig.begin() + last_sig_index, sig.begin() + next_sig_index},
          std::span{sig.begin() + next_sig_index, sig.end()});
        if (last_signatures_match) {
          // drop the last appended signature
          sig.erase(sig.begin() + last_sig_index, sig.end());
        }
        last_sig_index = next_sig_index;
      }
    }
    // err = caf::make_error(ec::type_clash, "list element type mismatch");
    // if (seed) {

    // }
  } else {
    // finally the happy case where our predetermined index is usable (its not
    // structural and not a mismatch index)
    sig.push_back(static_cast<std::byte>(type_index_));
  }
  // done:
  sig.push_back(list_end_marker);
}

auto node_list::commit_to(builder_ref r, class record_builder& rb,
                          const tenzir::list_type* seed,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto& v : data_) {
    if (not v.is_alive()) {
      break;
    }
    v.commit_to(r, rb, seed ? &field_seed : nullptr, mark_dead);
  }
  if (mark_dead) {
    type_index_ = type_index_empty;
    mark_this_dead();
  }
}
auto node_list::commit_to(tenzir::list& l, class record_builder& rb,
                          const tenzir::list_type* seed,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto& v : data_) {
    if (v.is_alive()) {
      auto& d = l.emplace_back();
      v.commit_to(d, rb, seed ? &field_seed : nullptr, mark_dead);
    } else {
      break;
    }
  }
  if (mark_dead) {
    type_index_ = type_index_empty;
    mark_this_dead();
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
