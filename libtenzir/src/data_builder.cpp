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

data_builder::data_builder(data_parsing_function parser, diagnostic_handler* dh,
                           bool schema_only, bool parse_schema_fields_only)
  : dh_{dh},
    parser_{std::move(parser)},
    schema_only_{schema_only},
    parse_schema_fields_only_{parse_schema_fields_only} {
  root_.mark_this_dead();
}

auto data_builder::record() -> detail::data_builder::node_record* {
  return root_.record();
}

auto data_builder::list() -> detail::data_builder::node_list* {
  return root_.list();
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
  root_.commit_to(builder_ref{builder}, *this, seed, mark_dead);
}

auto data_builder::materialize(bool mark_dead,
                               const tenzir::type* seed) -> tenzir::data {
  tenzir::data res;
  root_.commit_to(res, *this, seed, mark_dead);
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

auto data_builder::append_signature_to(signature_type& sig,
                                       const tenzir::type* seed) -> void {
  root_.append_to_signature(sig, *this, seed);
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
  auto p = parsers::duration;
  auto parse_res = duration{};
  if (p(s, parse_res)) {
    return data{std::move(parse_res)};
  }
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
auto parse_time(std::string_view s,
                const type& seed) -> detail::data_builder::data_parsing_result {
  auto p = parsers::time;
  auto res = time{};
  if (p(s, res)) {
    return data{std::move(res)};
  }
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

auto basic_seeded_parser(std::string_view s, const tenzir::type& seed)
  -> detail::data_builder::data_parsing_result {
  const auto visitor = detail::overload{
    [&s]<has_parser Type>(
      const Type& t) -> detail::data_builder::data_parsing_result {
      using T = type_to_data_t<Type>;
      auto res = T{};
      auto p = typename parser_for<Type>::type{};
      if (p(s, res)) {
        return tenzir::data{std::move(res)};
      } else {
        return diagnostic::warning("failed to parse value as requested type")
          .hint("value was `{}`, desired type was `{}`", s, t)
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
    []<typename T>(const T& t) -> tenzir::diagnostic {
      return diagnostic::warning("parsing a string as a {} is not supported by "
                                 "the basic parser",
                                 t)
        .done();
    },
  };
  return caf::visit(visitor, seed);
}

auto basic_parser(std::string_view s, const tenzir::type* seed)
  -> detail::data_builder::data_parsing_result {
  if (seed) {
    return basic_seeded_parser(s, *seed);
  }
  if (s.empty()) {
    return {std::string{}};
  }
  auto res = tenzir::data{};

  if (parse_as_data<bool_type, int64_type, uint64_type, double_type, time_type,
                    duration_type, subnet_type, ip_type>(s, res)) {
    return res;
  }
  return {};
}

auto non_number_parser(std::string_view s, const tenzir::type* seed)
  -> detail::data_builder::data_parsing_result {
  if (seed) {
    return data_builder::basic_seeded_parser(s, *seed);
  }
  if (s.empty()) {
    return {std::string{}};
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

auto node_record::try_field(std::string_view name) -> node_object* {
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

auto node_record::field(std::string_view name) -> node_object* {
  mark_this_alive();
  auto* f = try_field(name);
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
      // batch/event quality
      // If performance of the signature compute is ever needed, one may
      // investigate only doing a heuristic compute of the byte signature. Maybe
      // just every 2rd/3rd field another alternative would be to not accurately
      // compute the signatures of nested structural entries.
    }
    const auto key_bytes = as_bytes(k);
    sig.insert(sig.end(), key_bytes.begin(), key_bytes.end());
    field.append_to_signature(sig, rb, nullptr);
  }
  sig.push_back(record_end_marker);
}

auto node_record::commit_to(tenzir::record_ref r, class data_builder& rb,
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

auto node_record::commit_to(tenzir::record& r, class data_builder& rb,
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

auto node_object::null() -> void {
  mark_this_alive();
  value_state_ = value_state_type::has_value;
  data_ = caf::none;
}

auto node_object::data(tenzir::data d) -> void {
  mark_this_alive();
  value_state_ = value_state_type::has_value;
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
                                       "`data_builder::data`",
                                       typeid(T).name()));
    },
  };

  return caf::visit(visitor, d);
}

auto node_object::data_unparsed(std::string_view text) -> void {
  mark_this_alive();
  value_state_ = value_state_type::unparsed;
  data_.emplace<std::string>(text);
}

auto node_object::record() -> node_record* {
  mark_this_alive();
  value_state_ = value_state_type::has_value;
  if (auto* p = get_if<node_record>()) {
    p->mark_this_alive();
    return p;
  }
  return &data_.emplace<node_record>();
}

auto node_object::list() -> node_list* {
  mark_this_alive();
  value_state_ = value_state_type::has_value;
  if (auto* p = get_if<node_list>()) {
    p->mark_this_alive();
    return p;
  }
  return &data_.emplace<node_list>();
}

auto node_object::parse(class data_builder& rb,
                        const tenzir::type* seed) -> void {
  if (value_state_ != value_state_type::unparsed) {
    return;
  }
  if (not is_alive()) {
    return;
  }
  value_state_ = value_state_type::has_value;
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
    data(std::move(value));
    return;
  }
}

auto node_object::try_resolve_nonstructural_field_mismatch(
  class data_builder& rb, const tenzir::type* seed) -> void {
  if (not seed) {
    return;
  }
  if (not is_alive()) {
    return;
  }
  // TODO this should probably be a double visit on `data_` and `*seed`
  const auto visitor = detail::overload{
    [](const auto&) {
      /* noop */
    },
    [](const caf::none_t&) {
      /* noop */
    },
    [&rb, seed, this]<non_structured_data_type T>(const T& v) {
      constexpr static auto type_idx
        = caf::detail::tl_index_of<field_type_list, T>::value;
      if (not seed) {
        return;
      }
      const auto seed_idx = seed->type_index();
      if (type_idx == seed_idx) {
        return;
      }
      if constexpr (is_numeric(type_idx)) {
        // numeric conversion if possible
        if (is_numeric(seed_idx)) {
          switch (seed_idx) {
            case caf::detail::tl_index_of<field_type_list, int64_t>::value: {
              data(static_cast<int64_t>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, uint64_t>::value: {
              data(static_cast<uint64_t>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, double>::value: {
              data(static_cast<double>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, enumeration>::value: {
              data(static_cast<enumeration>(v));
              return;
            }
            default:
              TENZIR_UNREACHABLE();
          }
        } else if (seed_idx
                   == caf::detail::tl_index_of<field_type_list,
                                               duration>::value) {
          auto unit = seed->attribute("unit").value_or("s");
          if constexpr (std::same_as<T, int64_t>) {
            auto res = cast_value(int64_type{}, v, duration_type{}, unit);
            if (res) {
              data(*res);
              return;
            }
          } else if constexpr (std::same_as<T, uint64_t>) {
            auto res = cast_value(uint64_type{}, v, duration_type{}, unit);
            if (res) {
              data(*res);
              return;
            }

          } else if constexpr (std::same_as<T, double>) {
            auto res = cast_value(double_type{}, v, duration_type{}, unit);
            if (res) {
              data(*res);
              return;
            }
          }
        } else if (seed_idx
                   == caf::detail::tl_index_of<field_type_list, time>::value) {
          auto unit = seed->attribute("unit");
          if (not unit) {
            rb.emit_or_throw(
              diagnostic::warning("could not parse value as `{}`", time_type{})
                .note("the read value as a number, but the schema does not "
                      "specify a unit"));
            return;
          }
          if constexpr (std::same_as<T, int64_t>) {
            auto res = cast_value(int64_type{}, v, duration_type{}, *unit);
            if (res) {
              data(time{} + *res);
              return;
            }
          } else if constexpr (std::same_as<T, uint64_t>) {
            auto res = cast_value(uint64_type{}, v, duration_type{}, *unit);
            if (res) {
              data(time{} + *res);
              return;
            }

          } else if constexpr (std::same_as<T, double>) {
            auto res = cast_value(double_type{}, v, duration_type{}, *unit);
            if (res) {
              data(time{} + *res);
              data(*res);
              return;
            }
          }
        }
      } else if (seed_idx == type_index_string) {
        // stringify if possible
        if constexpr (fmt::is_formattable<T>{}) {
          data(fmt::format("{}", v));
          return;
        }
      }
      // TODO this happens in our intentionally "broken" zeek.json event in the
      // test input, where we have `id : 0`, whereas the schema expects `id :
      // zeek.conn_id` The resulting issue is that a protected/preparsed
      // series_builder will reject the value entirely This could be resolved by
      // not preparing any builders in the `multi_series_builder` and instead
      // ensuring that all fields top level are written (as null) on commit
      rb.emit_or_throw(
        diagnostic::warning("parsed field type does not match the type from "
                            "the schema")
          .note("parsed type was `{}`, but the schema expected `{}`",
                type{data_to_type_t<T>{}}.kind(), *seed));
    },
  };
  std::visit(visitor, data_);
}

auto node_object::append_to_signature(signature_type& sig,
                                      class data_builder& rb,
                                      const tenzir::type* seed) -> void {
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
  parse(rb, seed);
  try_resolve_nonstructural_field_mismatch(rb, seed);
  const auto visitor = detail::overload{
    [&sig, &rb, seed, this](node_list& v) {
      const auto* ls = caf::get_if<list_type>(seed);
      if (seed and not ls) {
        rb.emit_or_throw(
          diagnostic::warning("mismatch between event data and expected "
                              "schema")
            .note("schema expected `{}`, but event contained `{}`",
                  seed->kind(), "list"));
        null();
        // FIXME this needs to update the signature in some way
        return;
      }
      if (v.affects_signature() or ls) {
        v.append_to_signature(sig, rb, ls);
      }
    },
    [&sig, &rb, seed, this](node_record& v) {
      const auto* rs = caf::get_if<record_type>(seed);
      if (seed and not rs) {
        rb.emit_or_throw(
          diagnostic::warning("mismatch between event data and expected "
                              "schema")
            .note("schema expected `{}`, but event contained `{}`",
                  seed->kind(), type{record_type{}}.kind()));
        null();
        // FIXME this needs to update the signature in some way
        return;
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
          auto r = record();
          r->append_to_signature(sig, rb, sr);
          this->value_state_ = value_state_type::null;
          return;
        }
        if (auto sl = caf::get_if<tenzir::list_type>(seed)) {
          auto* l = list();
          l->append_to_signature(sig, rb, sl);
          this->value_state_ = value_state_type::null;
          return;
        }
        sig.push_back(static_cast<std::byte>(seed->type_index()));
      } else {
        constexpr static auto type_idx
          = caf::detail::tl_index_of<field_type_list, caf::none_t>::value;
        sig.push_back(static_cast<std::byte>(type_idx));
      }
    },
    [&sig]<non_structured_data_type T>(T&) {
      constexpr static auto type_idx
        = caf::detail::tl_index_of<field_type_list, T>::value;
      sig.push_back(static_cast<std::byte>(type_idx));
    },
    [&rb](auto&) {
      rb.emit_or_throw(diagnostic::error("node_object::append_to_signature "
                                         "UNREACHABLE"));
      TENZIR_UNREACHABLE();
    },
  };
  return std::visit(visitor, data_);
}

auto node_object::commit_to(tenzir::builder_ref builder, class data_builder& rb,
                            const tenzir::type* seed, bool mark_dead) -> void {
  if (rb.schema_only_ and not seed) {
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
  parse(rb, seed);
  try_resolve_nonstructural_field_mismatch(rb, seed);
  const auto visitor = detail::overload{
    [&builder, &rb, seed, mark_dead](node_list& v) {
      if (v.is_alive()) {
        const auto ls = caf::get_if<tenzir::list_type>(seed);
        if (not ls and seed) {
          rb.emit_or_throw(
            diagnostic::warning("mismatch between event data and expected "
                                "schema")
              .note("schema expected `{}`, but event contained `{}`",
                    seed->kind(), "list"));
          builder.null();
          v.mark_this_dead();
          return;
        }
        // Because we ensured that the seed matches above, we can now safely
        // call `builder.list()`
        auto l = builder.list();
        v.commit_to(std::move(l), rb, ls, mark_dead);
      }
    },
    [&builder, &rb, seed, mark_dead](node_record& v) {
      if (v.is_alive()) {
        const auto rs = caf::get_if<tenzir::record_type>(seed);
        if (not rs and seed) {
          rb.emit_or_throw(
            diagnostic::warning("mismatch between event data and expected "
                                "schema")
              .note("schema expected `{}`, but event contained `{}`",
                    seed->kind(), type{record_type{}}.kind()));
          builder.null();
          v.mark_this_dead();
          return;
        }
        // Because we ensured that the seed matches above, we can now safely
        // call `builder.record()`
        auto r = builder.record();
        v.commit_to(std::move(r), rb, rs, mark_dead);
      }
    },
    [&builder, seed, &rb]<non_structured_data_type T>(T& v) {
      auto res = builder.try_data(v);
      if (auto& e = res.error()) {
        rb.emit_or_throw(
          diagnostic::warning("mismatch between event data and "
                              "expected schema")
            .note("schema expected `{}`, but event contains `{}`", seed->kind(),
                  type{data_to_type_t<T>{}}.kind())
            .note("{}", e));
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
                            const tenzir::type* seed, bool mark_dead) -> void {
  if (rb.schema_only_ and not seed) {
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
  parse(rb, seed);
  const auto visitor = detail::overload{
    [&r, &rb, seed, mark_dead](node_list& v) {
      if (v.is_alive()) {
        auto ls = caf::get_if<tenzir::list_type>(seed);
        r = tenzir::list{};
        v.commit_to(caf::get<tenzir::list>(r), rb, ls, mark_dead);
      }
    },
    [&r, &rb, seed, mark_dead](node_record& v) {
      if (v.is_alive()) {
        auto rs = caf::get_if<tenzir::record_type>(seed);
        r = tenzir::record{};
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
    value_state_ = value_state_type::null;
  }
}

auto node_object::clear() -> void {
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

auto node_list::find_free() -> node_object* {
  for (auto& value : data_) {
    if (not value.is_alive()) {
      return &value;
    }
  }
  return nullptr;
}

auto node_list::back() -> node_object& {
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
      free->mark_this_alive();
      free->value_state_ = node_object::value_state_type::has_value;
      r->mark_this_alive();
      return r;
    } else {
      free->value_state_ = node_object::value_state_type::has_value;
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
    if (auto* l = free->get_if<node_list>()) {
      l->mark_this_alive();
      free->value_state_ = node_object::value_state_type::has_value;
      free->mark_this_alive();
      return l;
    } else {
      free->value_state_ = node_object::value_state_type::has_value;
      return &free->data_.emplace<node_list>();
    }
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on list size reached.");
    return data_.emplace_back().list();
  }
}

auto node_list::append_to_signature(signature_type& sig, class data_builder& rb,
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
      node_object sentinel;
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

auto node_list::commit_to(builder_ref r, class data_builder& rb,
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
auto node_list::commit_to(tenzir::list& l, class data_builder& rb,
                          const tenzir::list_type* seed,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto& v : data_) {
    if (not v.is_alive()) {
      break;
    }
    auto& d = l.emplace_back();
    v.commit_to(d, rb, seed ? &field_seed : nullptr, mark_dead);
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

} // namespace detail::data_builder
} // namespace tenzir
