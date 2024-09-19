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

auto data_builder::emit_mismatch_warning(const type& value_type,
                                         const type& seed_type) -> void {
  emit_or_throw(diagnostic::warning("parsed field contains `{}`, but the "
                                    "schema expects `{}`",
                                    value_type.kind(), seed_type.kind()));
}

auto data_builder::emit_mismatch_warning(std::string_view value_type,
                                         const type& seed_type) -> void {
  emit_or_throw(diagnostic::warning("parsed field contains `{}`, but the "
                                    "schema expects `{}`",
                                    value_type, seed_type.kind()));
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
auto parse_time(std::string_view s,
                const type& seed) -> detail::data_builder::data_parsing_result {
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
      return diagnostic::warning("schema expected `{}`, but the input "
                                 "contained a string",
                                 tenzir::type{t}.kind())
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
    return {};
  }
  auto res = best_effort_parser(s);
  if (res) {
    return std::move(*res);
  } else {
    return {};
  }
}

auto non_number_parser(std::string_view s, const tenzir::type* seed)
  -> detail::data_builder::data_parsing_result {
  if (seed) {
    return data_builder::basic_seeded_parser(s, *seed);
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
    = caf::detail::tl_index_of<field_type_list, Field>::value;
  constexpr static auto tenzir_type_index = Tenzir_Type::type_index;

  static_assert(index_in_field == tenzir_type_index);

  using type = void;
};

static_assert(caf::detail::tl_size<field_type_list>::value
                == caf::detail::tl_size<data::types>::value + 1,
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
    []<unsupported_type T>(T&) {
      TENZIR_ASSERT(false, fmt::format("Unexpected type \"{}\" in "
                                       "`data_builder::data`",
                                       typeid(T).name()));
    },
  };

  return caf::visit(visitor, d);
}

auto node_object::data_unparsed(std::string text) -> void {
  mark_this_alive();
  value_state_ = value_state_type::unparsed;
  data_.emplace<std::string>(std::move(text));
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
  // The double visit currently cant be done easily, because its std::variant &
  // caf::variant. (`data_`vs `tenzir::type`).
  // Below is a nice implementation that I wrote before realizing
  // that I really dont want to deal with turning the `data_` member into a
  // caf::variant. Once we update CAF, this will be easy to replace.
  // Be aware that this double visit may be out of date with the behaviour
  // below:
  // * its at the very least not warning for double -> int conversions that
  // loose precision const auto visitor2 = detail::overload{
  //   // fallback
  //   [&rb,seed]<non_structured_data_type T, typename S>(const T&, const S& s)
  //   {
  //     rb.emit_mismatch_warning( type{data_to_type_t<T>{}}, *seed );
  //   },
  //   // generic fallback
  //   []( const auto&, const auto& ){
  //     /* noop */
  //   },
  //   // null -> anything
  //   [](const caf::none_t&, const auto&) {
  //     /* noop */
  //   },
  //   // numeric -> double
  //   [this]<numeric_data_type T>(const T& value, const double_type&) {
  //     data(static_cast<double>(value));
  //   },
  //   // int -> uint
  //   [this](const int64_t value, const uint64_type&) {
  //     if (value < 0) {
  //       null();
  //     } else {
  //       data(static_cast<uint64_t>(value));
  //     }
  //   },
  //   // uint -> int
  //   [this](const uint64_t& value, const int64_type&) {
  //     if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
  //     {
  //       null();
  //     } else {
  //       data(static_cast<int64_t>(value));
  //     }
  //   },
  //   // numeric -> duration
  //   [this,seed]<numeric_data_type T>(const T& value, const duration_type&) {
  //     auto unit = seed->attribute("unit").value_or("s");
  //     auto res = cast_value(data_to_type_t<T>{}, value, duration_type{},
  //     unit); if (res) {
  //       data(*res);
  //       return;
  //     }
  //   },
  //   // numeric -> time
  //   [this, seed, &rb]<numeric_data_type T>(const T& value, const time_type&)
  //   {
  //     auto unit = seed->attribute("unit");
  //     if (not unit) {
  //       rb.emit_or_throw(
  //         diagnostic::warning("could not parse value as `{}`", time_type{})
  //           .note("the read value as a number, but the schema does not "
  //                 "specify a unit"));
  //       return;
  //     }
  //     auto res = cast_value(data_to_type_t<T>{}, value, duration_type{},
  //     *unit); if (res) {
  //       data(time{} + *res);
  //       return;
  //     }
  //   },
  //   // anything -> string
  //   [this]<fmt_formattable T>(const T& value, const string_type&) {
  //     data(fmt::format("{}", value));
  //   },
  // };
  // caf::visit( visitor2, data_, *seed );
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
      const auto seed_idx = seed->type_index();
      if (type_idx == seed_idx) {
        return;
      }
      if constexpr (is_numeric(type_idx)) {
        // numeric conversion if possible
        if (is_numeric(seed_idx)) {
          switch (seed_idx) {
            case caf::detail::tl_index_of<field_type_list, int64_t>::value: {
              if constexpr (std::same_as<uint64_t, T>) {
                if (v > static_cast<uint64_t>(
                      std::numeric_limits<int64_t>::max())) {
                  null();
                  rb.emit_or_throw(diagnostic::warning("value is out of range "
                                                       "for expected type")
                                     .note("value `{}` does not fit into `{}`",
                                           v, seed->kind()));
                  return;
                }
              } else if constexpr (std::same_as<double, T>) {
                if (static_cast<double>(static_cast<int64_t>(v)) == v) {
                  rb.emit_or_throw(diagnostic::warning("fractional value where "
                                                       "integral was expected")
                                     .note("value `{}` looses precision when "
                                           "converted to `{}`",
                                           v, seed->kind()));
                }
              }
              data(static_cast<int64_t>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, uint64_t>::value: {
              if (v < 0) {
                null();
                rb.emit_or_throw(diagnostic::warning("value is out of range "
                                                     "for expected type")
                                   .note("value `{}` does not fit into `{}`", v,
                                         seed->kind()));
                return;
              }
              if constexpr (std::same_as<double, T>) {
                if (static_cast<double>(static_cast<int64_t>(v)) == v) {
                  rb.emit_or_throw(diagnostic::warning("fractional value where "
                                                       "integral was expected")
                                     .note("value `{}` looses precision when "
                                           "converted to `{}`",
                                           v, seed->kind()));
                }
              }
              data(static_cast<uint64_t>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, double>::value: {
              data(static_cast<double>(v));
              return;
            }
            case caf::detail::tl_index_of<field_type_list, enumeration>::value: {
              if (v < 0
                  or v > static_cast<T>(
                       std::numeric_limits<enumeration>::max())) {
                rb.emit_or_throw(diagnostic::warning("value is out of range "
                                                     "for expected type")
                                   .note("value `{}` does not fit into `{}`", v,
                                         seed->kind()));
                null();
                return;
              }
              auto enum_t = caf::get_if<enumeration_type>(seed);
              TENZIR_ASSERT(enum_t);
              if (enum_t->field(static_cast<uint32_t>(v)).empty()) {
                null();
                rb.emit_or_throw(
                  diagnostic::warning("unknown integral enumeration value")
                    .note("value `{}` is not defined for `{}`", v, *enum_t));
              }
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
          auto res = cast_value(data_to_type_t<T>{}, v, duration_type{}, unit);
          if (res) {
            data(*res);
            return;
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
          auto res = cast_value(data_to_type_t<T>{}, v, duration_type{}, *unit);
          if (res) {
            data(time{} + *res);
            return;
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
      // test input, where we have field `id : 0`, whereas the schema expects
      // `id : zeek.conn_id` The resulting issue is that a protected/preparsed
      // series_builder will reject the value. This could be resolved by
      // not preparing any builders in the `multi_series_builder` and instead
      // ensuring that all fields top level are written (as null) on commit.
      // This is a non-trivial effort though and should be considered as a
      // follow-up to precise parsing.
      rb.emit_mismatch_warning(type{data_to_type_t<T>{}}, *seed);
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
    // Sentinel structural types get handled by the regular visit below
  }
  parse(rb, seed);
  try_resolve_nonstructural_field_mismatch(rb, seed);
  // This lambda handles the case where the node is either null, or its stored
  // type mismatches the given seed
  // In case of a mismatch, the node is nulled out and the visit is rerun.
  // Rerunning the visit then runs into the `caf::none_t` case, which correctly
  // re-creates the nodes contents according to the seed.
  const auto visitor = detail::overload{
    [&sig, &rb, seed, this](node_list& v) {
      const auto* ls = caf::get_if<list_type>(seed);
      if (seed and not ls) {
        rb.emit_mismatch_warning("list", *seed);
        null();
        return false;
      }
      if (v.affects_signature() or ls) {
        v.append_to_signature(sig, rb, ls);
      }
      return true;
    },
    [&sig, &rb, seed, this](node_record& v) {
      const auto* rs = caf::get_if<record_type>(seed);
      if (seed and not rs) {
        rb.emit_mismatch_warning(type{record_type{}}, *seed);
        null();
        return false;
      }
      if (v.affects_signature() or rs) {
        v.append_to_signature(sig, rb, rs);
      }
      return true;
    },
    [&sig, &rb, seed, this](caf::none_t&) {
      // none could be the result of pre-seeding or being built with a true
      // null via the API for the first case we need to ensure we continue
      // doing seeding if we have a seed
      if (seed) {
        if (auto sr = caf::get_if<tenzir::record_type>(seed)) {
          auto r = record();
          r->append_to_signature(sig, rb, sr);
          r->state_ = state::sentinel;
          this->value_state_ = value_state_type::null;
          return true;
        }
        if (auto sl = caf::get_if<tenzir::list_type>(seed)) {
          auto* l = list();
          l->append_to_signature(sig, rb, sl);
          l->state_ = state::sentinel;
          this->value_state_ = value_state_type::null;
          return true;
        }
        sig.push_back(static_cast<std::byte>(seed->type_index()));
        return true;
      } else {
        constexpr static auto type_idx
          = caf::detail::tl_index_of<field_type_list, caf::none_t>::value;
        sig.push_back(static_cast<std::byte>(type_idx));
        return true;
      }
    },
    [&sig, seed, this]<non_structured_data_type T>(T&) {
      constexpr static auto type_idx
        = caf::detail::tl_index_of<field_type_list, T>::value;
      if (seed) {
        const auto seed_idx = seed->type_index();
        if (seed_idx != type_idx) {
          null();
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
      if (v.is_dead()) {
        return;
      }
      const auto ls = caf::get_if<tenzir::list_type>(seed);
      if (not ls and seed) {
        rb.emit_mismatch_warning("list", *seed);
        builder.null();
        v.mark_this_dead();
        return;
      }
      // Because we ensured that the seed matches above, we can now safely
      // call `builder.list()`
      auto l = builder.list();
      v.commit_to(std::move(l), rb, ls, mark_dead);
    },
    [&builder, &rb, seed, mark_dead](node_record& v) {
      if (v.is_dead()) {
        return;
      }
      const auto rs = caf::get_if<tenzir::record_type>(seed);
      if (not rs and seed) {
        rb.emit_mismatch_warning(type{record_type{}}, *seed);
        builder.null();
        v.mark_this_dead();
        return;
      }
      // Because we ensured that the seed matches above, we can now safely
      // call `builder.record()`
      auto r = builder.record();
      v.commit_to(std::move(r), rb, rs, mark_dead);
    },
    [&builder, seed, &rb]<non_structured_data_type T>(T& v) {
      auto res = builder.try_data(v);
      if (auto& e = res.error()) {
        if (tenzir::ec{e.code()} == ec::type_clash) {
          rb.emit_mismatch_warning(type{data_to_type_t<T>{}}, *seed);
        } else {
          rb.emit_or_throw(
            diagnostic::warning("issue writing data into builder")
              .note("{}", e));
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
  try_resolve_nonstructural_field_mismatch(rb, seed);
  const auto visitor = detail::overload{
    [&r, &rb, seed, mark_dead](node_list& v) {
      if (v.is_dead()) {
        return;
      }
      const auto ls = caf::get_if<tenzir::list_type>(seed);
      if (not ls and seed) {
        rb.emit_mismatch_warning("list", *seed);
        r = caf::none;
        v.mark_this_dead();
        return;
      }
      r = tenzir::list{};
      v.commit_to(caf::get<tenzir::list>(r), rb, ls, mark_dead);
    },
    [&r, &rb, seed, mark_dead](node_record& v) {
      if (v.is_dead()) {
        return;
      }
      const auto rs = caf::get_if<tenzir::record_type>(seed);
      if (not rs and seed) {
        rb.emit_mismatch_warning(type{record_type{}}, *seed);
        r = caf::none;
        v.mark_this_dead();
        return;
      }
      r = tenzir::record{};
      v.commit_to(caf::get<tenzir::record>(r), rb, rs, mark_dead);
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

auto node_list::try_resurrect_dead() -> node_object* {
  if (first_dead_idx_ >= data_.size()) {
    return nullptr;
  }
  auto& value = data_[first_dead_idx_];
  TENZIR_ASSERT(not value.is_alive());
  ++first_dead_idx_;
  return &value;
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

auto node_list::data_unparsed(std::string text) -> void {
  mark_this_alive();
  type_index_ = type_index_generic_mismatch;
  if (auto* free = try_resurrect_dead()) {
    free->data_unparsed(std::move(text));
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on list size reached.");
    ++first_dead_idx_;
    data_.emplace_back().data_unparsed(std::move(text));
  }
}

auto node_list::null() -> void {
  return data(caf::none);
}

auto node_list::record() -> node_record* {
  mark_this_alive();
  update_type_index(type_index_, type_index_record);
  if (auto* free = try_resurrect_dead()) {
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
    ++first_dead_idx_;
    return data_.emplace_back().record();
  }
}

auto node_list::list() -> node_list* {
  mark_this_alive();
  update_type_index(type_index_, type_index_list);
  if (auto* free = try_resurrect_dead()) {
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
    ++first_dead_idx_;
    return data_.emplace_back().list();
  }
}

auto node_list::append_to_signature(signature_type& sig, class data_builder& rb,
                                    const tenzir::list_type* seed) -> void {
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
  } else if (seed) {
    node_object sentinel;
    sentinel.state_ = state::sentinel;
    sentinel.append_to_signature(sig, rb, seed_type_ptr);
  } else if (not is_structural(type_index_)
             and type_index_ < type_index_empty) {
    sig.push_back(static_cast<std::byte>(type_index_));
  } else if (not seed and type_index_ == type_index_numeric_mismatch) {
    auto negative = size_t{0};
    auto large_positive = size_t{0};
    auto floating = size_t{0};
    constexpr static auto idx_int
      = caf::detail::tl_index_of<field_type_list, int64_t>::value;
    constexpr static auto idx_uint
      = caf::detail::tl_index_of<field_type_list, uint64_t>::value;
    constexpr static auto idx_double
      = caf::detail::tl_index_of<field_type_list, double>::value;
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
    for (auto& v : alive_elements()) {
      if (v.current_index() == type_index_list) {
        if (not has_list) {
          sig.push_back(list_start_marker);
          sig.push_back(list_end_marker);
          has_list = true;
        }
        continue;
      }
      if (v.current_index() == type_index_record) {
        if (not has_record) {
          sig.push_back(record_start_marker);
          sig.push_back(record_end_marker);
          has_record = true;
        }
        continue;
      }
      if (v.current_index() == type_index_null) {
        continue;
      }
      auto curr_sig_start_index = sig.size();
      v.append_to_signature(sig, rb, seed_type_ptr);
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
      ///
      TENZIR_ASSERT(curr_sig.size() == 1);
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
        diagnostic::warning("type mismatch between list elements"));
    }
  }
  sig.push_back(list_end_marker);
}

auto node_list::commit_to(builder_ref r, class data_builder& rb,
                          const tenzir::list_type* seed,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto& v : alive_elements()) {
    v.commit_to(r, rb, seed ? &field_seed : nullptr, mark_dead);
  }
  if (mark_dead) {
    type_index_ = type_index_empty;
    first_dead_idx_ = 0;
    mark_this_dead();
  }
}
auto node_list::commit_to(tenzir::list& l, class data_builder& rb,
                          const tenzir::list_type* seed,
                          bool mark_dead) -> void {
  auto field_seed = seed ? seed->value_type() : tenzir::type{};
  for (auto& v : alive_elements()) {
    auto& d = l.emplace_back();
    v.commit_to(d, rb, seed ? &field_seed : nullptr, mark_dead);
  }
  if (mark_dead) {
    type_index_ = type_index_empty;
    first_dead_idx_ = 0;
    mark_this_dead();
  }
}

auto node_list::alive_elements() -> std::span<node_object> {
  return {data_.begin(), data_.begin() + first_dead_idx_};
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
