//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once
#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tsl/robin_map.h"

#include <caf/detail/type_list.hpp>
#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace tenzir {
class multi_series_builder;
class record_builder;
namespace detail::record_builder {

struct data_parsing_result {
  std::optional<tenzir::data> data;
  std::optional<tenzir::diagnostic> diagnostic;

  data_parsing_result() = default;
  data_parsing_result(tenzir::data data_) : data{std::move(data_)} {};

  data_parsing_result(tenzir::diagnostic diag_)
    : diagnostic{std::move(diag_)} {};

  data_parsing_result(tenzir::data data_, tenzir::diagnostic diag_)
    : data{std::move(data_)}, diagnostic{std::move(diag_)} {};
};

template <typename P>
concept data_parsing_function
  = requires(P parser, std::string_view str, const tenzir::type* seed) {
      { parser(str, seed) } -> std::same_as<data_parsing_result>;
    };

class node_record;
class node_field;
class node_list;

struct map_dummy {};
struct pattern_dummy {};
struct enriched_dummy {};
// The indices in this MUST line up with the tenzir type indices, hence the
// dummies
// clang-format off
using field_type_list = caf::detail::type_list<
  caf::none_t, 
  bool, 
  int64_t, 
  uint64_t,
  double, 
  duration, 
  time, 
  std::string,
  pattern_dummy,
  ip, 
  subnet, 
  enumeration, 
  node_list,
  map_dummy, 
  node_record, 
  enriched_dummy,
  blob
>;
// clang-format on

template <typename T>
concept non_structured_data_type
  = caf::detail::tl_contains<field_type_list, T>::value
    and not caf::detail::tl_contains<
      caf::detail::type_list<node_record, node_list, pattern_dummy, map_dummy,
                             enriched_dummy>,
      T>::value;

template <typename T>
concept non_structured_type_type
  = caf::detail::tl_contains<concrete_types, T>::value
    and not caf::detail::tl_contains<
      caf::detail::type_list<record_type, list_type, legacy_pattern_type,
                             map_type>,
      T>::value;

template <typename T>
concept numeric_data_type
  = non_structured_data_type<T>
    and (std::same_as<T, uint64_type> or std::same_as<T, int64_type>
         or std::same_as<T, double_type>);

// clang-format off
template <typename T>
concept unsupported_types
  =  caf::detail::tl_contains<
      caf::detail::type_list<
        tenzir::map_type, tenzir::map, map_dummy,
        tenzir::legacy_pattern_type, tenzir::pattern, pattern_dummy,
        enriched_dummy
      >,
      T>::value;
// clang-format on

using signature_type = std::vector<std::byte>;
// outer map needs iterator stability at the moment
// TODO maybe it can be made faster if we dont use iterator stability
// and instead re-query for `seed_it`
using field_type_lookup_map = tsl::robin_map<std::string, tenzir::type>;
using schema_type_lookup_map
  = std::unordered_map<tenzir::record_type, field_type_lookup_map>;

constexpr static size_t type_index_empty
  = caf::detail::tl_size<field_type_list>::value;
constexpr static size_t type_index_numeric_mismatch
  = caf::detail::tl_size<field_type_list>::value + 1;
constexpr static size_t type_index_generic_mismatch
  = caf::detail::tl_size<field_type_list>::value + 2;
constexpr static size_t type_index_string
  = caf::detail::tl_index_of<field_type_list, std::string>::value;
constexpr static size_t type_index_double
  = caf::detail::tl_index_of<field_type_list, double>::value;
constexpr static size_t type_index_list
  = caf::detail::tl_index_of<field_type_list, node_list>::value;
constexpr static size_t type_index_record
  = caf::detail::tl_index_of<field_type_list, node_record>::value;

static inline constexpr auto is_structural(size_t idx) -> bool {
  switch (idx) {
    case caf::detail::tl_index_of<field_type_list, node_list>::value:
    case caf::detail::tl_index_of<field_type_list, node_record>::value:
      return true;
    default:
      return false;
  }
}

static inline constexpr auto is_numeric(size_t idx) -> bool {
  switch (idx) {
    case caf::detail::tl_index_of<field_type_list, int64_t>::value:
    case caf::detail::tl_index_of<field_type_list, uint64_t>::value:
    case caf::detail::tl_index_of<field_type_list, double>::value:
    case caf::detail::tl_index_of<field_type_list, enumeration>::value:
      return true;
    default:
      return false;
  }
}

static inline auto is_null(size_t idx) -> bool {
  return idx == caf::detail::tl_index_of<field_type_list, caf::none_t>::value;
}

static inline auto
update_type_index(size_t& old_index, size_t new_index) -> void {
  if (old_index == type_index_generic_mismatch) {
    return;
  }
  if (old_index == new_index) {
    return;
  }
  if (is_null(new_index)) {
    return;
  }
  if (old_index == type_index_empty) {
    old_index = new_index;
    return;
  }
  if (is_null(old_index)) {
    old_index = new_index;
    return;
  }
  if ((old_index == type_index_numeric_mismatch or is_numeric(old_index))
      and is_numeric(new_index)) {
    old_index = type_index_numeric_mismatch;
    return;
  }
  old_index = type_index_generic_mismatch;
}

enum class state { alive, sentinel, dead };

class node_base {
  friend class node_record;
  friend class node_field;
  friend class node_list;
  friend class ::tenzir::record_builder;

private:
  auto mark_this_relevant() -> void {
    if (state_ != state::alive) {
      state_ = state::sentinel;
    }
  }
  auto mark_this_dead() -> void {
    state_ = state::dead;
  }
  auto mark_this_alive() -> void {
    state_ = state::alive;
  }
  auto is_dead() const -> bool;
  auto is_alive() const -> bool;
  auto affects_signature() const -> bool;
  state state_ = state::alive;
};

class node_record : public node_base {
  friend class node_list;
  friend class node_field;
  friend class ::tenzir::record_builder;

public:
  /// reserves storage for at least N elements in the record.
  /// this function can be used to get temporary pointer stability on the
  /// records elements
  auto reserve(size_t N) -> void;
  /// adds a field to the record.
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  [[nodiscard]] auto field(std::string_view name) -> node_field*;

private:
  // tries to get a field with the given name. Does not affect any field state
  auto try_field(std::string_view name) -> node_field*;
  // does lookup of a (nested( key
  auto at(std::string_view key) -> node_field*;
  // writes the record into a series builder
  auto
  commit_to(tenzir::record_ref r, class record_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;
  auto
  commit_to(tenzir::record& r, class record_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;
  // append the signature of this record to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class record_builder& rb,
                           const tenzir::record_type* seed) -> void;
  // clears the record by marking everything as dead
  auto clear() -> void;

  // record entry. contains a string for the key and a field
  // its defined out of line because node_field cannot be defined at this point
  struct entry_type;
  // this stores added fields in order of their appearance
  // this order is used for committing to the series builder
  // Using the appearance order to commit, ensures that fields outside of a
  // possible seed schema retain their order from first appearance The order of
  // fields in a seed/selector on the other hand is then practically ensured
  // because the multi_series_builder first seeds the respective series_builder
  std::vector<entry_type> data_;
  // this is a SORTED key -> index map. this is used for signature computation
  // if this map is not sorted, the signature computation algorithm breaks
  // flat_map<std::string, size_t> lookup_;
  flat_map<std::string, size_t> lookup_;
};

class node_list : public node_base {
  friend class node_record;
  friend class node_field;

public:
  /// reserves storage for at least N elements in the record.
  /// this function can be used to get temporary pointer stability on the
  /// records elements
  auto reserve(size_t N) -> void;
  /// appends a new typed value to this list
  /// if its type mismatches with the seed during the later parsing/signature
  /// computation, an error is returned
  template <non_structured_data_type T>
  auto data(T data) -> void;
  /// unpacks the tenzir::data into a new element at the end of th list
  auto data(tenzir::data) -> void;
  /// adds an unparsed data value to this field. It is later parsed during the
  /// signature computation step
  auto data_unparsed(std::string_view) -> void;
  /// adds a null value to the list
  auto null() -> void;
  /// adds a new record to the list
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  [[nodiscard]] auto record() -> node_record*;
  /// adds a new list to the list
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  [[nodiscard]] auto list() -> node_list*;

  auto combined_index() const -> size_t {
    return type_index_;
  }

private:
  /// finds an element marked as dead. This is part of the reallocation
  /// optimization.
  auto find_free() -> node_field*;
  auto back() -> node_field&;

  auto update_new_structural_signature() -> void;

  // writes the list into a series builder
  auto commit_to(tenzir::builder_ref r, class record_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;
  auto commit_to(tenzir::list& r, class record_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;
  // append the signature of this list to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class record_builder& rb,
                           const tenzir::list_type* seed) -> void;
  auto clear() -> void;

  size_t type_index_ = type_index_empty;
  signature_type current_structural_signature_;
  signature_type new_structural_signature_;
  std::vector<node_field> data_;
};

class node_field : public node_base {
  friend class node_record;
  friend class node_list;
  friend struct node_record::entry_type;
  friend class ::tenzir::record_builder;
  friend class ::tenzir::multi_series_builder;

public:
  /// sets this field to a parsed, typed data value
  /// if its type mismatches with the seed during the later parsing/signature
  /// computation, an error is returned
  template <non_structured_data_type T>
  auto data(T data) -> void;
  /// unpacks the tenzir::data into this field
  auto data(tenzir::data) -> void;
  /// adds an unparsed data value to this field. It is later parsed during the
  /// signature computation step
  auto data_unparsed(std::string_view raw_text) -> void;
  auto null() -> void;
  [[nodiscard]] auto record() -> node_record*;
  [[nodiscard]] auto list() -> node_list*;

  node_field() : data_{std::in_place_type<caf::none_t>} {
  }
  template <non_structured_data_type T>
  node_field(T data) : data_{std::in_place_type<T>, data} {
  }

private:
  auto current_index() const -> size_t {
    return data_.index();
  }
  template <typename T>
  auto get_if() -> T* {
    return std::get_if<T>(&data_);
  }
  /// tries to static_cast the held value to T.
  /// @returns whether the cast was performed
  template <typename T>
  auto cast_to() -> bool {
    const auto visitor = detail::overload{
      [this]<typename Current>(const Current& v) -> bool
        requires requires(Current c) { static_cast<T>(c); }
      {
        data(static_cast<T>(v));
        return true;
      },
      [](const auto&) -> bool {
        return false;
      },
      };
    return std::visit(visitor, data_);
  }
  auto try_resolve_nonstructural_field_mismatch( class record_builder& rb, const tenzir::type* seed ) -> void;
  /// parses any unparsed fields using `parser`, potentially providing a
  /// seed/schema to the parser
  auto parse(class record_builder& rb, const tenzir::type* seed) -> void;
  // append the signature of this field to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class record_builder& rb,
                           const tenzir::type* seed) -> void;
  // writes the field into a series builder
  auto commit_to(tenzir::builder_ref r, class record_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;
  auto commit_to(tenzir::data& r, class record_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;
  auto clear() -> void;

  // clang-format off
  using field_variant_type = caf::detail::tl_apply_t<
   field_type_list,
    std::variant
  >;
  // clang-format on

  field_variant_type data_;

  enum class value_state_type {
    has_value, unparsed, null
  };
  // this is the state of the contained value. This exists in case somebody calls 
  // `record.field("key")` but never inserts any data into the field
  // this is distinctly different from a node not being `alive`, 
  // which only happens as a result of internal storage reuse.
  value_state_type value_state_ = value_state_type::null;
};

struct node_record::entry_type {
  std::string key;
  node_field value;

  entry_type(std::string_view name) : key{name} {
  }
};

constexpr static std::byte record_start_marker{0xfa};
constexpr static std::byte record_end_marker{0xfb};

constexpr static std::byte list_start_marker{0xfc};
constexpr static std::byte list_end_marker{0xfd};

/// a very basic parser that simply uses `tenzir::parsers` under the hood.
/// this parser does not support the seed pointing to a structural type
auto basic_parser(std::string_view s, const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result;

auto non_number_parser(std::string_view s, const tenzir::type* seed)
  -> detail::record_builder::data_parsing_result;

/// a very basic parser that only supports parsing based on a seed
/// uses the `tenzir::parser` s under the hood.
/// this parser does not support the seed pointing to a structural type
auto basic_seeded_parser(std::string_view s, const tenzir::type& seed)
  -> detail::record_builder::data_parsing_result;

} // namespace detail::record_builder

class record_builder {
  friend class detail::record_builder::node_list;
  friend class detail::record_builder::node_record;
  friend class detail::record_builder::node_field;

public:
  template <detail::record_builder::data_parsing_function Parser
            = decltype(detail::record_builder::basic_parser)>
  record_builder(Parser parser = detail::record_builder::basic_parser,
                 diagnostic_handler* dh = nullptr, bool schema_only = false,
                 bool parse_schema_fields_only = false)
    : dh_{dh},
      parser_{std::move(parser)},
      schema_only_{schema_only},
      parse_schema_fields_only_{parse_schema_fields_only} {
    root_.mark_this_dead();
  }

  // accesses the currently building record
  [[nodiscard]] auto record() -> detail::record_builder::node_record*;

  [[nodiscard]] auto has_elements() -> bool {
    return root_.is_alive();
  }

  // are not removed, only possible conflict resolved towards string
  auto seed(std::optional<tenzir::type> seed) -> void;

  /// tries to find a field with the given (nested) key
  [[nodiscard]] auto
  find_field_raw(std::string_view key) -> detail::record_builder::node_field*;

  /// tries to find a field with the given (nested) key for a data type
  // template <detail::record_builder::non_structured_data_type T>
  // [[nodiscard]] auto find_value_typed(std::string_view key) -> T*;

  using signature_type = typename detail::record_builder::signature_type;
  /// computes the "signature" of the currently built record.
  auto append_signature_to(signature_type&, const tenzir::type* seed) -> void;

  /// clears the builder
  void clear();
  /// clears the builder and frees all memory
  void free();

  /// materializes the currently build record
  /// @param mark_dead whether to mark nodes in the record builder as dead
  [[nodiscard]] auto
  materialize(bool mark_dead = true, const tenzir::type* seed
                                     = nullptr) -> tenzir::record;
  /// commits the current record into the series builder
  /// @param mark_dead whether to mark nodes in the record builder as dead
  auto commit_to(series_builder&, bool mark_dead = true,
                 const tenzir::type* seed = nullptr) -> void;

private:
/// tries to lookup the type `r` in the type lookup map, 
/// and potentially writes creates sentinel fields in `apply` 
/// if they dont exist in the record yet
  auto lookup_record_fields(const tenzir::record_type* r,
                            detail::record_builder::node_record* apply)
    -> const detail::record_builder::field_type_lookup_map*;

  detail::record_builder::node_record root_;
  detail::record_builder::schema_type_lookup_map schema_type_lookup_;
  diagnostic_handler* dh_;

public:
  std::function<detail::record_builder::data_parsing_result(
    std::string_view, const tenzir::type*)>
    parser_;

private:
  bool schema_only_;
  bool parse_schema_fields_only_;

  auto emit_or_throw(tenzir::diagnostic&& diag) -> void;
  auto emit_or_throw(tenzir::diagnostic_builder&& builder) -> void;
};

namespace detail::record_builder {
template <non_structured_data_type T>
auto node_field::data(T data) -> void {
  mark_this_alive();
  value_state_ = value_state_type::has_value;
  data_.emplace<T>(std::move(data));
}

template <non_structured_data_type T>
auto node_list::data(T data) -> void {
  mark_this_alive();
  if (auto* free = find_free()) {
    free->data(std::move(data));
    update_type_index(type_index_, free->current_index());
  } else {
    TENZIR_ASSERT(data_.size() <= 20'000, "Upper limit on list size reached.");
    data_.emplace_back(std::move(data));
    data_.back().value_state_ = node_field::value_state_type::has_value;
    update_type_index(type_index_, data_.back().current_index());
  }
}
} // namespace detail::record_builder
} // namespace tenzir
