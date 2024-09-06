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
class data_builder;
namespace detail::data_builder {

/// Contains the result of a parser used in the `data_builder`.
/// If the `data` member optional is empty, that means that the value did not
/// parse as any type and should remain a string.
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

/// A very basic parser that simply uses `tenzir::parsers` under the hood.
/// If the returned optional is empty, that means that the value did not parse as
/// any type and should remain a string.
auto best_effort_parser(std::string_view s) -> std::optional<data>;

/// A very basic parser that only parses the string according to the `seed`
/// type. This parser does not support the seed pointing to a structural type
auto basic_seeded_parser(std::string_view s, const tenzir::type& seed)
  -> detail::data_builder::data_parsing_result;

/// A very basic parser that simply uses `tenzir::parsers` under the hood.
/// This parser does not support the seed pointing to a structural type
auto basic_parser(std::string_view s, const tenzir::type* seed)
  -> detail::data_builder::data_parsing_result;

/// A very basic parser that simply uses `tenzir::parsers` under the hood.
/// This parser will not attempt to parse strings as numeric types.
/// Its used for input formats that already are inherently aware of numbers,
/// such as JSON or YAML.
/// This parser does not support the seed pointing to a structural type
auto non_number_parser(std::string_view s, const tenzir::type* seed)
  -> detail::data_builder::data_parsing_result;

class node_record;
class node_object;
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
protected:
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
  friend class node_object;
  friend class ::tenzir::data_builder;

public:
  /// @brief Reserves storage for at least N elements in the record.
  /// this function can be used to get temporary pointer stability on the
  /// records elements
  auto reserve(size_t N) -> void;
  /// @brief Adds a field to the record.
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  [[nodiscard]] auto field(std::string_view name) -> node_object*;

private:
  // tries to get a field with the given name. Does not affect any field state
  auto try_field(std::string_view name) -> node_object*;
  // does lookup of a (nested( key
  auto at(std::string_view key) -> node_object*;
  // writes the record into a series builder
  auto
  commit_to(tenzir::record_ref r, class data_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;
  auto
  commit_to(tenzir::record& r, class data_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;
  // append the signature of this record to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::record_type* seed) -> void;
  // clears the record by marking everything as dead
  auto clear() -> void;

  // Record entry. This contains a string for the key and a field.
  // Its defined out of line because node_object cannot be defined at this point.
  struct entry_type;
  // This stores added fields in order of their appearance
  // This order is used for committing to the `series_builder`, in order to
  // (mostly) preserved the field order from the input, apart from fields the
  // `series_builder` was seeded with. The order of fields in a seed/selector on
  // the other hand is then practically ensured because the multi_series_builder
  // first seeds the respective `series_builder`.
  std::vector<entry_type> data_;
  // This is a sorted key -> index map. It is used for signature computation.
  // If this map is not sorted, the signature computation algorithm breaks,
  // since it would then be order dependent.
  flat_map<std::string, size_t> lookup_;
};

class node_list : public node_base {
  friend class node_record;
  friend class node_object;

public:
  /// @brief Reserves storage for at least N elements in the list.
  /// This function can be used to get temporary pointer stability on the
  /// list elements.
  auto reserve(size_t N) -> void;
  /// @brief appends a new typed value to this list.
  /// If its type mismatches with the seed during the later parsing/signature
  /// computation, a warning is emitted.
  template <non_structured_data_type T>
  auto data(T data) -> void;
  /// @brief Unpacks the tenzir::data into a new element at the end of th list
  auto data(tenzir::data) -> void;
  /// @brief Appends some unparsed data to this list.
  /// It is later parsed when a seed is potentially available.
  auto data_unparsed(std::string_view) -> void;
  /// @brief adds a null value to the list
  auto null() -> void;
  /// @brief adds a new record to the list
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  [[nodiscard]] auto record() -> node_record*;
  /// @brief Appends a new list to the list.
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
  auto find_free() -> node_object*;
  auto back() -> node_object&;

  auto update_new_structural_signature() -> void;

  // writes the list into a series builder
  auto commit_to(tenzir::builder_ref r, class data_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;
  auto commit_to(tenzir::list& r, class data_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;
  // append the signature of this list to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::list_type* seed) -> void;
  auto clear() -> void;

  size_t type_index_ = type_index_empty;
  signature_type current_structural_signature_;
  signature_type new_structural_signature_;
  std::vector<node_object> data_;
};

class node_object : public node_base {
  friend class node_record;
  friend class node_list;
  friend struct node_record::entry_type;
  friend class ::tenzir::data_builder;
  friend class ::tenzir::multi_series_builder;

public:
  /// @brief Sets this field to a parsed, typed data value.
  /// If its type mismatches with the seed during the later parsing/signature
  /// computation, a warning is emitted.
  template <non_structured_data_type T>
  auto data(T data) -> void;
  /// @brief Unpacks the tenzir::data into this field
  auto data(tenzir::data) -> void;
  /// @brief Sets this field to some unparsed data.
  /// It is later parsed when a seed is potentially available.
  auto data_unparsed(std::string_view raw_text) -> void;
  auto null() -> void;
  [[nodiscard]] auto record() -> node_record*;
  [[nodiscard]] auto list() -> node_list*;

  node_object() : data_{std::in_place_type<caf::none_t>} {
  }
  template <non_structured_data_type T>
  node_object(T data) : data_{std::in_place_type<T>, data} {
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
  auto
  try_resolve_nonstructural_field_mismatch(class data_builder& rb,
                                           const tenzir::type* seed) -> void;
  /// parses any unparsed fields using `parser`, potentially providing a
  /// seed/schema to the parser
  auto parse(class data_builder& rb, const tenzir::type* seed) -> void;
  // append the signature of this field to `sig`.
  // including sentinels is important for signature computation
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::type* seed) -> void;
  // writes the field into a series builder
  auto commit_to(tenzir::builder_ref r, class data_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;
  auto commit_to(tenzir::data& r, class data_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;
  auto clear() -> void;

  // clang-format off
  using field_variant_type = caf::detail::tl_apply_t<
   field_type_list,
    std::variant
  >;
  // clang-format on

  field_variant_type data_;

  enum class value_state_type { has_value, unparsed, null };
  // this is the state of the contained value. This exists in case somebody calls
  // `record.field("key")` but never inserts any data into the field
  // this is distinctly different from a node not being `alive`,
  // which only happens as a result of internal storage reuse.
  value_state_type value_state_ = value_state_type::null;
};

struct node_record::entry_type {
  std::string key;
  node_object value;

  entry_type(std::string_view name) : key{name} {
  }
};

constexpr static std::byte record_start_marker{0xfa};
constexpr static std::byte record_end_marker{0xfb};

constexpr static std::byte list_start_marker{0xfc};
constexpr static std::byte list_end_marker{0xfd};

} // namespace detail::data_builder

/// @brief The `data_builder` provides an incremental factory API to
/// create a single `tenzir::data`. It also supports writing the result
/// directly into a `series_builder` instead.
/// * record() inserts a record
/// * list() inserts a list
/// * data( value ) inserts a value
/// * data_unparsed( string ) inserts a value that will be parsed later on
/// * record_generator::field( string ) inserts a field that will be unflattend
/// * record_generator::exact_field( string ) inserts a field with the exact name
/// * record_generator::unflattend_field inserts a field that is explicitly
///   unflattend
class data_builder {
  friend class detail::data_builder::node_list;
  friend class detail::data_builder::node_record;
  friend class detail::data_builder::node_object;

public:
  using data_parsing_function
    = std::function<detail::data_builder::data_parsing_result(
      std::string_view str, const tenzir::type* seed)>;
  data_builder(data_parsing_function parser
               = detail::data_builder::basic_parser,
               diagnostic_handler* dh = nullptr, bool schema_only = false,
               bool parse_schema_fields_only = false);

  /// @brief Start building a record
  [[nodiscard]] auto record() -> detail::data_builder::node_record*;

  /// @brief Start building a list
  [[nodiscard]] auto list() -> detail::data_builder::node_list*;

  /// @brief Sets the top level value to the given data
  template <detail::data_builder::non_structured_data_type T>
  auto data(T value) {
    root_.data(std::move(value));
  }

  /// @brief Sets the top level value to the given data
  auto data(tenzir::data value) {
    root_.data(std::move(value));
  }

  /// @brief Sets the top level the given string.
  /// The string will automatically be parsed (later) according to the parser
  /// that was parser the `data_builder` was constructed with.
  auto data_unparsed(std::string_view) -> void;

  [[nodiscard]] auto has_elements() -> bool {
    return root_.is_alive();
  }

  /// tries to find a field with the given (nested) key
  [[nodiscard]] auto
  find_field_raw(std::string_view key) -> detail::data_builder::node_object*;

  /// tries to find a field with the given (nested) key for a data type
  // template <detail::data_builder::non_structured_data_type T>
  // [[nodiscard]] auto find_value_typed(std::string_view key) -> T*;

  using signature_type = typename detail::data_builder::signature_type;
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
                                     = nullptr) -> tenzir::data;
  /// commits the current record into the series builder
  /// @param mark_dead whether to mark nodes in the record builder as dead
  auto commit_to(series_builder&, bool mark_dead = true,
                 const tenzir::type* seed = nullptr) -> void;

private:
  /// tries to lookup the type `r` in the type lookup map,
  /// and potentially writes creates sentinel fields in `apply`
  /// if they dont exist in the record yet
  auto lookup_record_fields(const tenzir::record_type* r,
                            detail::data_builder::node_record* apply)
    -> const detail::data_builder::field_type_lookup_map*;

  detail::data_builder::node_object root_;
  detail::data_builder::schema_type_lookup_map schema_type_lookup_;
  diagnostic_handler* dh_;

public:
  data_parsing_function parser_;

private:
  bool schema_only_;
  bool parse_schema_fields_only_;

  auto emit_or_throw(tenzir::diagnostic&& diag) -> void;
  auto emit_or_throw(tenzir::diagnostic_builder&& builder) -> void;
};

namespace detail::data_builder {
template <non_structured_data_type T>
auto node_object::data(T data) -> void {
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
    data_.back().value_state_ = node_object::value_state_type::has_value;
    update_type_index(type_index_, data_.back().current_index());
  }
}
} // namespace detail::data_builder
} // namespace tenzir
