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
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/type_list.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/series_builder.hpp"
#include "tsl/robin_map.h"

#include <caf/error.hpp>
#include <caf/expected.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
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
/// If the returned optional is empty, that means that the value did not parse
/// as any type and should remain a string.
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
using field_type_list = detail::type_list<
  /* 0 */ caf::none_t,
  /* 1 */ bool,
  /* 2 */ int64_t,
  /* 3 */ uint64_t,
  /* 4 */ double,
  /* 5 */ duration,
  /* 6 */ time,
  /* 7 */ std::string,
  /* 8 */ pattern_dummy,
  /* 9 */ ip,
  /*10 */ subnet,
  /*11 */ enumeration,
  /*12 */ node_list,
  /*13 */ map_dummy,
  /*14 */ node_record,
  /*15 */ enriched_dummy,
  /*16 */ blob,
  /*17 */ secret
>;
// clang-format on

template <typename T>
concept non_structured_data_type
  = detail::tl_contains<field_type_list, T>::value
    and not detail::is_any_v<T, node_record, node_list, pattern_dummy,
                             map_dummy, enriched_dummy>;

template <typename T>
concept non_structured_type_type
  = detail::tl_contains<concrete_types, T>::value
    and not detail::is_any_v<record_type, list_type, legacy_pattern_type,
                             map_type>;

template <typename T>
concept numeric_type_type
  = non_structured_data_type<T>
    and detail::is_any_v<T, uint64_type, int64_type, double_type>;

template <typename T>
concept numeric_data_type = non_structured_data_type<T>
                            and detail::is_any_v<T, uint64_t, int64_t, double>;

template <typename T>
concept unsupported_type
  = detail::is_any_v<T, tenzir::map_type, tenzir::map, map_dummy,
                     tenzir::legacy_pattern_type, tenzir::pattern,
                     pattern_dummy, enriched_dummy>;

using signature_type = std::vector<std::byte>;
// outer map needs iterator stability at the moment
// TODO maybe it can be made faster if we dont use iterator stability
// and instead re-query for `seed_it`
using field_type_lookup_map = tsl::robin_map<std::string, tenzir::type>;
using schema_type_lookup_map
  = std::unordered_map<tenzir::record_type, field_type_lookup_map>;

constexpr inline size_t type_index_empty
  = detail::tl_size<field_type_list>::value;
constexpr inline size_t type_index_generic_mismatch
  = detail::tl_size<field_type_list>::value + 1;
constexpr inline size_t type_index_numeric_mismatch
  = detail::tl_size<field_type_list>::value + 2;
constexpr inline size_t type_index_null
  = detail::tl_index_of<field_type_list, caf::none_t>::value;
constexpr inline size_t type_index_string
  = detail::tl_index_of<field_type_list, std::string>::value;
constexpr inline size_t type_index_double
  = detail::tl_index_of<field_type_list, double>::value;
constexpr inline size_t type_index_list
  = detail::tl_index_of<field_type_list, node_list>::value;
constexpr inline size_t type_index_record
  = detail::tl_index_of<field_type_list, node_record>::value;

inline constexpr auto is_structural(size_t idx) -> bool {
  switch (idx) {
    case detail::tl_index_of<field_type_list, node_list>::value:
    case detail::tl_index_of<field_type_list, node_record>::value:
      return true;
    default:
      return false;
  }
}

inline constexpr auto is_numeric(size_t idx) -> bool {
  switch (idx) {
    case detail::tl_index_of<field_type_list, int64_t>::value:
    case detail::tl_index_of<field_type_list, uint64_t>::value:
    case detail::tl_index_of<field_type_list, double>::value:
    case detail::tl_index_of<field_type_list, enumeration>::value:
      return true;
    default:
      return false;
  }
}

inline auto is_null(size_t idx) -> bool {
  return idx == detail::tl_index_of<field_type_list, caf::none_t>::value;
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
  if (is_numeric(old_index) and is_numeric(new_index)) {
    old_index = type_index_numeric_mismatch;
    return;
  }
  old_index = type_index_generic_mismatch;
}

enum class state {
  /// The node contains an active value
  /// It should be considered for the signature and actually
  /// written in `commit_to`
  /// This state is used when actual data is written into the builder via
  /// some `data`, `data_unparsed`, `record`, `list` or `null` call
  alive,
  /// The node contains a sentinel value
  /// It only affects the signature, but should not be written in `commit_to`
  /// This state is only created when a `node_record` is used in conjuction
  /// with a seed. The function `data_builder::lookup_record_fields` is used with
  /// an `apply` parameter, which then ensures that all nodes in the schema that
  /// aren't already `alive` exist as `sentinel` nodes in the record.
  sentinel,
  /// The node is dead. Its only retained for memory efficiency, so that we dont
  /// allocate and deallocate nodes all the time.
  /// This state is created via the nodes `commit_to` function's `mark_dead`
  /// parameter or any explicit `clear` call.
  /// These functions will mark nodes as `dead` so that the node itself remains
  /// but it can be re-used for the next event created.
  dead,
};

class node_base {
protected:
  /// Updates the nodes state to reflect that it affects the signature
  /// * If the node is already alive or a sentinel, this has no effect.
  /// * If the node is dead, it makes it a sentinel instead.
  auto mark_this_relevant_for_signature() -> void {
    if (state_ != state::alive) {
      state_ = state::sentinel;
    }
  }
  auto mark_this_alive() -> void {
    state_ = state::alive;
  }
  auto mark_this_dead() -> void {
    state_ = state::dead;
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

  /// @brief writes the list into a series builder
  /// @param r The builder_ref to write to.
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto
  commit_to(tenzir::record_ref r, class data_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;

  /// @brief writes the list into a series builder
  /// @param r The tenzir::record to write to.
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto
  commit_to(tenzir::record& r, class data_builder& rb,
            const tenzir::record_type* seed, bool mark_dead = true) -> void;
  /// @brief Append the signature of this field to `sig`.
  /// @param sig The out parameter
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::record_type* seed) -> void;

  /// @brief marks all fields in the record as dead.
  auto clear() -> void;

  /// @brief reduces the number of elements to the limit
  auto prune() -> void;

  // Record entry. This contains a string for the key and a field.
  // Its defined out of line because node_object cannot be defined at this point.
  struct entry_type;
  // This stores added fields in order of their appearance
  // This order is used for committing to the `series_builder`, in order to
  // (mostly) preserved the field order from the input, apart from fields the
  // `series_builder` was seeded with. The order of fields in a seed/selector on
  // the other hand is then practically ensured because the multi_series_builder
  // first seeds the respective `series_builder`.
  // Notably if the series_builder resets on a `finish`, *and* the
  // `data_builder` looses its internal ordering (because the record node was
  // replaced), then events with the same fields, but different appearance order
  // will have the same signature but a different resulting schema.
  // TODO: Consider dropping the input field order for non-schema fields (just
  // sort alphabetically) or extending the `series_builder` to not drop fields
  // on `finish` (probably via some special flag)
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
  /// @brief Unpacks The tenzir::data into a new element at the end of th list
  auto data(tenzir::data) -> void;
  /// @brief Appends some unparsed data to this list.
  /// It is later parsed when a seed is potentially available.
  auto data_unparsed(std::string) -> void;
  /// @brief adds a null value to the list
  auto null() -> void;
  /// @brief adds a new record to the list
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  auto record() -> node_record*;
  /// @brief Appends a new list to the list.
  /// @note the returned pointer is not permanently stable. If the underlying
  /// vector reallocates, the pointer becomes invalid
  /// @ref reserve can be used to ensure stability for a given number of elements
  auto list() -> node_list*;

  node_list() = default;
  node_list(const node_list&) = default;
  node_list(node_list&&) = default;
  node_list& operator=(const node_list&) = default;
  node_list& operator=(node_list&&) = default;
  ~node_list();

private:
  /// Appends a node by first trying to resurrect a dead node and only creating
  /// a new one if necessary
  auto push_back_node() -> node_object*;

  /// @brief writes the list into a series builder
  /// @param r The builder_ref to write to.
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto commit_to(tenzir::builder_ref r, class data_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;

  /// @brief writes the list into a series builder
  /// @param r The tenzir::list to write to
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto commit_to(tenzir::list& r, class data_builder& rb,
                 const tenzir::list_type* seed, bool mark_dead = true) -> void;
  /// @brief Append the signature of this field to `sig`.
  /// @param sig The out parameter
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::list_type* seed) -> void;

  /// @brief marks the list and all its contents as dead, resetting its size to 0
  auto clear() -> void;

  /// @brief reduces the number of elements to the limit
  auto prune() -> void;

  // gets all alive nodes, i.e. all nodes with indices in `[0, first_dead_idx_)`
  auto alive_elements() -> std::span<node_object>;

  /// The index of the first node that is already dead.
  /// Only nodes with indices in `[0, first_dead_idx_)` are `alive`
  /// All nodes beyond that are `dead` and are only retained to keep the memory
  /// around.
  size_t first_dead_idx_ = 0;
  /// The current type index of the entire list
  /// This is computed when adding elements to the list
  /// In some cases, this can allow us to avoid iterating the entire list
  /// when computing the signature.
  size_t type_index_ = type_index_empty;
  /// The actual contents of the list.
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
  /// @brief Unpacks The tenzir::data into this field
  auto data(tenzir::data) -> void;
  /// @brief Sets this field to some unparsed data.
  /// It is later parsed when a seed is potentially available.
  auto data_unparsed(std::string raw_text) -> void;
  auto null() -> void;
  auto record() -> node_record*;
  auto list() -> node_list*;

  node_object();

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

  auto
  try_resolve_nonstructural_field_mismatch(class data_builder& rb,
                                           const tenzir::type* seed) -> void;
  /// parses any unparsed fields using `parser`, potentially providing a
  /// seed/schema to the parser
  auto parse(class data_builder& rb, const tenzir::type* seed) -> void;
  /// @brief Append the signature of this field to `sig`.
  /// @param sig The out parameter
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  auto append_to_signature(signature_type& sig, class data_builder& rb,
                           const tenzir::type* seed) -> void;

  /// @brief writes the list into a series builder
  /// @param r The builder_ref to write to.
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto commit_to(tenzir::builder_ref r, class data_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;

  /// @brief writes the list into a series builder
  /// @param r The tenzir::data to write to.
  /// @param rb The data_builder that is doing the writing.
  /// @param seed The seed to use. This is used both for parsing and
  ///             to ensure that field types actually match it.
  /// @param mark_dead whether to mark the node (and its children) as
  ///                  `state::dead` afterwards
  auto commit_to(tenzir::data& r, class data_builder& rb,
                 const tenzir::type* seed, bool mark_dead = true) -> void;
  /// @brief marks the node and its contents as dead
  auto clear() -> void;

  using object_variant_type = detail::tl_apply_t<field_type_list, std::variant>;

  object_variant_type data_;

  enum class value_state_type {
    /// The node actually has a value that should be used.
    /// This means that `data`, `list`, `record` or `null` were called on this
    /// node
    has_value,
    /// The node is yet to be parsed.
    /// This means that `data_unparsed` was called on this node.
    unparsed,
    /// The node is null. This means that it was created in a record as
    /// `.field("key")`, not nothing further was ever be done with the node.
    null,
  };
  /// This is the state of the contained value. See `value_state_type`
  value_state_type value_state_ = value_state_type::null;
};

struct node_record::entry_type {
  std::string key;
  node_object value;

  /// This must exist for the `data_.resize()` call, but is guaranteed by the
  /// surrounding logic to never be called.
  entry_type() {
    TENZIR_UNREACHABLE();
  }

  entry_type(std::string_view name) : key{name} {
  }
};

inline node_object::node_object() : data_{std::in_place_type<caf::none_t>} {
}

constexpr static std::byte record_start_marker{0xfa};
constexpr static std::byte record_end_marker{0xfb};

constexpr static std::byte list_start_marker{0xfc};
constexpr static std::byte list_end_marker{0xfd};
constexpr static std::byte list_error_marker{0xfe};

} // namespace detail::data_builder

/// @brief The `data_builder` provides an incremental factory API to
/// create a single `tenzir::data`. It also supports writing the result
/// directly into a `series_builder` instead.
/// * record() inserts a record
/// * list() inserts a list
/// * record.field( name ) inserts a field into a record
/// * data( value ) inserts a value
/// * data_unparsed( string ) inserts a value that will be parsed later on
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
  auto data_unparsed(std::string) -> void;

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
  ///////
  /// The members below exist to speed up schema lookup by name
  /// The map allows us to quickly lookup a record type and get all its fields
  /// Without the map we would be bottle-necked by iterating
  /// `record_type::fields`
  ///////
  /// @brief tries to lookup the type in the map. If the type looked for is not
  ////       yet in the map, its also added.
  /// @param r a pointer to the type to look for. If this is null, the function
  ///          returns null and has no effect
  /// @param apply a pointer to a record to which all fields should be added if
  ///              the type for `r` was found. Fields that arent already `alive`
  ///              in `apply` will either be
  ///               * resurrected if they exist and marked `sentinel`
  ///               * created and marked `sentinel`
  auto lookup_record_fields(const tenzir::record_type* r,
                            detail::data_builder::node_record* apply)
    -> const detail::data_builder::field_type_lookup_map*;

  /// This is the root object that actually holds all data
  detail::data_builder::node_object root_;
  /// The map used to lookup fields and their types.
  /// This map should only be used via `lookup_record_fields`
  detail::data_builder::schema_type_lookup_map schema_type_lookup_;
  diagnostic_handler* dh_;

public:
  data_parsing_function parser_;

private:
  /// whether to discard fields that are not present in the used schema.
  bool schema_only_;
  /// Whether to only parse fields that are present in the used schema.
  /// If this is enabled, fields created as `data_unparsed` will only be parsed
  /// as typed data, if the are present in the used schema (seed)
  bool parse_schema_fields_only_;

  auto emit_or_throw(tenzir::diagnostic&& diag) -> void;
  auto emit_or_throw(tenzir::diagnostic_builder&& builder) -> void;
  auto
  emit_mismatch_warning(const type& value_type, const type& seed_type) -> void;
  auto emit_mismatch_warning(std::string_view value_type,
                             const type& seed_type) -> void;
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
  push_back_node()->data(std::move(data));
  update_type_index(type_index_, data_.back().current_index());
}
} // namespace detail::data_builder
} // namespace tenzir
