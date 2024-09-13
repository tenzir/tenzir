//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data_builder.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/series.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/type.hpp"

#include <arrow/type_fwd.h>
#include <caf/error.hpp>
#include <tsl/robin_map.h>

#include <chrono>
#include <concepts>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace tenzir {

class multi_series_builder;

using parser_function_type
  = std::function<detail::data_builder::data_parsing_result(
    std::string_view, const tenzir::type*)>;

namespace detail::multi_series_builder {

using signature_type = std::vector<std::byte>;

class list_generator;
class object_generator;

class record_generator {
  using raw_pointer = detail::data_builder::node_record*;

public:
  explicit record_generator(class multi_series_builder* msb,
                            tenzir::record_ref builder)
    : msb_{msb}, var_{std::in_place_type<tenzir::record_ref>, builder} {
  }
  explicit record_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }
  /// @brief Adds an field with exactly the given name to the record.
  /// This function does not perform any unflatten operation.
  auto exact_field(std::string_view name) -> object_generator;
  /// @brief Adds a new field to the record and returns a generator for that
  /// field. Iff the backing `multi_series_builder` has an unnest-separator,
  /// this function will also unflatten.
  auto field(std::string_view name) -> object_generator;

  /// @brief Creates an explicitly unflattend field. This function does
  /// not respect the builders unflatten setting.
  auto unflattend_field(std::string_view key,
                        std::string_view unflatten) -> object_generator;
  /// @brief Creates an explicitly unflattend field according to the
  /// `multi_series_builder`s unflatten setting.
  auto unflattend_field(std::string_view key) -> object_generator;

private:
  class multi_series_builder* msb_ = nullptr;
  std::variant<tenzir::record_ref, raw_pointer> var_;
};

class object_generator {
  using raw_pointer = detail::data_builder::node_object*;

public:
  /// A non-associated field generator. This is used in the unflatten function.
  object_generator() : object_generator(nullptr, nullptr) {
  }
  object_generator(class multi_series_builder* msb, builder_ref builder)
    : msb_{msb}, var_{builder} {
  }
  object_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }

  /// @brief Sets the value of the field to some data
  template <tenzir::detail::data_builder::non_structured_data_type T>
  auto data(T d) -> void {
    const auto visitor = detail::overload{
      [&](tenzir::builder_ref b) {
        b.data(d);
      },
      [&](raw_pointer raw) {
        raw->data(d);
      },
    };
    return std::visit(visitor, var_);
  }

  /// @brief sets the value of the field to the contents of a `tenzir::data`
  auto data(const tenzir::data& d) -> void;

  /// @brief sets the value of the field to some unparsed text. Parsing will
  /// happen at a later time for the precise modes or immediately in merging mode.
  auto data_unparsed(std::string_view s) -> void;
  /// @brief sets the value of the field to some unparsed text. Parsing will
  /// happen at a later time for the precise modes or immediately in merging mode.
  auto data_unparsed(std::string s) -> void;

  /// @brief Sets the value of the field an empty record and returns a generator
  /// for the record
  auto record() -> record_generator;

  /// @brief Sets the value of the field an empty list and returns a generator
  /// for the list
  auto list() -> list_generator;

  /// @brief Sets the value of the field to null
  auto null() -> void;

private:
  class multi_series_builder* msb_;
  std::variant<tenzir::builder_ref, raw_pointer> var_;
};

class list_generator {
  using raw_pointer = detail::data_builder::node_list*;

public:
  list_generator(class multi_series_builder* msb, builder_ref builder)
    : msb_{msb}, var_{builder} {
  }
  list_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }

  /// @brief appends a data value T to the list
  template <tenzir::detail::data_builder::non_structured_data_type T>
  auto data(T d) -> void {
    const auto visitor = detail::overload{
      [&](tenzir::builder_ref b) {
        b.data(d);
      },
      [&](raw_pointer raw) {
        raw->data(d);
      },
    };
    return std::visit(visitor, var_);
  }

  /// @brief appends the contents of a `tenzir::data` to the list
  auto data(const tenzir::data& d) -> void;

  /// @brief sets the value of the field to some unparsed text. Parsing will
  /// happen at a later time for the precise modes or immediately in merging mode.
  auto data_unparsed(std::string_view s) -> void;
  /// @brief sets the value of the field to some unparsed text. Parsing will
  /// happen at a later time for the precise modes or immediately in merging mode.
  auto data_unparsed(std::string s) -> void;
  /// @brief appends a record to the list and returns a generator for the record
  auto record() -> record_generator;

  /// @brief appends a list to the list and returns a generator for the list
  auto list() -> list_generator;

  /// @brief append a null value to the list
  auto null() -> void;

private:
  class multi_series_builder* msb_;
  std::variant<tenzir::builder_ref, raw_pointer> var_;
};

template <typename T>
concept has_exact_field
  = requires(T& t, std::string key) { t.exact_field(key); };

template <typename T>
concept has_unflattend_field
  = requires(T& t, std::string key) { t.unflattend_field(key); };

template <typename T>
concept has_data_unparsed
  = requires(T& t, std::string_view txt) { t.data_unparsed(txt); };

static_assert(has_exact_field<record_generator>);
static_assert(has_unflattend_field<record_generator>);
static_assert(has_data_unparsed<object_generator>);

auto series_to_table_slice(series array, std::string_view fallback_name
                                         = "tenzir.unknown") -> table_slice;
auto series_to_table_slice(std::vector<series> data,
                           std::string_view fallback_name
                           = "tenzir.unknown") -> std::vector<table_slice>;
} // namespace detail::multi_series_builder

/// @brief This class provides an incremental builder API to build multiple
/// different table slices based on the input. Unlike the `series_builder`,
/// The plain `series_builder`s behaviour can be obtained by using the
/// `merging_policy`.
/// In the other policies, there is one `series_builder` per input schema.
/// An event is first written into a `data_builder`, which is then used
/// to compute a byte-signature. This byte-signature then determines which
/// `series_builder` the event is written into.
///
/// The API works identical to the `series_builder`:
/// * record() inserts a record
/// * list() inserts a list
/// * data( value ) inserts a value
/// * data_unparsed( string ) inserts a value that will be parsed later on
/// * record_generator::field( string ) inserts a field that will be unflattend
/// * record_generator::exact_field( string ) inserts a field with the exact name
/// * record_generator::unflattend_field inserts a field that is explicitly
///   unflattend
class multi_series_builder {
public:
  friend class detail::multi_series_builder::record_generator;
  friend class detail::multi_series_builder::object_generator;
  friend class detail::multi_series_builder::list_generator;
  using record_generator = detail::multi_series_builder::record_generator;
  using list_generator = detail::multi_series_builder::list_generator;

  /// @returns a vector of all currently finished series
  [[nodiscard("The result of a flush must be handled")]]
  auto yield_ready() -> std::vector<series>;
  /// @returns a vector of all currently finished series
  [[nodiscard("The result of a flush must be handled")]]
  auto yield_ready_as_table_slice() -> std::vector<table_slice>;

  /// @brief Starts building a new record.
  [[nodiscard]] auto record() -> record_generator;
  /// @brief Starts building a new list.
  [[nodiscard]] auto list() -> list_generator;

  /// @brief Inserts a new value into the builder.
  template <detail::data_builder::non_structured_data_type T>
  auto data(T value) -> void {
    if (uses_merging_builder()) {
      return merging_builder_.data(value);
    } else {
      complete_last_event();
      return builder_raw_.data(std::move(value));
    }
  }

  /// @brief Drops the last event from active builder.
  void remove_last();

  [[nodiscard("The result of a flush must be handled")]]
  auto finalize() -> std::vector<series>;
  [[nodiscard("The result of a flush must be handled")]]
  auto finalize_as_table_slice() -> std::vector<table_slice>;

  /// @brief This policy will merge all events into a single schema
  struct policy_default {
    static constexpr std::string_view name = "none";
    // a schema name to seed with. If this is given

    auto friend inspect(auto& f, policy_default& x) -> bool {
      return f.object(x).fields();
    }
  };

  /// @brief This policy will keep all schemas in separate batches
  struct policy_schema {
    static constexpr std::string_view name = "schema";
    // If this is given, all resulting events will have exactly this schema
    // * all fields in the schema but not in the event will be null
    std::string seed_schema = {};

    auto friend inspect(auto& f, policy_schema& x) -> bool {
      return f.object(x).fields(f.field("seed_schema", x.seed_schema));
    }
  };

  /// @brief This policy will keep all schemas in batches according to selector
  struct policy_selector {
    static constexpr std::string_view name = "selector";
    // The field name to use for selection
    std::string field_name;
    // A naming prefix, doing the following transformation on the name:
    // selector("event_type", "suricata")
    // => {"event_type": "flow"}
    // => "suricata.flow"
    std::optional<std::string> naming_prefix = std::nullopt;

    auto friend inspect(auto& f, policy_selector& x) -> bool {
      return f.object(x).fields(f.field("field_name", x.field_name),
                                f.field("naming_prefix", x.naming_prefix));
    }
  };

  using policy_type
    = tenzir::variant<policy_default, policy_schema, policy_selector>;

  /// @brief Holds generic settings for the builder.
  struct settings_type {
    // The default name given to a schema, if its not determined by `schema` or
    // `selector`
    std::string default_schema_name = "tenzir.unknown";
    // Whether the output should adhere to the input order
    bool ordered = true;
    // Whether, given a known schema via `schema` or `selector`, only fields
    // from that should be output. If the schema does not exist, this has no
    // effect.
    bool schema_only = false;
    // Whether to "merge" results
    // * In the `policy_selector`, this merges all events with the same selector
    // * In the `policy_schema` and `policy_default` this merges all events
    //   into a single schema
    bool merge = false;
    // Whether to not parse fields that are not present in a known schema.
    bool raw = false;
    // Unnest separator to be used when calling any `field` in the builder
    // pattern.
    std::string unnest_separator = {};
    // Timeout after which events will be yielded regardless of whether the
    // desired batch size has been reached.
    duration timeout = defaults::import::batch_timeout;
    // Batch size after which the events should be yielded.
    size_t desired_batch_size = defaults::import::table_slice_size;

    auto friend inspect(auto& f, settings_type& x) -> bool {
      return f.object(x).fields(
        f.field("default_schema_name", x.default_schema_name),
        f.field("ordered", x.ordered), f.field("schema_only", x.schema_only),
        f.field("merge", x.merge), f.field("raw", x.raw),
        f.field("unnest_separator", x.unnest_separator),
        f.field("timeout", x.timeout),
        f.field("desired_batch_size", x.desired_batch_size));
    }
  };

  /// @brief A simple convenience wrapper, holding both settings and policy.
  struct options {
    multi_series_builder::policy_type policy
      = multi_series_builder::policy_default{};
    multi_series_builder::settings_type settings = {};

    friend auto inspect(auto& f, options& x) -> bool {
      return f.object(x).fields(f.field("policy", x.policy),
                                f.field("settings", x.settings));
    }
  };

  multi_series_builder(options opts, diagnostic_handler& dh,
                       std::vector<type> schemas = modules::schemas(),
                       data_builder::data_parsing_function parser
                       = detail::data_builder::basic_parser)
    : multi_series_builder{std::move(opts.policy), std::move(opts.settings), dh,
                           std::move(schemas), std::move(parser)} {
  }

  multi_series_builder(
    policy_type policy, settings_type settings, diagnostic_handler& dh,
    std::vector<type> schemas
    = modules::schemas(), // FIXME remove the explicit call at use sites
    data_builder::data_parsing_function parser
    = detail::data_builder::basic_parser);

  multi_series_builder(multi_series_builder&& other) noexcept;
  multi_series_builder& operator=(const multi_series_builder&) = delete;
  // unimplemented
  multi_series_builder& operator=(multi_series_builder&&) = delete;

private:
  /// @brief Gets a pointer to the active policy, if its the given one.
  template <typename T>
  auto get_policy() const -> const T* {
    return std::get_if<T>(&policy_);
  }

  auto uses_merging_builder() const -> bool {
    return not get_policy<policy_selector>() and settings_.merge;
  };

  /// @brief Called internally to complete the last built event.
  /// This is called whenever the user starts building a new event,
  /// or requests ready events.
  /// This function is responsible for computing the signature and
  /// committing the event to the correct `series_builder` based on that.
  void complete_last_event();

  /// @brief clears the currently build raw event.
  void clear_raw_event();

  /// @brief Gets the next free index into `entries_`.
  std::optional<size_t> next_free_index() const;

  /// @brief Look up a schema by name.
  auto type_for_schema(std::string_view str) -> const type*;

  struct entry_data {
    entry_data(const tenzir::type* schema = nullptr)
      : builder{schema}, flushed{std::chrono::steady_clock::now()} {
    }

    auto flush() -> std::vector<series> {
      flushed = std::chrono::steady_clock::now();
      return builder.finish();
    }

    series_builder builder;
    std::chrono::steady_clock::time_point flushed;
    bool unused = false;
  };

  /// @brief Finishes all events that satisfy the predicate.
  /// These events are moved out of their respective series_builders and into
  /// `ready_events_`.
  /// the implementation is in the source file, since its a private/internal
  /// function and thus will only be instantiated by other member functions.
  /// @ref `ready_events_`
  /// @ref `yield_ready`
  void make_events_available_where(std::predicate<const entry_data&> auto pred);

  /// @brief appends `new_events` to `ready_events_`
  void append_ready_events(std::vector<series>&& new_events);

  /// @brief "garbage collects" all entries in `entries_` that satisfy the
  /// predicate.
  /// The implementation is in the source file, since its a private/internal
  /// function and thus will only be instantiated by other member functions.
  void garbage_collect_where(std::predicate<const entry_data&> auto pred);

  using signature_type = typename data_builder::signature_type;

  // the builders policy
  policy_type policy_;
  // the builders settings
  settings_type settings_;
  // the diagnostic handler to be used
  diagnostic_handler& dh_;
  // used for quick name -> schema mapping
  detail::flat_map<std::string, tenzir::type> schemas_;
  // builder used in merging mode
  series_builder merging_builder_;
  // builder_raw_ must be constructed after `dh_` as it depends on it
  data_builder builder_raw_;
  // used to determine whether we need a signature compute
  bool needs_signature_ = true;
  // used to name builders
  tenzir::type naming_sentinel_;
  // the schema to construct the series builder with
  const tenzir::type* builder_schema_ = nullptr;
  // the schema to use when during parsing/signature computation
  const tenzir::type* parsing_signature_schema_ = nullptr;
  // signature vector, kept around for memory
  signature_type signature_raw_;
  // lookup map to lookup from signature -> index into `entries_`
  tsl::robin_map<signature_type, size_t, detail::hash_algorithm_proxy<>>
    signature_map_;
  // all currently active builders
  std::vector<entry_data> entries_;
  // events that have been made ready (timeout,  batch size, ordered mode
  // builder switch)
  std::vector<series> ready_events_;
  // time at which the entire builder made its last yields
  std::chrono::steady_clock::time_point last_yield_time_
    = std::chrono::steady_clock::now();
  // currently active builder index. used in ordered mode to check whether we
  // need to yield on builder switch
  size_t active_index_ = 0;
};
} // namespace tenzir
