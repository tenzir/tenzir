//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/defaults.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/record_builder.hpp"
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
  = std::function<detail::record_builder::data_parsing_result(
    std::string_view, const tenzir::type*)>;

namespace detail::multi_series_builder {

using signature_type = std::vector<std::byte>;

class list_generator;
class field_generator;

class record_generator {
  using raw_pointer = detail::record_builder::node_record*;

public:
  explicit record_generator(class multi_series_builder* msb,
                            tenzir::record_ref builder)
    : msb_{msb}, var_{std::in_place_type<tenzir::record_ref>, builder} {
  }
  explicit record_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }
  /// adds a new field to the record and returns a generator for that field
  /// if the backing `multi_series_builder` has an unnest-separator, this
  /// function will also unflatten
  auto exact_field(std::string_view name) -> field_generator;

  /// creates an explicitly unflattend field.
  /// DOES NOT RESPECT THE `multi_series_builder`s unflatten settings
  auto unflattend_field(std::string_view key,
                        std::string_view unflatten) -> field_generator;
  /// creates an explicitly unflattend field according to the
  /// `multi_series_builder`s unflatten setting
  auto unflattend_field(std::string_view key) -> field_generator;

private:
  class multi_series_builder* msb_ = nullptr;
  std::variant<tenzir::record_ref, raw_pointer> var_;
};

class field_generator {
  using raw_pointer = detail::record_builder::node_field*;

public:
  /// A non-associated field generator. BE CAREFUL WITH THIS.
  field_generator() : field_generator(nullptr, nullptr) {
  }
  field_generator(class multi_series_builder* msb, builder_ref builder)
    : msb_{msb}, var_{builder} {
  }
  field_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }

  /// sets the value of the field to some data
  template <tenzir::detail::record_builder::non_structured_data_type T>
  void data(T d) {
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

  void data_unparsed(std::string_view s);

  /// sets the value of the field an empty record and returns a generator for
  /// the record
  auto record() -> record_generator;

  /// sets the value of the field an empty list and returns a generator for the
  /// list
  auto list() -> list_generator;

  /// sets the value of the field to null
  void null();

private:
  class multi_series_builder* msb_;
  std::variant<tenzir::builder_ref, raw_pointer> var_;
};

class list_generator {
  using raw_pointer = detail::record_builder::node_list*;

public:
  list_generator(class multi_series_builder* msb, builder_ref builder)
    : msb_{msb}, var_{builder} {
  }
  list_generator(class multi_series_builder* msb, raw_pointer raw)
    : msb_{msb}, var_{raw} {
  }

  /// appends a data value T to the list
  template <tenzir::detail::record_builder::non_structured_data_type T>
  void data(T d) {
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

  void data_unparsed(std::string_view s);
  /// appends a record to the list and returns a generator for the record
  auto record() -> record_generator;

  /// appends a list to the list and returns a generator for the list
  auto list() -> list_generator;

  /// append a null value to the list
  void null();

private:
  class multi_series_builder* msb_;
  std::variant<tenzir::builder_ref, raw_pointer> var_;
};

struct diagnostic_handler : tenzir::diagnostic_handler {
  virtual void emit(diagnostic d) override {
    diagnostics.push_back(std::move(d));
  }

  auto yield() -> std::vector<diagnostic> {
    return std::exchange(diagnostics, {});
  }
  std::vector<diagnostic> diagnostics;
};

auto series_to_table_slice(series array, std::string_view fallback_name
                                         = "tenzir.unknown") -> table_slice;
auto series_to_table_slice(std::vector<series> data,
                           std::string_view fallback_name
                           = "tenzir.unknown") -> std::vector<table_slice>;
} // namespace detail::multi_series_builder

class multi_series_builder {
public:
  friend class detail::multi_series_builder::record_generator;
  friend class detail::multi_series_builder::field_generator;
  friend class detail::multi_series_builder::list_generator;
  using record_generator = detail::multi_series_builder::record_generator;

  /// @returns a vector of all currently finished series
  [[nodiscard("The result of a flush must be handled")]]
  auto yield_ready() -> std::vector<series>;
  /// @returns a vector of all currently finished series
  [[nodiscard("The result of a flush must be handled")]]
  auto yield_ready_as_table_slice() -> std::vector<table_slice>;

  [[nodiscard("The result of a flush must be handled")]]
  auto last_errors() -> std::vector<tenzir::diagnostic>;

  /// adds a record to the currently active builder
  [[nodiscard]] auto record() -> record_generator;

  /// drops the last event from active builder
  void remove_last();

  [[nodiscard("The result of a flush must be handled")]]
  auto finalize() -> std::vector<series>;
  [[nodiscard("The result of a flush must be handled")]]
  auto finalize_as_table_slice() -> std::vector<table_slice>;

  // this policy will merge all events into a single schema
  // FIXME this does not correctly support --schema-only yet
  struct policy_merge {
    static constexpr std::string_view name = "merge";
    // a schema name to seed with. If this is given
    std::string seed_schema = {};

    auto friend inspect(auto& f, policy_merge& x) -> bool {
      return f.object(x).fields(f.field("seed_schema", x.seed_schema));
    }
  };

  // this policy will keep all schemas in separate batches
  struct policy_precise {
    static constexpr std::string_view name = "precise";
    // If this is given, all resulting events will have exactly this schema
    // * all fields in the schema but not in the event will be null
    std::string seed_schema = {};

    auto friend inspect(auto& f, policy_precise& x) -> bool {
      return f.object(x).fields(f.field("seed_schema", x.seed_schema));
    }
  };

  // this policy will keep all schemas in batches according to selector
  struct policy_selector {
    static constexpr std::string_view name = "selector";
    // the field name to use for selection
    std::string field_name;
    // a naming prefix, doing the following transformation on the name:
    // selector("event_type", "suricata")
    // => {"event_type": "flow"}
    // => "suricata.flow"
    std::optional<std::string> naming_prefix = std::nullopt;
    bool unique_selector = false;

    auto friend inspect(auto& f, policy_selector& x) -> bool {
      return f.object(x).fields(f.field("field_name", x.field_name),
                                f.field("naming_prefix", x.naming_prefix),
                                f.field("unique_selector", x.unique_selector));
    }
  };

  using policy_type
    = tenzir::variant<policy_merge, policy_precise, policy_selector>;

  struct settings_type {
    // the default name given to a schema
    std::string parser_name = "tenzir.unknown";
    // whether the output should adhere to the input order
    bool ordered = true;
    // whether a known schema should be expanded.
    bool schema_only = false;
    // whether to not parse fields that are not present in a schema
    bool raw = false;
    // unnest separator to be used when calling any `field` in the builder pattern
    std::string unnest_separator = {};
    // timeout after which events will be yielded regardless of whether the
    // desired batch size has been reached
    duration timeout = defaults::import::batch_timeout;
    // batch size after which the events should be yielded
    size_t desired_batch_size = defaults::import::table_slice_size;

    auto friend inspect(auto& f, settings_type& x) -> bool {
      return f.object(x).fields(
        f.field("default_name", x.parser_name), f.field("ordered", x.ordered),
        f.field("expand_schema", x.schema_only), f.field("raw", x.raw),
        f.field("unnest_separator", x.unnest_separator),
        f.field("timeout", x.timeout),
        f.field("desired_batch_size", x.desired_batch_size));
    }
  };

  template <detail::record_builder::data_parsing_function Parser
            = decltype(detail::record_builder::basic_parser)>
  multi_series_builder(policy_type policy, settings_type settings,
                       std::vector<type> schemas = {},
                       Parser&& parser = detail::record_builder::basic_parser)
    : policy_{std::move(policy)},
      settings_{std::move(settings)},
      builder_raw_{std::forward<Parser>(parser), dh_.get(),
                   settings_.schema_only, settings_.raw} {
    schemas_.reserve(schemas.size());
    for (auto t : schemas) {
      const auto [it, success] = schemas_.try_emplace(t.name(), std::move(t));
      TENZIR_ASSERT(success, "Repeated schema name");
    }
    // setup the merging builder in merging mode
    if (auto p = get_policy<policy_merge>()) {
      settings_.ordered = true; // merging mode is necessarily ordered
      if (auto seed = type_for_schema(p->seed_schema)) {
        merging_builder_ = seed;
      } else {
        merging_builder_ = tenzir::type{ p->seed_schema, null_type{} };
      }
    }
    // setup the naming sentinel for naming builders in precise mode
    else if (auto p = get_policy<policy_precise>()) {
      settings_.ordered = true; // merging mode is necessarily ordered
      if (auto seed = type_for_schema(p->seed_schema)) {
        naming_sentinel_ = *seed;
        needs_signature_ = true;
      } else {
        naming_sentinel_ = tenzir::type{ p->seed_schema, null_type{} };
      }
    }
  }

  // BE AWARE THAT MOVING A MULTI_SERIES_BUILDER MAY
  // INVALIDATE PREVIOUSLY OBTAINED HANDLES
  multi_series_builder(multi_series_builder&&) = default;

private:
  /// gets a pointer to the active policy, if its the given one.
  /// the implementation is in the source file, since its a private/internal
  /// function and thus will only be instantiated by other member functions
  template <typename T>
  T* get_policy() {
    return std::get_if<T>(&policy_);
  }

  // called internally once an event is complete.
  // this function is responsible for committing
  // the currently built event to its respective `series_builder`
  // this is only relevant for the precise mode
  void complete_last_event();

  // clears the currently build raw event
  void clear_raw_event();

  // gets the next free index into `entries_`.
  std::optional<size_t> next_free_index() const;

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

  /// finishes all events that satisfy the predicate.
  /// these events are moved out of their respective series_builders and into
  /// `ready_events_`
  /// the implementation is in the source file, since its a private/internal
  /// function and thus will only be instantiated by other member functions
  void make_events_available_where(std::predicate<const entry_data&> auto pred);

  /// appends `new_events` to `ready_events_`
  /// TODO Improvement: The `series_builder` could take in a vector instead of
  /// returning one from flush(). That way would could write into an existing
  /// allocation
  void append_ready_events(std::vector<series>&& new_events);

  /// GCs `series_builders` from `entries_` that satisfy the predicate
  /// the implementation is in the source file, since its a private/internal
  /// function and thus will only be instantiated by other member functions
  void garbage_collect_where(std::predicate<const entry_data&> auto pred);

  constexpr static size_t invalid_index = static_cast<size_t>(-1);
  using signature_type = typename record_builder::signature_type;

  struct schema_lookup_element;

  policy_type policy_;
  settings_type settings_;
  detail::flat_map<std::string, tenzir::type> schemas_;
  std::unique_ptr<detail::multi_series_builder::diagnostic_handler> dh_
    = std::make_unique<
      detail::multi_series_builder::diagnostic_handler>(); // must be
                                                           // initialized before
                                                           // builder_raw_
  record_builder builder_raw_;
  bool needs_signature_ = true; // used to determine whether we need a signature compute
  tenzir::type naming_sentinel_; // used to name builders
  signature_type signature_raw_;
  tsl::robin_map<signature_type, size_t, detail::hash_algorithm_proxy<>>
    signature_map_;
  series_builder merging_builder_;
  std::vector<entry_data> entries_;
  std::vector<series> ready_events_;
  std::chrono::steady_clock::time_point last_yield_time_
    = std::chrono::steady_clock::now();
  size_t active_index_ = 0;
};
} // namespace tenzir
