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

using parser_function_type = std::function<caf::expected<tenzir::data>(
  std::string_view, const tenzir::type*)>;

namespace detail::multi_series_builder {

using signature_type = std::vector<std::byte>;

class list_generator;
class field_generator;

class record_generator {
  using raw_pointer = detail::record_builder::node_record*;

public:
  explicit record_generator(tenzir::record_ref builder,
                            parser_function_type* parser)
    : var_{std::in_place_type<series_builder_element>, builder, parser} {
  }
  explicit record_generator(raw_pointer raw) : var_{raw} {
  }
  /// adds a new field to the record and returns a generator for that field
  auto field(std::string_view name) -> field_generator;

  auto unflattend_field(std::string_view key,
                        std::string_view unflatten) -> field_generator;

private:
  struct series_builder_element {
    tenzir::record_ref ref;
    parser_function_type* parser;
  };
  std::variant<series_builder_element, raw_pointer> var_;
};

class field_generator {
  using raw_pointer = detail::record_builder::node_field*;

public:
  /// A non-associated field generator. BE CAREFUL WITH THIS.
  field_generator() : field_generator(nullptr) {
  }
  field_generator(builder_ref builder, parser_function_type* parser)
    : var_{std::in_place_type<series_builder_element>, builder, parser} {
  }
  field_generator(raw_pointer raw) : var_{raw} {
  }

  /// sets the value of the field to some data
  template <tenzir::detail::record_builder::non_structured_data_type T>
  void data(T d) {
    const auto visitor = detail::overload{
      [&](series_builder_element& b) {
        b.ref.data(d);
      },
      [&](raw_pointer raw) {
        raw->data(d);
      },
    };
    return std::visit(visitor, var_);
  }

  void data_unparsed(std::string_view s) {
    const auto visitor = detail::overload{
      [&](series_builder_element& b) {
        auto res = (*b.parser)(s, nullptr);
        TENZIR_ASSERT(res);
        b.ref.data(*res);
      },
      [&](raw_pointer raw) {
        raw->data_unparsed(std::move(s));
      },
    };
    return std::visit(visitor, var_);
  }

  /// sets the value of the field an empty record and returns a generator for
  /// the record
  auto record() -> record_generator;

  /// sets the value of the field an empty list and returns a generator for the
  /// list
  auto list() -> list_generator;

  /// sets the value of the field to null
  void null();

private:
  struct series_builder_element {
    tenzir::builder_ref ref;
    parser_function_type* parser;
  };
  std::variant<series_builder_element, raw_pointer> var_;
};

class list_generator {
  using raw_pointer = detail::record_builder::node_list*;

public:
  list_generator(builder_ref builder, parser_function_type* parser)
    : var_{std::in_place_type<series_builder_element>, builder, parser} {
  }
  list_generator(raw_pointer raw) : var_{raw} {
  }

  /// appends a data value T to the list
  template <tenzir::detail::record_builder::non_structured_data_type T>
  void data(T d) {
    const auto visitor = detail::overload{
      [&](series_builder_element& b) {
        b.ref.data(d);
      },
      [&](raw_pointer raw) {
        raw->data(d);
      },
    };
    return std::visit(visitor, var_);
  }

  void data_unparsed(std::string_view s) {
    const auto visitor = detail::overload{
      [&](series_builder_element& b) {
        auto res = (*b.parser)(s, nullptr);
        TENZIR_ASSERT(res);
        b.ref.data(*res);
      },
      [&](raw_pointer raw) {
        raw->data_unparsed(s);
      },
    };
    return std::visit(visitor, var_);
  }
  /// appends a record to the list and returns a generator for the record
  auto record() -> record_generator;

  /// appends a list to the list and returns a generator for the list
  auto list() -> list_generator;

  /// append a null value to the list
  void null();

private:
  struct series_builder_element {
    tenzir::builder_ref ref;
    parser_function_type* parser;
  };
  std::variant<series_builder_element, raw_pointer> var_;
};

inline auto
get_schemas_unnested(bool actually_do_it, bool unflatten) -> std::vector<type> {
  std::vector<type> ret;
  if (not actually_do_it) {
    return ret;
  }
  ret = modules::schemas();
  if (not unflatten) {
    return ret;
  }
  constexpr static auto flatten_in_place = [](type& t) {
    t = flatten(t);
  };
  std::ranges::for_each(ret, flatten_in_place);
  return ret;
}

} // namespace detail::multi_series_builder

auto series_to_table_slice(series array, std::string_view fallback_name
                                         = "tenzir.unknown") -> table_slice;
auto series_to_table_slice(std::vector<series> data,
                           std::string_view fallback_name
                           = "tenzir.unknown") -> std::vector<table_slice>;

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
  auto last_errors() -> std::vector<caf::error>;

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
    std::optional<std::string> seed_schema = {};

    auto friend inspect(auto& f, policy_merge& x) -> bool {
      return f.object(x).fields(f.field("seed_schema", x.seed_schema));
    }
  };

  // this policy will keep all schemas in separate batches
  struct policy_precise {
    static constexpr std::string_view name = "precise";
    // If this is given, all resulting events will have exactly this schema
    // * all fields in the schema but not in the event will be null
    std::optional<std::string> seed_schema = {};

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
    std::optional<std::string> naming_prefix = {};

    auto friend inspect(auto& f, policy_selector& x) -> bool {
      return f.object(x).fields(f.field("field_name", x.field_name),
                                f.field("naming_prefix", x.naming_prefix));
    }
  };

  using policy_type
    = tenzir::variant<policy_merge, policy_precise, policy_selector>;

  struct settings_type {
    // the default name given to a schema
    std::string default_name = "tenzir.unknown";
    // whether the output should adhere to the input order
    bool ordered = true;
    // if the given policy finds a schema, only fields from that schema should
    // be present in the output and extra fields should be discarded
    bool schema_only = false;
    // timeout after which events will be yielded regardless of whether the
    // desired batch size has been reached
    duration timeout = defaults::import::batch_timeout;
    // batch size after which the events should be yielded
    size_t desired_batch_size = defaults::import::table_slice_size;

    auto friend inspect(auto& f, settings_type& x) -> bool {
      return f.object(x).fields(
        f.field("default_name", x.default_name), f.field("ordered", x.ordered),
        f.field("schema_only", x.schema_only), f.field("timeout", x.timeout),
        f.field("desired_batch_size", x.desired_batch_size));
    }
  };

  template <detail::record_builder::data_parsing_function Parser>
  multi_series_builder(policy_type policy, settings_type settings,
                       Parser&& parser, std::vector<type> schemas = {})
    : policy_{std::move(policy)},
      settings_{std::move(settings)},
      parser_{std::forward<Parser>(parser)} {
    schemas_.reserve(schemas.size());
    for (auto t : schemas) {
      const auto [it, success] = schemas_.try_emplace(t.name(), std::move(t));
      TENZIR_ASSERT(success, "Repeated schema name");
    }
    if (auto p = get_policy<policy_merge>()) {
      settings_.ordered = true; // merging mode is necessarily ordered
      merging_builder_ = series_builder{
        type_for_schema(p->seed_schema.value_or(settings_.default_name))};
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
    entry_data(std::string name, const tenzir::type* schema = nullptr)
      : name{std::move(name)},
        builder{schema},
        flushed{std::chrono::steady_clock::now()} {
    }

    auto flush() -> std::vector<series> {
      flushed = std::chrono::steady_clock::now();
      return builder.finish();
    }

    std::string name;
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
  parser_function_type parser_;
  detail::flat_map<std::string, tenzir::type> schemas_;

  record_builder builder_raw_;
  signature_type signature_raw_;
  tsl::robin_map<signature_type, size_t, detail::hash_algorithm_proxy<>>
    signature_map_;
  series_builder merging_builder_;
  std::vector<entry_data> entries_;
  std::vector<series> ready_events_;
  std::vector<caf::error> errors_;
  std::chrono::steady_clock::time_point last_yield_time_
    = std::chrono::steady_clock::now();
  size_t active_index_ = 0;
};
} // namespace tenzir
