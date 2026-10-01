//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/multi_series_builder_argument_parser.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/record_array_builder.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_path.hpp"
#include "tenzir/variant.hpp"

#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::nova {

/// Builds events from loosely typed input, such as JSON, into `Events`.
///
/// Compared to a plain `ArrayBuilder<Record>`, it additionally
/// - parses strings that the input left unparsed (`data_unparsed`),
/// - applies a schema given up front while building each event,
/// - picks a schema per event through a selector field, applied when the
///   batch is finished,
/// - unflattens keys at a separator,
/// - collects the values of a repeated key into a list, unless a policy says
///   which schema the input follows, and
/// - names the resulting events.
///
/// Parsing functions produce arbitrary values rather than events: they start
/// each row with `value()` instead of `event()` and finish with
/// `finish_data()`. Policies then apply to the rows that hold records, while
/// other values keep their type.
class EventBuilder {
public:
  /// Infers types and names all events `default_schema_name`.
  struct NoPolicy {};

  /// Applies the schema `name` to every event.
  struct SchemaPolicy {
    std::string name;
  };

  /// Applies the schema named by the value of the field `field`, which may be
  /// a dotted path, optionally prefixed with `prefix` and a dot.
  struct SelectorPolicy {
    std::string field;
    Option<std::string> prefix;
  };

  using Policy = variant<NoPolicy, SchemaPolicy, SelectorPolicy>;

  struct Settings {
    Policy policy = NoPolicy{};
    /// Drops fields that are not part of the applied schema.
    bool schema_only = false;
    /// Keeps unparsed strings as strings unless a schema says otherwise.
    bool raw = false;
    /// Infers numbers in unparsed strings for formats without numeric types.
    bool infer_numbers = false;
    /// Field segments of explicit string paths; selectors do not infer them.
    std::vector<std::vector<std::string>> string_fields = {};
    /// Splits keys at this separator into nested records, if not empty.
    std::string unflatten_separator;
    std::string default_schema_name = "tenzir.unknown";
    /// Restricts inference to this top-level field and enables number parsing.
    /// Other strings stay strings. Empty leaves inference unrestricted.
    /// Schema declarations and `raw` take precedence.
    std::string infer_unparsed_under = {};
    /// Retains repeated values even when a schema policy is active.
    bool merge_structural = false;
  };

  class Field;
  class List;

private:
  /// Where a `Field` writes to: a field of a record, or a top-level row.
  class Slot {
  public:
    explicit Slot(FieldBuilder inner);
    explicit Slot(ArrayBuilder<Data>& inner);

    template <fundamental_view_type V>
    auto data(V v) -> void;

    auto data(Time v) -> void {
      data<Time>(v);
    }

    auto data(std::string_view v) -> void {
      data<std::string_view>(v);
    }

    auto null() -> void;
    auto record() -> ArrayBuilder<nova::Record>::RecordBuilder;
    auto list() -> ArrayBuilder<nova::List>::ListBuilder;

  private:
    variant<FieldBuilder, Ref<ArrayBuilder<Data>>> inner_;
  };

public:
  /// The open event, or a record nested within it.
  class Record {
  public:
    auto field(std::string_view name) -> Field;
    /// Writes a literal field name, bypassing unflattening.
    auto exact_field(std::string_view name) -> Field;

  private:
    friend class EventBuilder;
    friend class Field;
    friend class List;
    enum class State { fresh, reopened };

    Record(EventBuilder& parent,
           Option<ArrayBuilder<nova::Record>::RecordBuilder> inner, type seed,
           value_path path, State state = State::fresh);

    auto descend(std::string_view key, value_path const& path) -> Field;
    auto lookup(std::string_view name, value_path const& path) -> Field;
    auto lookup_record(std::string_view name, value_path const& path) -> Record;

    Ref<EventBuilder> parent_;
    /// Absent if everything written to this record is discarded.
    Option<ArrayBuilder<nova::Record>::RecordBuilder> inner_;
    /// The `record_type` this record must conform to, or a null type.
    type seed_;
    value_path path_;
  };

  /// A field of a `Record`.
  class Field {
  public:
    template <fundamental_view_type V>
    auto data(V v) -> void;

    auto data(Time v) -> void {
      data<Time>(v);
    }

    auto data(std::string_view v) -> void {
      data<std::string_view>(v);
    }

    /// Writes a string whose type the input left unspecified.
    auto data_unparsed(std::string_view v) -> void;
    auto null() -> void;
    auto record() -> Record;
    auto list() -> List;

  private:
    friend class EventBuilder;
    friend class Record;
    Field(EventBuilder& parent, Option<Slot> inner, type seed, value_path path,
          Option<Data> previous = None{}, bool extend = false);

    /// Rewrites the previous value as the list of the key's values, to which
    /// the caller appends the new value.
    auto repeat() -> List;

    Ref<EventBuilder> parent_;
    Option<Slot> inner_;
    type seed_;
    value_path path_;
    /// The value that the record already held for a repeated key.
    Option<Data> previous_;
    /// Whether `previous_` is the list of an already repeated key's values.
    bool extend_ = false;
  };

  /// A list within an event.
  class List {
  public:
    template <fundamental_view_type V>
    auto data(V v) -> void;

    auto data(Time v) -> void {
      data<Time>(v);
    }

    auto data(std::string_view v) -> void {
      data<std::string_view>(v);
    }

    /// Writes a string whose type the input left unspecified.
    auto data_unparsed(std::string_view v) -> void;
    auto null() -> void;
    auto record() -> Record;
    auto list() -> List;

  private:
    friend class Field;
    friend class List;
    List(EventBuilder& parent,
         Option<ArrayBuilder<nova::List>::ListBuilder> inner, type seed,
         value_path path);

    Ref<EventBuilder> parent_;
    Option<ArrayBuilder<nova::List>::ListBuilder> inner_;
    /// The type of the elements, or a null type.
    type seed_;
    value_path path_;
  };

  /// Resolves the schema of a `SchemaPolicy`. Fails for a schema that is not
  /// a record, or that does not exist while `schema_only` is set.
  static auto make(Settings settings, diagnostic_handler& dh)
    -> failure_or<EventBuilder>;
  /// Like `make`, for settings that `make` already accepted once. Does not
  /// repeat the diagnostics about the schema.
  static auto make_prevalidated(Settings settings, diagnostic_handler& dh)
    -> EventBuilder;

  EventBuilder(EventBuilder&&) noexcept;
  auto operator=(EventBuilder&&) noexcept -> EventBuilder&;
  EventBuilder(EventBuilder const&) = delete;
  auto operator=(EventBuilder const&) -> EventBuilder& = delete;
  ~EventBuilder();

  /// Starts a new event.
  auto event() -> Record;
  /// Starts a new row that may hold any value, not only a record.
  auto value() -> Field;
  /// Adds a row without a value.
  auto skip() -> void;
  /// Removes the last row, which must hold a value.
  auto discard_last() -> void;
  /// Returns the number of rows, including a row that is still open.
  auto length() const -> storage::Index;
  /// Returns the built events and resets the builder for the next batch. All
  /// rows must have been started with `event()`.
  auto finish() -> Events;
  /// Returns the built rows and resets the builder for the next batch. Event
  /// names are discarded.
  auto finish_data() -> Array<Data>;

private:
  EventBuilder(Settings settings, diagnostic_handler& dh);

  /// Returns the record schema named `name`, looked up once.
  auto schema(std::string const& name) -> Option<type> const&;
  /// Returns the slot for `name` in `record`. Without a policy, a repeated key
  /// collects its values into a list: then this stores the value it held in
  /// `previous`, and whether that value is already the list of the key's
  /// values in `extend`. With a policy, the last value wins unless structural
  /// merging is enabled.
  auto take(ArrayBuilder<nova::Record>::RecordBuilder& record,
            std::string_view name, Option<Data>& previous, bool& extend,
            bool record_prefix = false) -> FieldBuilder;
  auto open_record(ArrayBuilder<nova::Record>::RecordBuilder& record,
                   std::string_view name)
    -> Option<ArrayBuilder<nova::Record>::RecordBuilder>;
  /// Finishes the rows and applies the policy. Stores the event names in
  /// `names` if a selector picked them.
  auto finish_array(Option<Array<String>>& names) -> Array<Data>;
  auto finish_selected(Array<Data> array, storage::BitMap rows)
    -> std::pair<Array<Data>, Array<String>>;

  Settings settings_;
  Ref<diagnostic_handler> dh_;
  /// Whether unseeded unparsed strings stay strings while building.
  bool raw_ = false;
  /// The schema applied while building, or a null type.
  type seed_;
  /// The name of the events unless a selector picks one.
  std::string name_;
  ArrayBuilder<Data> builder_;
  /// Rows that were started rather than skipped.
  storage::BitMap::Builder rows_;
  std::unordered_map<std::string, Option<type>> schemas_;
  /// A key that repeated within the open event, identified by the builder
  /// and row of its record.
  struct RepeatedKey {
    void const* builder;
    storage::Index row;
    std::string name;

    friend auto operator==(RepeatedKey const&, RepeatedKey const&) -> bool
      = default;
  };
  std::vector<RepeatedKey> repeated_;
};

/// Checks event-builder-specific options after shared parser validation.
template <class Args>
auto validate_event_builder_options(msb_validator<Args> const& options,
                                    DescribeCtx& ctx) -> Empty {
  if (nova_enabled()) {
    if (auto loc = ctx.get_location(options.merge)) {
      diagnostic::warning("`merge` has no effect with `--nova`")
        .primary(*loc)
        .emit(ctx);
    }
  }
  return {};
}

/// Returns the settings that correspond to the options of the
/// `multi_series_builder`, which the parser arguments produce.
auto event_builder_settings(multi_series_builder::options const& options)
  -> EventBuilder::Settings;

// -- Function arguments -------------------------------------------------------

/// Controls which options `add_event_builder_to_describer` registers.
struct EventBuilderDescriberOptions {
  merge_option merge = merge_option::hidden;
  bool add_schema = true;
  bool add_selector = true;
  bool add_unflatten = true;
  bool schema_only_requires_schema_or_selector = true;
};

/// The locations of the options that `add_event_builder_to_describer`
/// registers, as far as a call provided them.
struct EventBuilderLocations {
  Option<location> schema;
  Option<location> selector;
  Option<location> schema_only;
};

/// Checks the combination of options that the setters of
/// `add_event_builder_to_describer` could not check on their own, and resolves
/// the schema once.
auto validate_event_builder_settings(EventBuilder::Settings const& settings,
                                     EventBuilderLocations const& locations,
                                     bool schema_only_requires_policy,
                                     diagnostic_handler& dh)
  -> failure_or<void>;

namespace _ {

auto set_schema(EventBuilder::Settings& settings, located<std::string> value,
                diagnostic_handler& dh) -> failure_or<void>;
auto set_selector(EventBuilder::Settings& settings, located<std::string> value,
                  diagnostic_handler& dh) -> failure_or<void>;
auto set_unflatten_separator(EventBuilder::Settings& settings,
                             located<std::string> value, diagnostic_handler& dh)
  -> failure_or<void>;

} // namespace _

/// Returned by `add_event_builder_to_describer`, and passed to the
/// describer's `validate`, either directly or from a function's own check.
template <class Args>
struct EventBuilderValidator {
  EventBuilder::Settings Args::* settings;
  Option<NamedArgument> schema;
  Option<NamedArgument> selector;
  NamedArgument schema_only;
  bool schema_only_requires_policy = true;

  auto operator()(Args const& args, FunctionValidateCtx& ctx) const
    -> failure_or<void> {
    auto locations = EventBuilderLocations{
      .schema = schema ? ctx.get_location(*schema) : None{},
      .selector = selector ? ctx.get_location(*selector) : None{},
      .schema_only = ctx.get_location(schema_only),
    };
    return validate_event_builder_settings(args.*settings, locations,
                                           schema_only_requires_policy, ctx);
  }
};

/// Registers the options of the event builder, such as `schema` and
/// `selector`, on a function. The options write into the `settings` member.
template <class Args, class Impl>
auto add_event_builder_to_describer(FunctionDescriber<Args, Impl>& d,
                                    EventBuilder::Settings Args::* settings,
                                    EventBuilderDescriberOptions options = {})
  -> EventBuilderValidator<Args> {
  using Setter = std::function<
    auto(Args&, located<std::string>, diagnostic_handler&)->failure_or<void>>;
  using FlagSetter = std::function<
    auto(Args&, located<bool>, diagnostic_handler&)->failure_or<void>>;
  auto forward = [settings](auto f) -> Setter {
    return [settings, f](Args& args, located<std::string> value,
                         diagnostic_handler& dh) -> failure_or<void> {
      return f(args.*settings, std::move(value), dh);
    };
  };
  auto flag = [settings](bool EventBuilder::Settings::* member) -> FlagSetter {
    return [settings, member](Args& args, located<bool> value,
                              diagnostic_handler&) -> failure_or<void> {
      if (value.inner) {
        (args.*settings).*member = true;
      }
      return {};
    };
  };
  auto result = EventBuilderValidator<Args>{
    .settings = settings,
    .schema = None{},
    .selector = None{},
    .schema_only = {},
    .schema_only_requires_policy
    = options.schema_only_requires_schema_or_selector,
  };
  if (options.add_schema) {
    result.schema = d.template named_with_setter<std::string>(
      "schema", forward(&_::set_schema));
  }
  if (options.add_selector) {
    result.selector = d.template named_with_setter<std::string>(
      "selector", forward(&_::set_selector));
  }
  result.schema_only = d.template named_with_setter<bool>(
    "schema_only", flag(&EventBuilder::Settings::schema_only));
  if (options.merge != merge_option::no) {
    // Values of different types coexist without merging their schemas, so
    // the option only remains for compatibility.
    auto ignore
      = [](Args&, located<bool>, diagnostic_handler&) -> failure_or<void> {
      return {};
    };
    d.template named_with_setter<bool>(
      options.merge == merge_option::yes ? "merge" : "_merge", ignore);
  }
  d.template named_with_setter<bool>("raw", flag(&EventBuilder::Settings::raw));
  if (options.add_unflatten) {
    d.template named_with_setter<std::string>(
      "unflatten_separator", forward(&_::set_unflatten_separator));
  }
  return result;
}

} // namespace tenzir::nova
