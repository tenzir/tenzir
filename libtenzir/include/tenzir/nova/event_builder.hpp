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
#include "tenzir/nova/record_array_builder.hpp"
#include "tenzir/nova_flag.hpp"
#include "tenzir/option.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_path.hpp"
#include "tenzir/variant.hpp"

#include <string>
#include <string_view>
#include <unordered_map>
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
    /// Splits keys at this separator into nested records, if not empty.
    std::string unflatten_separator;
    std::string default_schema_name = "tenzir.unknown";
    /// Restricts inference to this top-level field and enables number parsing.
    /// Other strings stay strings. Empty keeps the default non-number
    /// inference. Schema declarations and `raw` take precedence.
    std::string infer_unparsed_under = {};
    /// Merges repeated records and concatenates repeated lists. Parsed scalars,
    /// including nulls, colliding with records use an empty field name.
    /// Applies before schema policies, except `SchemaPolicy` with `schema_only`.
    bool merge_structural = false;
  };

  class Field;
  class List;

  /// The open event, or a record nested within it.
  class Record {
  public:
    auto field(std::string_view name) -> Field;

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
    friend class Record;
    Field(EventBuilder& parent, Option<FieldBuilder> inner, type seed,
          value_path path, Option<Data> previous = None{}, bool extend = false);

    /// Rewrites the previous value as the list of the key's values, to which
    /// the caller appends the new value.
    auto repeat() -> List;

    Ref<EventBuilder> parent_;
    Option<FieldBuilder> inner_;
    type seed_;
    value_path path_;
    /// The value that the record already held for a repeated key.
    Option<Data> previous_;
    /// Whether to extend `previous_` as a list rather than keep it as an element.
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

  EventBuilder(EventBuilder&&) noexcept;
  auto operator=(EventBuilder&&) noexcept -> EventBuilder&;
  EventBuilder(EventBuilder const&) = delete;
  auto operator=(EventBuilder const&) -> EventBuilder& = delete;
  ~EventBuilder();

  /// Starts a new event.
  auto event() -> Record;
  /// Returns the number of events, including an event that is still open.
  auto length() const -> storage::Index;
  /// Returns the built events and resets the builder for the next batch.
  auto finish() -> Events;

private:
  EventBuilder(Settings settings, diagnostic_handler& dh);

  /// Returns the record schema named `name`, looked up once.
  auto schema(std::string const& name) -> Option<type> const&;
  /// Returns the slot for `name` in `record`, preserving repeated values in
  /// `previous` for collection or structural merging. `extend` determines
  /// whether to extend that value as a list or keep it as an element. Policies
  /// overwrite unless structural merging is enabled, except schema-only fixed
  /// schemas, which still overwrite.
  auto take(ArrayBuilder<nova::Record>::RecordBuilder& record,
            std::string_view name, Option<Data>& previous, bool& extend)
    -> FieldBuilder;
  auto open_record(ArrayBuilder<nova::Record>::RecordBuilder& record,
                   std::string_view name)
    -> Option<ArrayBuilder<nova::Record>::RecordBuilder>;
  auto finish_selected(Array<nova::Record> array) -> Events;

  Settings settings_;
  Ref<diagnostic_handler> dh_;
  /// Whether unseeded unparsed strings stay strings while building.
  bool raw_ = false;
  /// The schema applied while building, or a null type.
  type seed_;
  /// The name of the events unless a selector picks one.
  std::string name_;
  ArrayBuilder<nova::Record> builder_;
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

} // namespace tenzir::nova
