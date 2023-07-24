//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/adaptive_table_slice_builder.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/config_options.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/detail/padded_buffer.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql/parser.hpp>

#include <arrow/record_batch.h>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <chrono>
#include <simdjson.h>

namespace tenzir::plugins::json {

namespace {

/// A variant of *to_lines* that returns a string view with additional padding
/// bytes that are safe to read.
inline auto to_padded_lines(generator<chunk_ptr> input)
  -> generator<std::optional<simdjson::padded_string_view>> {
  auto buffer = std::string{};
  bool ended_on_linefeed = false;
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    if (ended_on_linefeed && *begin == '\n') {
      ++begin;
    };
    ended_on_linefeed = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' && *current != '\r') {
        continue;
      }
      const auto capacity = static_cast<size_t>(end - begin);
      const auto size = static_cast<size_t>(current - begin);
      if (buffer.empty() and capacity >= size + simdjson::SIMDJSON_PADDING) {
        co_yield simdjson::padded_string_view{begin, size, capacity};
      } else {
        buffer.append(begin, current);
        buffer.reserve(buffer.size() + simdjson::SIMDJSON_PADDING);
        co_yield simdjson::padded_string_view{buffer};
        buffer.clear();
      }
      if (*current == '\r') {
        auto next = current + 1;
        if (next == end) {
          ended_on_linefeed = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer.append(begin, end);
    co_yield std::nullopt;
  }
  if (!buffer.empty()) {
    buffer.reserve(buffer.size() + simdjson::SIMDJSON_PADDING);
    co_yield simdjson::padded_string_view{buffer};
  }
}

enum class parser_action {
  skip = 0,
  yield = 1,
  parse = 2,
};

struct selector {
  std::string prefix;
  std::string selector_field;

  template <class Inspector>
  friend auto inspect(Inspector& f, selector& x) -> bool {
    return f.object(x)
      .pretty_name("selector")
      .fields(f.field("prefix", x.prefix),
              f.field("selector_field", x.selector_field));
  }
};

struct entry_data {
  template <class... Ts>
  explicit entry_data(std::string name, Ts&&... xs)
    : name{std::move(name)},
      builder{std::make_unique<adaptive_table_slice_builder>(
        std::forward<Ts>(xs)...)},
      flushed{std::chrono::steady_clock::now()} {
  }

  template <class... Ts>
  entry_data(std::string_view name, Ts&&... xs)
    : entry_data{std::string{name}, std::forward<Ts>(xs)...} {
  }

  auto flush(operator_control_plane& ctrl) -> table_slice {
    flushed = std::chrono::steady_clock::now();
    auto slice = builder->finish(name);
    if (expected_rows != slice.rows()) {
      // TODO: Remove this check once the parser is reliable.
      diagnostic::warning("JSON parser detected internal error: expected {} "
                          "rows but got {}",
                          expected_rows, slice.rows())
        .note("this is a known issue and we are already working on a fix")
        .emit(ctrl.diagnostics());
    }
    expected_rows = 0;
    return slice;
  }

  std::string name;
  // TODO: This is only a `unique_ptr` because it is currently not movable.
  std::unique_ptr<adaptive_table_slice_builder> builder;
  std::chrono::steady_clock::time_point flushed;
  // TODO: Remove this once `builder.finish()` is guarnateed to return the
  // correct number of rows.
  size_t expected_rows = 0;
};

constexpr auto unknown_entry_name = std::string_view{};

struct parser_state {
  explicit parser_state(operator_control_plane& ctrl, bool preserve_order)
    : ctrl_{ctrl}, preserve_order{preserve_order} {
  }

  operator_control_plane& ctrl_;
  /// Maps schema names to indices for the `entries` member.
  detail::heterogeneous_string_hashmap<size_t> entry_map;
  /// Stores the schema-specific builders and some additional metadata.
  std::vector<entry_data> entries;
  /// The index of the currently active or last used builder.
  size_t active_entry{};
  /// Used to communicate a need for a co_return in the operator coroutine from
  /// the ndjson parser/default parser coroutine.
  bool abort_requested = false;
  /// If this is false, then the JSON parser is allowed to reorder events
  /// between different schemas.
  bool preserve_order = true;

  auto get_entry(size_t idx) -> entry_data& {
    TENZIR_ASSERT_CHEAP(idx < entries.size());
    return entries[idx];
  }

  auto get_active_entry() -> entry_data& {
    return get_entry(active_entry);
  }

  /// Registers a new entry and returns its index.
  /// @pre An entry with this name must not exist yet.
  template <class... Ts>
  auto add_entry(Ts&&... xs) -> size_t {
    auto index = entries.size();
    auto& entry = entries.emplace_back(std::forward<Ts>(xs)...);
    auto inserted = entry_map.try_emplace(entry.name, index).second;
    TENZIR_ASSERT_CHEAP(inserted);
    return index;
  }

  auto find_entry(std::string_view name) -> std::optional<size_t> {
    auto it = entry_map.find(name);
    if (it == entry_map.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  /// Activates an entry after potentially flushing the active one.
  [[nodiscard]] auto activate(size_t entry) -> std::optional<table_slice> {
    if (entry == active_entry) {
      return std::nullopt;
    }
    auto result = std::optional<table_slice>{};
    if (preserve_order) {
      auto slice = get_entry(active_entry).flush(ctrl_);
      if (slice.rows() > 0) {
        result = std::move(slice);
      }
    }
    active_entry = entry;
    return result;
  }
};

template <class FieldValidator>
class doc_parser {
public:
  doc_parser(const FieldValidator& field_validator,
             std::string_view parsed_document, operator_control_plane& ctrl)
    : field_validator_{field_validator},
      parsed_document_{parsed_document},
      ctrl_{ctrl} {
  }

  doc_parser(const FieldValidator& field_validator,
             std::string_view parsed_document, operator_control_plane& ctrl,
             std::size_t parsed_lines)
    : field_validator_{field_validator},
      parsed_document_{parsed_document},
      ctrl_{ctrl},
      parsed_lines_{parsed_lines} {
  }

  [[nodiscard]] auto parse_object(simdjson::ondemand::value v,
                                  auto&& field_pusher, size_t depth = 0u)
    -> bool {
    auto obj = v.get_object().value_unsafe();
    for (auto pair : obj) {
      if (pair.error()) {
        report_parse_err(v, "key value pair");
        return false;
      }
      auto maybe_key = pair.unescaped_key();
      if (maybe_key.error()) {
        report_parse_err(v, "key in an object");
        return false;
      }
      auto key = maybe_key.value_unsafe();
      auto val = pair.value();
      if (val.error()) {
        report_parse_err(val, fmt::format("object value at key {}", key));
        return false;
      }
      auto field = field_pusher.push_field(key);
      if (not field_validator_(field))
        continue;
      if (not parse_impl(val.value_unsafe(), field, depth + 1)) {
        return false;
      }
    }
    return true;
  }

private:
  void emit_unparsed_json_diagnostics(
    std::string description,
    simdjson::simdjson_result<const char*> document_location) {
    auto document_to_truncate = parsed_document_;
    auto note_prefix = "somewhere in";
    if (not document_location.error()) {
      document_to_truncate = std::string_view{document_location.value_unsafe(),
                                              parsed_document_.end()};
      note_prefix = "at";
    }
    constexpr auto character_limit = 50u;
    if (document_to_truncate.length() > character_limit) {
      diagnostic::warning("failed to parse {} in the JSON document",
                          std::move(description))
        .note("{} {} ...", note_prefix,
              document_to_truncate.substr(0, character_limit))
        .emit(ctrl_.diagnostics());
    }
    diagnostic::warning("failed to parse {} in the JSON document",
                        std::move(description))
      .note("{} {}", note_prefix, document_to_truncate)
      .emit(ctrl_.diagnostics());
  }

  void report_parse_err(auto& v, std::string description) {
    if (parsed_lines_) {
      report_parse_err_with_parsed_lines(v, std::move(description));
      return;
    }
    emit_unparsed_json_diagnostics(std::move(description),
                                   v.current_location());
  }

  void report_parse_err_with_parsed_lines(auto& v, std::string description) {
    if (v.current_location().error()) {
      diagnostic::warning("failed to parse {} in the JSON document",
                          std::move(description))
        .note("line {}", *parsed_lines_)
        .emit(ctrl_.diagnostics());
      return;
    }
    auto column = v.current_location().value_unsafe() - parsed_document_.data();
    diagnostic::warning("failed to parse {} in the JSON document",
                        std::move(description))
      .note("line {} column {}", *parsed_lines_, column)
      .emit(ctrl_.diagnostics());
  }

  [[nodiscard]] auto parse_number(simdjson::ondemand::value val, auto& pusher)
    -> bool {
    switch (val.get_number_type().value_unsafe()) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        return add_value(pusher, result.value_unsafe());
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        return add_value(pusher, result.value_unsafe());
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        return add_value(pusher, result.value_unsafe());
      }
    }
    TENZIR_UNREACHABLE();
  }

  [[nodiscard]] auto parse_string(simdjson::ondemand::value val, auto& pusher)
    -> bool {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return false;
    }
    auto str = maybe_str.value_unsafe();
    using namespace parser_literals;
    static constexpr auto parser
      = parsers::time | parsers::duration | parsers::net | parsers::ip;
    data result;
    if (parser(str, result)) {
      return add_value(pusher, make_view(result));
    }
    // Take the input as-is if nothing worked.
    return add_value(pusher, str);
  }

  [[nodiscard]] auto parse_array(simdjson::ondemand::array arr, auto& pusher,
                                 size_t depth) -> bool {
    auto list = pusher.push_list();
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        return false;
      }
      if (not parse_impl(element.value_unsafe(), list, depth + 1)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] auto parse_impl(simdjson::ondemand::value val, auto& pusher,
                                size_t depth) -> bool {
    if (depth > defaults::max_recursion)
      die("nesting too deep in json_parser parse");
    auto type = val.type();
    if (type.error()) {
      report_parse_err(val, "a value");
      return false;
    }
    switch (type.value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        return true;
      case simdjson::ondemand::json_type::number:
        return parse_number(val, pusher);
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return false;
        }
        return add_value(pusher, result.value_unsafe());
      }
      case simdjson::ondemand::json_type::string:
        return parse_string(val, pusher);
      case simdjson::ondemand::json_type::array:
        return parse_array(val.get_array().value_unsafe(), pusher, depth + 1);
      case simdjson::ondemand::json_type::object:
        return parse_object(val, pusher.push_record(), depth + 1);
    }
    TENZIR_UNREACHABLE();
  }

  [[nodiscard]] auto add_value(auto& guard, auto value) -> bool {
    if (auto err = guard.add(value)) {
      ctrl_.warn(std::move(err));
      return false;
    }
    return true;
  }

  const FieldValidator& field_validator_;
  std::string_view parsed_document_;
  operator_control_plane& ctrl_;
  std::optional<std::size_t> parsed_lines_;
};

auto get_schema_name(simdjson::ondemand::document_reference doc,
                     const selector& selector) -> caf::expected<std::string> {
  auto type = doc[selector.selector_field];
  doc.rewind();
  if (auto err = type.error()) {
    if (err != simdjson::error_code::NO_SUCH_FIELD)
      return caf::make_error(ec::parse_error, error_message(err));
    return std::string{unknown_entry_name};
  }
  auto maybe_schema_name = type.value_unsafe().get_string();
  if (auto err = maybe_schema_name.error()) {
    return caf::make_error(ec::parse_error, error_message(err));
  }
  if (selector.prefix.empty()) {
    return std::string{maybe_schema_name.value_unsafe()};
  }
  return fmt::format("{}.{}", selector.prefix,
                     maybe_schema_name.value_unsafe());
}

auto non_empty_entries(parser_state& state)
  -> generator<std::reference_wrapper<entry_data>> {
  if (state.preserve_order) {
    // In that case, only the active builder can be non-empty.
    if (state.get_active_entry().builder->rows() > 0) {
      co_yield std::ref(state.get_active_entry());
    }
  } else {
    // Otherwise, builders are not flushed when changing schema. Thus, we have
    // to take a look at every entry.
    for (auto& entry : state.entries) {
      if (entry.builder->rows() > 0) {
        co_yield std::ref(entry);
      }
    }
  }
}

auto get_schemas(bool try_find_schema, operator_control_plane& ctrl,
                 bool unflatten) -> std::vector<type> {
  if (not try_find_schema)
    return {};
  if (not unflatten)
    return ctrl.schemas();
  auto schemas = ctrl.schemas();
  std::vector<type> ret;
  std::transform(schemas.begin(), schemas.end(), std::back_inserter(ret),
                 [](const auto& schema) {
                   return flatten(schema);
                 });
  return ret;
}

auto unflatten_if_needed(std::string_view separator, table_slice slice)
  -> table_slice {
  if (separator.empty())
    return slice;
  return unflatten(slice, separator);
}

[[nodiscard]] auto activate_unknown_entry(parser_state& state)
  -> std::optional<table_slice> {
  if (auto idx = state.find_entry(unknown_entry_name)) {
    return state.activate(*idx);
  }
  return state.activate(state.add_entry(unknown_entry_name));
}

template <class FieldValidator>
class parser_base {
public:
  parser_base(operator_control_plane& ctrl, std::optional<selector> selector,
              std::optional<type> schema, std::vector<type> schemas,
              FieldValidator field_validator, bool infer_types,
              bool preserve_order)
    : ctrl_{ctrl},
      selector_{std::move(selector)},
      schema_{std::move(schema)},
      schemas_{std::move(schemas)},
      field_validator_{std::move(field_validator)},
      infer_types_{infer_types},
      preserve_order{preserve_order} {
  }

protected:
  auto handle_schema_found(parser_state& state, const type& schema) const
    -> std::optional<table_slice> {
    // The case where this schema exists is already handled before.
    return state.activate(state.add_entry(schema.name(), schema, infer_types_));
  }

  auto handle_no_matching_schema_found(parser_state& state,
                                       std::string_view schema_name,
                                       std::string_view parsed_doc) const
    -> caf::expected<std::optional<table_slice>> {
    if (not infer_types_) {
      return caf::make_error(
        ec::parse_error, fmt::format("json parser failed to find schema for "
                                     "'{}' and skips the "
                                     "JSON object '{}'",
                                     schema_name, parsed_doc));
    }
    // The case where this schema exists is already handled before.
    return state.activate(state.add_entry(schema_name));
  }

  auto handle_schema_name_found(std::string_view schema_name,
                                std::string_view json_source,
                                parser_state& state) const
    -> caf::expected<std::optional<table_slice>> {
    if (auto idx = state.find_entry(schema_name)) {
      return state.activate(*idx);
    }
    auto schema_it
      = std::find_if(schemas_.begin(), schemas_.end(), [&](const auto& schema) {
          return schema.name() == schema_name;
        });
    if (schema_it == schemas_.end()) {
      return handle_no_matching_schema_found(state, schema_name, json_source);
    }
    return {handle_schema_found(state, *schema_it)};
  }

  auto
  handle_with_selector(simdjson::ondemand::document_reference doc_ref,
                       std::string_view json_source, parser_state& state) const
    -> std::pair<parser_action, std::optional<table_slice>> {
    TENZIR_ASSERT(not schema_);
    TENZIR_ASSERT(selector_);
    auto maybe_schema_name = get_schema_name(doc_ref, *selector_);
    if (not maybe_schema_name) {
      ctrl_.warn(std::move(maybe_schema_name.error()));
      if (not infer_types_)
        return {parser_action::skip, std::nullopt};
      auto maybe_slice_to_yield = activate_unknown_entry(state);
      if (maybe_slice_to_yield) {
        return {parser_action::yield, std::move(maybe_slice_to_yield)};
      }
      return {parser_action::parse, std::nullopt};
    }
    auto maybe_slice_to_yield
      = handle_schema_name_found(*maybe_schema_name, json_source, state);
    if (maybe_slice_to_yield) {
      if (auto slice = *maybe_slice_to_yield) {
        return {parser_action::yield, std::move(slice)};
      }
      return {parser_action::parse, std::nullopt};
    }
    ctrl_.warn(std::move(maybe_slice_to_yield.error()));
    return {parser_action::skip, std::nullopt};
  }

  auto handle_selector(simdjson::ondemand::document_reference doc_ref,
                       std::string_view json_source, parser_state& state) const
    -> std::pair<parser_action, std::optional<table_slice>> {
    if (not selector_) {
      return {parser_action::parse, std::nullopt};
    }
    return handle_with_selector(doc_ref, json_source, state);
  }

  auto handle_max_rows(parser_state& state) const
    -> std::optional<table_slice> {
    if (state.get_active_entry().builder->rows() < max_table_slice_rows_) {
      return std::nullopt;
    }
    return state.get_active_entry().flush(ctrl_);
  }

  operator_control_plane& ctrl_;
  std::optional<selector> selector_;
  std::optional<type> schema_;
  std::vector<type> schemas_;
  FieldValidator field_validator_;
  bool infer_types_ = true;
  bool preserve_order = true;
  simdjson::ondemand::parser parser_;
  // TODO: change max table slice size to be fetched from options.
  tenzir::detail::arrow_length_type max_table_slice_rows_
    = defaults::import::table_slice_size;
};

template <class FieldValidator>
class ndjson_parser : public parser_base<FieldValidator> {
public:
  using parser_base<FieldValidator>::parser_base;

  auto parse(simdjson::padded_string_view json_line, parser_state& state)
    -> generator<table_slice> {
    ++lines_processed_;
    auto maybe_doc = this->parser_.iterate(json_line);
    auto val = maybe_doc.get_value();
    // val.error() will inherit all errors from maybe_doc. No need to check
    // for error after each operation.
    if (auto err = val.error()) {
      this->ctrl_.warn(caf::make_error(
        ec::parse_error, fmt::format("skips invalid JSON '{}' : {}", json_line,
                                     error_message(err))));
      co_return;
    }
    auto& doc = maybe_doc.value_unsafe();
    auto [action, slice] = this->handle_selector(doc, json_line, state);
    switch (action) {
      case parser_action::parse:
        break;
      case parser_action::skip:
        co_return;
      case parser_action::yield:
        TENZIR_ASSERT(slice);
        co_yield std::move(*slice);
    }
    {
      // The `row` gets it's own scope so that it is destroyed before we finish.
      auto row = state.get_active_entry().builder->push_row();
      auto success = doc_parser{this->field_validator_, json_line, this->ctrl_,
                                lines_processed_}
                       .parse_object(val.value_unsafe(), row);
      // After parsing one JSON object it is expected for the result to be at
      // the end. If it's otherwise then it means that a line contains more than
      // one object in which case we don't add any data and emit a warning.
      // It is also possible for a parsing failure to occurr in doc_parser. the
      // is_alive() call ensures that the first object was parsed without
      // errors. Calling at_end() when is_alive() returns false is unsafe and
      // resulted in crashes.
      if (success and not doc.at_end()) {
        this->ctrl_.warn(caf::make_error(
          ec::parse_error, fmt::format("more than one JSON object in a "
                                       "single line for NDJSON "
                                       "mode (while parsing '{}')",
                                       json_line)));
        success = false;
      }
      if (not success) {
        // We already reported the issue.
        row.cancel();
        co_return;
      }
    }
    state.get_active_entry().expected_rows += 1;
    if (auto slice = this->handle_max_rows(state)) {
      co_yield std::move(*slice);
    }
  }

private:
  std::size_t lines_processed_ = 0u;
};

template <class FieldValidator>
class default_parser : public parser_base<FieldValidator> {
public:
  using parser_base<FieldValidator>::parser_base;

  auto parse(const chunk& json_chunk, parser_state& state)
    -> generator<table_slice> {
    buffer_.append(
      {reinterpret_cast<const char*>(json_chunk.data()), json_chunk.size()});
    auto view = buffer_.view();
    auto err = this->parser_
                 .iterate_many(view.data(), view.length(),
                               simdjson::ondemand::DEFAULT_BATCH_SIZE)
                 .get(stream_);
    if (err) {
      // For the simdjson 3.1 it seems impossible to have an error
      // returned here so it is hard to understand if we can recover from
      // it somehow.
      buffer_.reset();
      this->ctrl_.warn(caf::make_error(ec::parse_error, error_message(err)));
      co_return;
    }
    for (auto doc_it = stream_.begin(); doc_it != stream_.end(); ++doc_it) {
      // doc.error() will inherit all errors from *doc_it and get_value.
      // No need to check after each operation.
      auto doc = (*doc_it).get_value();
      if (auto err = doc.error()) {
        state.abort_requested = true;
        this->ctrl_.abort(caf::make_error(
          ec::parse_error, fmt::format("skips invalid JSON '{}' : {}", view,
                                       error_message(err))));
        co_return;
      }
      auto [action, slice]
        = this->handle_selector(*doc_it, doc_it.source(), state);
      switch (action) {
        case parser_action::skip:
          continue;
        case parser_action::parse:
          break;
        case parser_action::yield:
          TENZIR_ASSERT(slice);
          co_yield std::move(*slice);
      }
      {
        // The `row` gets it's own scope so that it is destroyed before we finish.
        auto row = state.get_active_entry().builder->push_row();
        auto success
          = doc_parser{this->field_validator_, doc_it.source(), this->ctrl_}
              .parse_object(doc.value_unsafe(), row);
        if (not success) {
          // We already reported the issue.
          row.cancel();
          continue;
        }
      }
      state.get_active_entry().expected_rows += 1;
      if (auto slice = this->handle_max_rows(state))
        co_yield std::move(*slice);
    }
    handle_truncated_bytes(state);
  }

private:
  auto handle_truncated_bytes(parser_state& state) -> void {
    auto truncated_bytes = stream_.truncated_bytes();
    if (truncated_bytes == 0) {
      buffer_.reset();
      return;
    }
    // Likely not needed, but should be harmless. Needs additional
    // investigation in the future.
    if (truncated_bytes > buffer_.view().size()) {
      state.abort_requested = true;
      this->ctrl_.abort(caf::make_error(
        ec::parse_error, fmt::format("detected malformed JSON and "
                                     "aborts parsing: '{}'",
                                     buffer_.view())));
      return;
    }
    buffer_.truncate(truncated_bytes);
  }

  // The simdjson suggests to initialize the padding part to either 0s or
  // spaces.
  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  simdjson::ondemand::document_stream stream_;
};

template <class GeneratorValue>
auto make_parser(generator<GeneratorValue> json_chunk_generator,
                 operator_control_plane& ctrl, std::string separator,
                 bool try_find_schema, std::optional<type> schema,
                 bool infer_types, bool preserve_order, auto parser_impl)
  -> generator<table_slice> {
  // TODO: Seems like we don't need it anymore? Check this.
  (void)try_find_schema;
  auto state = parser_state{ctrl, preserve_order};
  if (schema) {
    state.active_entry = state.add_entry(schema->name(), *schema, infer_types);
  } else {
    state.active_entry = state.add_entry(unknown_entry_name);
  }
  // After this point, we always have an active entry.
  for (auto chnk : json_chunk_generator) {
    // Flush builders if their timeout has expired.
    auto now = std::chrono::steady_clock::now();
    for (auto&& entry_ref : non_empty_entries(state)) {
      auto& entry = entry_ref.get();
      if (now > entry.flushed + defaults::import::batch_timeout) {
        co_yield unflatten_if_needed(separator, entry.flush(ctrl));
      }
    }
    if (not chnk or chnk->size() == 0u) {
      co_yield {};
      continue;
    }
    // This also flushes the builder if they grow over the threshold.
    for (auto slice : parser_impl.parse(*chnk, state)) {
      co_yield unflatten_if_needed(separator, std::move(slice));
    }
    if (state.abort_requested) {
      co_return;
    }
  }
  // Flush all entries.
  for (auto&& entry : non_empty_entries(state)) {
    co_yield unflatten_if_needed(separator, entry.get().flush(ctrl));
  }
}

auto parse_selector(std::string_view x, location source) -> selector {
  auto split = detail::split(x, ":");
  TENZIR_ASSERT(!x.empty());
  if (split.size() > 2 or split[0].empty()) {
    diagnostic::error("invalid selector `{}`: must contain at most "
                      "one `:` and field name must "
                      "not be empty",
                      x)
      .primary(source)
      .throw_();
  }
  auto prefix = split.size() == 2 ? std::string{split[1]} : "";
  return selector{std::move(prefix), std::string{split[0]}};
}

struct parser_args {
  std::optional<struct selector> selector;
  std::optional<located<std::string>> schema;
  std::string unnest_separator;
  bool no_infer = false;
  bool use_ndjson_mode = false;
  bool preserve_order = true;

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("selector", x.selector), f.field("schema", x.schema),
              f.field("unnest_separator", x.unnest_separator),
              f.field("no_infer", x.no_infer),
              f.field("use_ndjson_mode", x.use_ndjson_mode),
              f.field("preserve_order", x.preserve_order));
  }
};

auto add_common_options_to_parser(argument_parser& parser, parser_args& args)
  -> void {
  parser.add("--no-infer", args.no_infer);
}

class json_parser final : public plugin_parser {
public:
  json_parser() = default;

  explicit json_parser(parser_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "json";
  }

  auto optimize(event_order order) -> std::unique_ptr<plugin_parser> override {
    auto args = args_;
    args.preserve_order = order == event_order::ordered;
    return std::make_unique<json_parser>(std::move(args));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto strict_validator = [](const detail::field_guard& guard) {
      return guard.field_exists();
    };
    auto no_validation_validator = [](const detail::field_guard&) {
      return true;
    };
    if ((args_.selector.has_value() or args_.schema.has_value())
        and args_.no_infer) {
      return instantiate_impl(std::move(input), ctrl,
                              std::move(strict_validator));
    }
    return instantiate_impl(std::move(input), ctrl,
                            std::move(no_validation_validator));
  }

  friend auto inspect(auto& f, json_parser& x) -> bool {
    return f.apply(x.args_);
  }

private:
  template <class FieldValidator>
  auto
  instantiate_impl(generator<chunk_ptr> input, operator_control_plane& ctrl,
                   FieldValidator field_validator) const
    -> std::optional<generator<table_slice>> {
    auto schemas
      = get_schemas(args_.schema.has_value() or args_.selector.has_value(),
                    ctrl, not args_.unnest_separator.empty());
    auto schema = std::optional<type>{};
    if (args_.schema) {
      const auto found
        = std::find_if(schemas.begin(), schemas.end(), [&](const type& schema) {
            for (const auto& name : schema.names()) {
              if (name == args_.schema->inner) {
                return true;
              }
            }
            return false;
          });
      if (found == schemas.end()) {
        diagnostic::error("failed to find schema `{}`", args_.schema->inner)
          .primary(args_.schema->source)
          // TODO: Refer to the show operator once we have that.
          .note("use `tenzir-ctl show schemas` to show all available schemas")
          .emit(ctrl.diagnostics());
        return {};
      }
      schema = *found;
    }
    if (args_.use_ndjson_mode) {
      return make_parser(to_padded_lines(std::move(input)), ctrl,
                         args_.unnest_separator, args_.selector.has_value(),
                         schema, not args_.no_infer, args_.preserve_order,
                         ndjson_parser<FieldValidator>{
                           ctrl,
                           args_.selector,
                           schema,
                           std::move(schemas),
                           std::move(field_validator),
                           not args_.no_infer,
                           args_.preserve_order,
                         });
    }
    return make_parser(std::move(input), ctrl, args_.unnest_separator,
                       args_.selector.has_value(), schema, not args_.no_infer,
                       args_.preserve_order,
                       default_parser<FieldValidator>{
                         ctrl,
                         args_.selector,
                         schema,
                         std::move(schemas),
                         std::move(field_validator),
                         not args_.no_infer,
                         args_.preserve_order,
                       });
  }

  parser_args args_;
};

struct printer_args {
  std::optional<location> compact_output;
  std::optional<location> color_output;
  std::optional<location> monochrome_output;
  std::optional<location> omit_empty;
  std::optional<location> omit_nulls;
  std::optional<location> omit_empty_objects;
  std::optional<location> omit_empty_lists;

  template <class Inspector>
  friend auto inspect(Inspector& f, printer_args& x) -> bool {
    return f.object(x)
      .pretty_name("printer_args")
      .fields(f.field("compact_output", x.compact_output),
              f.field("color_output", x.color_output),
              f.field("monochrome_output", x.monochrome_output),
              f.field("omit_empty", x.omit_empty),
              f.field("omit_nulls", x.omit_nulls),
              f.field("omit_empty_objects", x.omit_empty_objects),
              f.field("omit_empty_lists", x.omit_empty_lists));
  }
};

class json_printer final : public plugin_printer {
public:
  json_printer() = default;

  explicit json_printer(printer_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "json";
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    const auto compact = !!args_.compact_output;
    auto style = default_style();
    if (args_.monochrome_output)
      style = no_style();
    else if (args_.color_output)
      style = jq_style();
    const auto omit_nulls
      = args_.omit_nulls.has_value() or args_.omit_empty.has_value();
    const auto omit_empty_objects
      = args_.omit_empty_objects.has_value() or args_.omit_empty.has_value();
    const auto omit_empty_lists
      = args_.omit_empty_lists.has_value() or args_.omit_empty.has_value();
    return printer_instance::make(
      [compact, style, omit_nulls, omit_empty_objects,
       omit_empty_lists](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer = tenzir::json_printer{{
          .style = style,
          .oneline = compact,
          .omit_nulls = omit_nulls,
          .omit_empty_records = omit_empty_objects,
          .omit_empty_lists = omit_empty_lists,
        }};
        // TODO: Since this printer is per-schema we can write an optimized
        // version of it that gets the schema ahead of time and only expects
        // data corresponding to exactly that schema.
        auto buffer = std::vector<char>{};
        auto resolved_slice = resolve_enumerations(slice);
        auto array
          = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
        auto out_iter = std::back_inserter(buffer);
        for (const auto& row :
             values(caf::get<record_type>(resolved_slice.schema()), *array)) {
          TENZIR_ASSERT_CHEAP(row);
          const auto ok = printer.print(out_iter, *row);
          TENZIR_ASSERT_CHEAP(ok);
          out_iter = fmt::format_to(out_iter, "\n");
        }
        auto chunk = chunk::make(std::move(buffer));
        co_yield std::move(chunk);
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  friend auto inspect(auto& f, json_printer& x) -> bool {
    return f.apply(x.args_);
  }

private:
  printer_args args_;
};

class plugin final : public virtual parser_plugin<json_parser>,
                     public virtual printer_plugin<json_printer> {
public:
  auto name() const -> std::string override {
    return "json";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto args = parser_args{};
    auto selector = std::optional<located<std::string>>{};
    auto parser
      = argument_parser{"json", "https://docs.tenzir.com/next/formats/json"};
    parser.add("--selector", selector, "<selector>");
    parser.add("--schema", args.schema, "<schema>");
    parser.add("--unnest-separator", args.unnest_separator, "<separator>");
    add_common_options_to_parser(parser, args);
    parser.add("--ndjson", args.use_ndjson_mode);
    parser.parse(p);
    if (args.schema and selector) {
      diagnostic::error("cannot use both `--selector` and `--schema`")
        .primary(args.schema->source)
        .primary(selector->source)
        .throw_();
    } else if (selector) {
      args.selector = parse_selector(selector->inner, selector->source);
    }
    return std::make_unique<json_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto args = printer_args{};
    auto parser
      = argument_parser{"json", "https://docs.tenzir.com/next/formats/json"};
    // We try to follow 'jq' option naming.
    parser.add("-c,--compact-output", args.compact_output);
    parser.add("-C,--color-output", args.color_output);
    parser.add("-M,--monochrome-output", args.color_output);
    parser.add("--omit-empty", args.omit_empty);
    parser.add("--omit-nulls", args.omit_nulls);
    parser.add("--omit-empty-objects", args.omit_empty_objects);
    parser.add("--omit-empty-lists", args.omit_empty_lists);
    parser.parse(p);
    return std::make_unique<json_printer>(std::move(args));
  }
};

template <detail::string_literal Name, detail::string_literal Selector,
          detail::string_literal Separator = "">
class selector_parser final : public virtual parser_parser_plugin {
public:
  auto name() const -> std::string override {
    return std::string{Name.str()};
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/next/formats/{}", name())};
    auto args = parser_args{};
    add_common_options_to_parser(parser, args);
    parser.parse(p);
    args.use_ndjson_mode = true;
    args.selector = parse_selector(Selector.str(), location::unknown);
    args.unnest_separator = Separator.str();
    return std::make_unique<json_parser>(std::move(args));
  }
};

using suricata_parser = selector_parser<"suricata", "event_type:suricata">;
using zeek_parser = selector_parser<"zeek-json", "_path:zeek", ".">;

} // namespace

} // namespace tenzir::plugins::json

TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::suricata_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::zeek_parser)
