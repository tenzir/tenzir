//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/adaptive_table_slice_builder.hpp>
#include <vast/argument_parser.hpp>
#include <vast/arrow_table_slice.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/config_options.hpp>
#include <vast/defaults.hpp>
#include <vast/detail/assert.hpp>
#include <vast/detail/padded_buffer.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/diagnostics.hpp>
#include <vast/generator.hpp>
#include <vast/operator_control_plane.hpp>
#include <vast/plugin.hpp>
#include <vast/to_lines.hpp>
#include <vast/tql/parser.hpp>

#include <arrow/record_batch.h>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <simdjson.h>

namespace vast::plugins::json {

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

struct parser_state {
  // Cache of table slice builder for each schema. These objects can be reused
  // and there is no need to recreate them each time we parse an event.
  std::unordered_map<std::string_view, adaptive_table_slice_builder>
    builders_per_schema{};
  // Used to check if the parser must yield in case the parser was seeded with a
  // known schema. The parses must yield the table_slice of previously parsed
  // schema when it parses an event of a different one.
  adaptive_table_slice_builder* last_used_builder = nullptr;
  std::string last_used_schema_name{};
  // Table slice builder used when the schema is not known.
  adaptive_table_slice_builder unknown_schema_builder{};
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

  auto parse_object(simdjson::ondemand::value v, auto&& field_pusher,
                    size_t depth = 0u) -> void {
    auto obj = v.get_object().value_unsafe();
    for (auto pair : obj) {
      if (pair.error()) {
        report_parse_err(v, "key value pair");
        return;
      }
      auto maybe_key = pair.unescaped_key();
      if (maybe_key.error()) {
        report_parse_err(v, "key in an object");
        return;
      }
      auto key = maybe_key.value_unsafe();
      auto val = pair.value();
      if (val.error()) {
        report_parse_err(val, fmt::format("object value at key {}", key));
        return;
      }
      auto field = field_pusher.push_field(key);
      if (not field_validator_(field))
        continue;
      parse_impl(val.value_unsafe(), field, depth + 1);
    }
  }

private:
  auto report_parse_err(auto& v, std::string description) -> void {
    ctrl_.warn(caf::make_error(
      ec::parse_error,
      fmt::format("json parser failed to parse {} in line {} from '{}'",
                  std::move(description), parsed_document_,
                  v.current_location().value_unsafe())));
  }

  auto parse_number(simdjson::ondemand::value val, auto& pusher) -> void {
    switch (val.get_number_type().value_unsafe()) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        add_value(pusher, result.value_unsafe());
        return;
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        add_value(pusher, result.value_unsafe());
        return;
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return;
        }
        add_value(pusher, result.value_unsafe());
        return;
      }
    }
  }

  auto parse_string(simdjson::ondemand::value val, auto& pusher) -> void {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return;
    }
    auto str = val.get_string().value_unsafe();
    using namespace parser_literals;
    static constexpr auto parser
      = parsers::time | parsers::duration | parsers::net | parsers::ip;
    data result;
    if (parser(str, result)) {
      add_value(pusher, make_view(result));
      return;
    }
    // Take the input as-is if nothing worked.
    add_value(pusher, str);
  }

  auto parse_array(simdjson::ondemand::array arr, auto& pusher, size_t depth)
    -> void {
    auto list = pusher.push_list();
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        continue;
      }
      parse_impl(element.value_unsafe(), list, depth + 1);
    }
  }

  auto parse_impl(simdjson::ondemand::value val, auto& pusher, size_t depth)
    -> void {
    if (depth > defaults::max_recursion)
      die("nesting too deep in json_parser parse");
    auto type = val.type();
    if (type.error())
      return;
    switch (val.type().value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        return;
      case simdjson::ondemand::json_type::number:
        parse_number(val, pusher);
        return;
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return;
        }
        add_value(pusher, result.value_unsafe());
        return;
      }
      case simdjson::ondemand::json_type::string:
        parse_string(val, pusher);
        return;
      case simdjson::ondemand::json_type::array:
        parse_array(val.get_array().value_unsafe(), pusher, depth + 1);
        return;
      case simdjson::ondemand::json_type::object:
        parse_object(val, pusher.push_record(), depth + 1);
        return;
    }
  }

  auto add_value(auto& guard, auto value) -> void {
    if (auto err = guard.add(value))
      ctrl_.warn(std::move(err));
  }

  const FieldValidator& field_validator_;
  std::string_view parsed_document_;
  operator_control_plane& ctrl_;
};

auto handle_empty_chunk(parser_state& state, bool has_selector) -> table_slice {
  if (has_selector) {
    if (state.last_used_builder)
      return state.last_used_builder->finish(state.last_used_schema_name);
    return table_slice{};
  }
  return std::exchange(state.unknown_schema_builder, {}).finish();
}

auto get_schema_name(simdjson::ondemand::document_reference doc,
                     const selector& selector) -> caf::expected<std::string> {
  auto type = doc[selector.selector_field];
  doc.rewind();
  if (auto err = type.error()) {
    if (err != simdjson::error_code::NO_SUCH_FIELD)
      return caf::make_error(ec::parse_error, error_message(err));
    return "";
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

auto handle_builder_change(adaptive_table_slice_builder& builder_to_use,
                           parser_state& state) -> std::optional<table_slice> {
  if (not state.last_used_builder)
    return {};
  if (state.last_used_builder != std::addressof(builder_to_use)) {
    if (auto slice
        = state.last_used_builder->finish(state.last_used_schema_name);
        slice.rows() > 0) {
      if (state.last_used_builder
          == std::addressof(state.unknown_schema_builder))
        state.unknown_schema_builder = {};
      return slice;
    }
  }
  return {};
}

auto finalize(adaptive_table_slice_builder* last_used_builder,
              const std::string& last_used_schema_name)
  -> std::optional<table_slice> {
  if (not last_used_builder)
    return {};
  if (auto slice = last_used_builder->finish(last_used_schema_name);
      slice.rows() > 0u)
    return std::move(slice);
  return {};
}

std::vector<type> get_schemas(bool try_find_schema,
                              operator_control_plane& ctrl, bool unflatten) {
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

auto unflatten_if_needed(std::string_view separator, table_slice slice) {
  if (separator.empty())
    return slice;
  return unflatten(slice, separator);
}

template <class FieldValidator>
class parser_base {
public:
  parser_base(operator_control_plane& ctrl, std::optional<selector> selector,
              std::optional<type> schema, std::vector<type> schemas,
              FieldValidator field_validator, bool infer_types)
    : ctrl_{ctrl},
      selector_{std::move(selector)},
      schema_{std::move(schema)},
      schemas_{std::move(schemas)},
      field_validator_{std::move(field_validator)},
      infer_types_{infer_types} {
  }

protected:
  auto handle_schema_found(parser_state& state, const type& schema) const
    -> std::optional<table_slice> {
    if (not state.builders_per_schema.contains(schema.name())) {
      state.builders_per_schema[schema.name()]
        = adaptive_table_slice_builder{schema, infer_types_};
    }
    auto& current_builder = state.builders_per_schema[schema.name()];
    auto maybe_slice_to_yield = handle_builder_change(current_builder, state);
    state.last_used_builder = std::addressof(current_builder);
    state.last_used_schema_name = std::string{schema.name()};
    return maybe_slice_to_yield;
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
    if (state.last_used_schema_name == schema_name) {
      return {std::nullopt};
    }
    std::optional<table_slice> maybe_slice_to_yield;
    if (state.last_used_builder) {
      if (auto slice
          = state.last_used_builder->finish(state.last_used_schema_name);
          slice.rows() > 0) {
        maybe_slice_to_yield.emplace(std::move(slice));
      }
    }
    state.unknown_schema_builder = {};
    state.last_used_builder = std::addressof(state.unknown_schema_builder);
    state.last_used_schema_name = std::string{schema_name};
    return {std::move(maybe_slice_to_yield)};
  }

  auto handle_schema_name_found(std::string_view schema_name,
                                std::string_view json_source,
                                parser_state& state) const
    -> caf::expected<std::optional<table_slice>> {
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
  handle_known_schema(simdjson::ondemand::document_reference doc_ref,
                      std::string_view json_source, parser_state& state) const
    -> std::pair<parser_action, std::optional<table_slice>> {
    VAST_ASSERT(not schema_);
    VAST_ASSERT(selector_);
    auto maybe_schema_name = get_schema_name(doc_ref, *selector_);
    if (not maybe_schema_name) {
      ctrl_.warn(std::move(maybe_schema_name.error()));
      if (not infer_types_)
        return {parser_action::skip, std::nullopt};
      auto maybe_slice_to_yield
        = handle_builder_change(state.unknown_schema_builder, state);
      state.last_used_builder = std::addressof(state.unknown_schema_builder);
      state.last_used_schema_name.clear();
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
    return handle_known_schema(doc_ref, json_source, state);
  }

  auto handle_max_rows(parser_state& state)
    -> std::optional<table_slice> const {
    if (state.last_used_builder->rows() < max_table_slice_rows_)
      return std::nullopt;
    auto slice = state.last_used_builder->finish(state.last_used_schema_name);
    if (not this->selector_)
      state.unknown_schema_builder = {};
    return slice;
  }

  operator_control_plane& ctrl_;
  std::optional<selector> selector_;
  std::optional<type> schema_;
  std::vector<type> schemas_;
  FieldValidator field_validator_;
  bool infer_types_ = true;
  simdjson::ondemand::parser parser_;
  // TODO: change max table slice size to be fetched from options.
  vast::detail::arrow_length_type max_table_slice_rows_
    = defaults::import::table_slice_size;
};

template <class FieldValidator>
class ndjson_parser : public parser_base<FieldValidator> {
public:
  using parser_base<FieldValidator>::parser_base;

  auto parse(simdjson::padded_string_view json_line, parser_state& state)
    -> generator<table_slice> {
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
        co_yield *slice;
    }
    auto row = state.last_used_builder->push_row();
    doc_parser{this->field_validator_, json_line, this->ctrl_}.parse_object(
      val.value_unsafe(), row);
    // After parsing one JSON object it is expected for the result to be at
    // the end. If it's otherwise then it means that a line contains more than
    // one object in which case we don't add any data and emit a warning.
    if (not doc.at_end()) {
      row.cancel();
      this->ctrl_.warn(caf::make_error(
        ec::parse_error, fmt::format("more than one JSON object in a "
                                     "single line for NDJSON "
                                     "mode (while parsing '{}')",
                                     json_line)));
    }
    if (auto slice = this->handle_max_rows(state))
      co_yield *slice;
  }
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
        this->ctrl_.warn(caf::make_error(
          ec::parse_error, fmt::format("skips invalid JSON '{}' : {}", view,
                                       error_message(err))));
        continue;
      }
      auto [action, slice]
        = this->handle_selector(*doc_it, doc_it.source(), state);
      switch (action) {
        case parser_action::skip:
          continue;
        case parser_action::parse:
          break;
        case parser_action::yield:
          co_yield *slice;
      }
      auto row = state.last_used_builder->push_row();
      doc_parser{this->field_validator_, doc_it.source(), this->ctrl_}
        .parse_object(doc.value_unsafe(), row);
      if (auto slice = this->handle_max_rows(state))
        co_yield *slice;
    }
    handle_truncated_bytes();
  }

private:
  auto handle_truncated_bytes() -> void {
    auto truncated_bytes = stream_.truncated_bytes();
    if (truncated_bytes == 0) {
      buffer_.reset();
      return;
    }
    // The branch below can ocurr when we have malformed JSON that
    // triggers some UB in the simdjson parser. The simdjson parser is supposed
    // to be used with well formed JSON or truncated JSON. In this case we don't
    // know how to recover. It might be possible to use different parser or our
    // custom logic to try recover as much data as possible.
    if (truncated_bytes > buffer_.view().size()) {
      this->ctrl_.abort(caf::make_error(
        ec::parse_error, fmt::format("detected malformed JSON and "
                                     "aborts parsing: '{}'",
                                     buffer_.view())));
      return;
    }
    buffer_.truncate(truncated_bytes);
  }

  // The simdjson suggests to initialize the padding part to either 0s or spaces.
  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  simdjson::ondemand::document_stream stream_;
};

template <class GeneratorValue>
auto make_parser(generator<GeneratorValue> json_chunk_generator,
                 std::string separator, bool try_find_schema,
                 std::optional<type> schema, bool infer_types, auto parser_impl)
  -> generator<table_slice> {
  auto state = parser_state{};
  if (schema) {
    const auto [it, inserted] = state.builders_per_schema.emplace(
      schema->name(), adaptive_table_slice_builder{*schema, infer_types});
    VAST_ASSERT(inserted);
    state.last_used_builder = std::addressof(it->second);
  } else {
    state.last_used_builder = std::addressof(state.unknown_schema_builder);
  }
  for (auto chnk : json_chunk_generator) {
    if (not chnk or chnk->size() == 0u) {
      co_yield unflatten_if_needed(separator,
                                   handle_empty_chunk(state, try_find_schema));
      continue;
    }
    for (auto slice : parser_impl.parse(*chnk, state)) {
      co_yield unflatten_if_needed(separator, std::move(slice));
    }
  }
  if (auto slice
      = finalize(state.last_used_builder, state.last_used_schema_name))
    co_yield unflatten_if_needed(separator, std::move(*slice));
}

auto parse_selector(std::string_view x, location source) -> selector {
  auto split = detail::split(x, ":");
  VAST_ASSERT(!x.empty());
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

struct config {
  std::optional<struct selector> selector;
  std::optional<located<std::string>> schema;
  std::string unnest_separator;
  bool no_infer = false;
  bool use_ndjson_mode = false;

  template <class Inspector>
  friend auto inspect(Inspector& f, config& x) -> bool {
    return f.object(x).pretty_name("config").fields(
      f.field("selector", x.selector),
      f.field("unnest_separator", x.unnest_separator),
      f.field("no_infer", x.no_infer),
      f.field("use_ndjson_mode", x.use_ndjson_mode));
  }
};

auto add_common_options_to_parser(argument_parser& parser, config& cfg)
  -> void {
  parser.add("--no-infer", cfg.no_infer);
}

class json_parser final : public plugin_parser {
public:
  json_parser() = default;

  explicit json_parser(config cfg) : cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "json";
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
    if ((cfg_.selector.has_value() or cfg_.schema.has_value())
        and cfg_.no_infer) {
      return instantiate_impl(std::move(input), ctrl,
                              std::move(strict_validator));
    }
    return instantiate_impl(std::move(input), ctrl,
                            std::move(no_validation_validator));
  }

  friend auto inspect(auto& f, json_parser& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  template <class FieldValidator>
  auto
  instantiate_impl(generator<chunk_ptr> input, operator_control_plane& ctrl,
                   FieldValidator field_validator) const
    -> std::optional<generator<table_slice>> {
    auto schemas
      = get_schemas(cfg_.schema.has_value() or cfg_.selector.has_value(), ctrl,
                    not cfg_.unnest_separator.empty());
    auto schema = std::optional<type>{};
    if (cfg_.schema) {
      const auto found
        = std::find_if(schemas.begin(), schemas.end(), [&](const type& schema) {
            for (const auto& name : schema.names()) {
              if (name == cfg_.schema->inner) {
                return true;
              }
            }
            return false;
          });
      if (found == schemas.end()) {
        diagnostic::error("failed to find schema `{}`", cfg_.schema->inner)
          .primary(cfg_.schema->source)
          // TODO: Refer to the show operator once we have that.
          .note("use `tenzir-ctl show schemas` to show all available schemas")
          .emit(ctrl.diagnostics());
        return {};
      }
      schema = *found;
    }
    if (cfg_.use_ndjson_mode) {
      return make_parser(to_padded_lines(std::move(input)),
                         cfg_.unnest_separator, cfg_.selector.has_value(),
                         std::move(schema), not cfg_.no_infer,
                         ndjson_parser<FieldValidator>{
                           ctrl,
                           cfg_.selector,
                           std::move(schema),
                           std::move(schemas),
                           std::move(field_validator),
                           not cfg_.no_infer,
                         });
    }
    return make_parser(std::move(input), cfg_.unnest_separator,
                       cfg_.selector.has_value(), std::move(schema),
                       not cfg_.no_infer,
                       default_parser<FieldValidator>{
                         ctrl,
                         cfg_.selector,
                         std::move(schema),
                         std::move(schemas),
                         std::move(field_validator),
                         not cfg_.no_infer,
                       });
  }

  config cfg_;
};

class json_printer final : public plugin_printer {
public:
  json_printer() = default;

  explicit json_printer(bool pretty) : pretty_{pretty} {
  }

  auto name() const -> std::string override {
    return "json";
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return printer_instance::make(
      [pretty = pretty_](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        // JSON printer should output NDJSON, see:
        // https://github.com/ndjson/ndjson-spec
        auto printer = vast::json_printer{{.oneline = not pretty}};
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
          VAST_ASSERT_CHEAP(row);
          const auto ok = printer.print(out_iter, *row);
          VAST_ASSERT_CHEAP(ok);
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
    return f.apply(x.pretty_);
  }

private:
  bool pretty_;
};

class plugin final : public virtual parser_plugin<json_parser>,
                     public virtual printer_plugin<json_printer> {
public:
  auto name() const -> std::string override {
    return "json";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto cfg = config{};
    auto selector = std::optional<located<std::string>>{};
    auto parser
      = argument_parser{"json", "https://docs.tenzir.com/next/formats/json"};
    parser.add("--selector", selector, "<selector>");
    parser.add("--schema", cfg.schema, "<schema>");
    parser.add("--unnest-separator", cfg.unnest_separator, "<separator>");
    add_common_options_to_parser(parser, cfg);
    parser.add("--ndjson", cfg.use_ndjson_mode);
    parser.parse(p);
    if (cfg.schema and selector) {
      diagnostic::error("cannot use both `--selector` and `--schema`")
        .primary(cfg.schema->source)
        .primary(selector->source)
        .throw_();
    } else if (selector) {
      cfg.selector = parse_selector(selector->inner, selector->source);
    }
    return std::make_unique<json_parser>(std::move(cfg));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto pretty = false;
    auto parser
      = argument_parser{"json", "https://docs.tenzir.com/next/formats/json"};
    parser.add("--pretty", pretty);
    parser.parse(p);
    return std::make_unique<json_printer>(pretty);
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
    auto cfg = config{};
    add_common_options_to_parser(parser, cfg);
    parser.parse(p);
    cfg.use_ndjson_mode = true;
    cfg.selector = parse_selector(Selector.str(), location::unknown);
    cfg.unnest_separator = Separator.str();
    return std::make_unique<json_parser>(std::move(cfg));
  }
};

using suricata_parser = selector_parser<"suricata", "event_type:suricata">;
using zeek_parser = selector_parser<"zeek-json", "_path:zeek", ".">;

} // namespace

} // namespace vast::plugins::json

VAST_REGISTER_PLUGIN(vast::plugins::json::plugin)
VAST_REGISTER_PLUGIN(vast::plugins::json::suricata_parser)
VAST_REGISTER_PLUGIN(vast::plugins::json::zeek_parser)
