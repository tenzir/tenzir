//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/cast.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/config_options.hpp>
#include <tenzir/data.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/env.hpp>
#include <tenzir/detail/heterogeneous_string_hash.hpp>
#include <tenzir/detail/padded_buffer.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try_simdjson.hpp>

#include <arrow/record_batch.h>
#include <caf/detail/is_one_of.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <chrono>
#include <simdjson.h>

namespace tenzir::plugins::json {

namespace {

/// This is the maximum size of a single object/event when *not* using the
/// NDJSON mode. If this becomes problematic in the future, we can use a dynamic
/// approach instead.
constexpr auto max_object_size = size_t{10'000'000};

inline auto split_at_crlf(generator<chunk_ptr> input)
  -> generator<std::optional<simdjson::padded_string_view>> {
  auto buffer = std::string{};
  bool ended_on_carriage_return = false;
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    if (ended_on_carriage_return && *begin == '\n') {
      ++begin;
    };
    ended_on_carriage_return = false;
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
          ended_on_carriage_return = true;
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
inline auto split_at_null(generator<chunk_ptr> input, char split)
  -> generator<std::optional<simdjson::padded_string_view>> {
  auto buffer = std::string{};
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    for (const auto* current = begin; current != end; ++current) {
      if (*current != split) {
        continue;
      }
      const auto size = static_cast<size_t>(current - begin);
      const auto capacity = static_cast<size_t>(end - begin);
      if (buffer.empty() and capacity >= size + simdjson::SIMDJSON_PADDING) {
        co_yield simdjson::padded_string_view{begin, size, capacity};
      } else {
        buffer.append(begin, current);
        buffer.reserve(buffer.size() + simdjson::SIMDJSON_PADDING);
        co_yield simdjson::padded_string_view{buffer};
        buffer.clear();
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

auto json_string_parser(std::string_view s, const tenzir::type* seed)
  -> std::variant<tenzir::data, tenzir::diagnostic> {
  if (seed) {
    return record_builder::basic_seeded_parser(s, *seed);
  }
  tenzir::data result;
  constexpr static auto p = (parsers::data - parsers::number - parsers::pattern);
  if (p(s, result)) {
    return result;
  } else {
    return tenzir::data{std::string{s}};
  }
}

/// Parses simdjson objects into the given `series_builder` handles.
class doc_parser {
public:
  doc_parser(std::string_view parsed_document, diagnostic_handler& diag)
    : parsed_document_{parsed_document}, diag_{diag} {
  }

  doc_parser(std::string_view parsed_document, diagnostic_handler& diag,
             std::size_t parsed_lines)
    : parsed_document_{parsed_document},
      diag_{diag},
      parsed_lines_{parsed_lines} {
  }

  [[nodiscard]] auto parse_object(simdjson::ondemand::value v, auto builder,
                                  size_t depth = 0u) -> bool {
    auto obj = v.get_object();
    if (obj.error()) {
      report_parse_err(v, "object");
      return false;
    }
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
        report_parse_err(val, fmt::format("object value at key `{}`", key));
        return false;
      }
      bool value_parse_success = false;
      // this guards the base series_builder currently used by tql2 parse_json
      if constexpr (std::same_as<detail::multi_series_builder::record_generator,
                                 decltype(builder)>) {
        value_parse_success = parse_value(
          val.value_unsafe(), builder.unflattend_field(key), depth + 1);
      } else {
        value_parse_success
          = parse_value(val.value_unsafe(), builder.field(key), depth + 1);
      }
      if (not value_parse_success) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] auto parse_value(simdjson::ondemand::value val, auto builder,
                                 size_t depth) -> bool {
    if (depth > defaults::max_recursion) {
      die("nesting too deep in json_parser parse");
    }
    auto type = val.type();
    if (type.error()) {
      report_parse_err(val, "a value");
      return false;
    }
    switch (type.value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        builder.null();
        return true;
      case simdjson::ondemand::json_type::number:
        return parse_number(val, builder);
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return false;
        }
        builder.data(result.value_unsafe());
        return true;
      }
      case simdjson::ondemand::json_type::string:
        return parse_string(val, builder);
      case simdjson::ondemand::json_type::array:
        return parse_array(val.get_array().value_unsafe(), builder.list(),
                           depth + 1);
      case simdjson::ondemand::json_type::object:
        return parse_object(val, builder.record(), depth + 1);
    }
    TENZIR_UNREACHABLE();
  }

private:
  [[nodiscard]] auto
  parse_number(simdjson::ondemand::value val, auto builder) -> bool {
    auto kind = simdjson::ondemand::number_type{};
    auto result = val.get_number_type();
    if (result.error()) {
      report_parse_err(val, "a number");
      return false;
    }
    kind = result.value_unsafe();
    switch (kind) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        builder.data(result.value_unsafe());
        return true;
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        builder.data(result.value_unsafe());
        return true;
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return false;
        }
        builder.data(result.value_unsafe());
        return true;
      }
      case simdjson::ondemand::number_type::big_integer: {
        report_parse_err(val, "a smaller number");
        builder.null(); // TODO is this a good idea?
        return false;
      }
    }
    TENZIR_UNREACHABLE();
  }

  [[nodiscard]] auto
  parse_string(simdjson::ondemand::value val, auto builder) -> bool {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return false;
    }
    // TODO because of this it would be better to adapt the multi_series_builder
    if constexpr (std::same_as<decltype(builder), builder_ref>) {
      auto d = json_string_parser(maybe_str.value_unsafe(), nullptr);
      auto err = std::get_if<tenzir::diagnostic>(&d);
      if (err) {
        diag_.emit(std::move(*err));
      }
      builder.data(std::get<tenzir::data>(d));
    } else {
      builder.data_unparsed(std::string{maybe_str.value_unsafe()});
    }
    return true;
  }

  [[nodiscard]] auto parse_array(simdjson::ondemand::array arr, auto builder,
                                 size_t depth) -> bool {
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        return false;
      }
      if (not parse_value(element.value_unsafe(), builder, depth + 1)) {
        return false;
      }
    }
    return true;
  }

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
        .emit(diag_);
    }
    diagnostic::warning("failed to parse {} in the JSON document",
                        std::move(description))
      .note("{} {}", note_prefix, document_to_truncate)
      .emit(diag_);
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
        .emit(diag_);
      return;
    }
    auto column = v.current_location().value_unsafe() - parsed_document_.data();
    diagnostic::warning("failed to parse {} in the JSON document",
                        std::move(description))
      .note("line {} column {}", *parsed_lines_, column)
      .emit(diag_);
  }

  std::string_view parsed_document_;
  diagnostic_handler& diag_;
  std::optional<std::size_t> parsed_lines_;
};

class parser_base {
public:
  parser_base(operator_control_plane& ctrl,
              multi_series_builder_options options, std::vector<type> schemas)
    : builder{std::move(options.policy), std::move(options.settings),
              json_string_parser, std::move(schemas)},
      ctrl{ctrl} {
  }

  multi_series_builder builder;
  operator_control_plane& ctrl;
  simdjson::ondemand::parser parser;
  bool abort_requested = false;
};

class ndjson_parser final : public parser_base {
public:
  using parser_base::parser_base;

  auto parse(simdjson::padded_string_view json_line) -> void {
    ++lines_processed_;
    auto maybe_doc = this->parser.iterate(json_line);
    auto val = maybe_doc.get_value();
    // val.error() will inherit all errors from maybe_doc. No need to check
    // for error after each operation.
    if (auto err = val.error()) {
      diagnostic::warning("{}", error_message(err))
        .note("skips invalid JSON `{}`", json_line)
        .emit(this->ctrl.diagnostics());
      return;
    }
    auto& doc = maybe_doc.value_unsafe();
    auto success = doc_parser{json_line, this->ctrl.diagnostics()}.parse_object(
      val.value_unsafe(), builder.record());
    // After parsing one JSON object it is expected for the result to be at
    // the end. If it's otherwise then it means that a line contains more than
    // one object in which case we don't add any data and emit a warning.
    // It is also possible for a parsing failure to occurr in doc_parser. the
    // is_alive() call ensures that the first object was parsed without
    // errors. Calling at_end() when is_alive() returns false is unsafe and
    // resulted in crashes.
    if (success and not doc.at_end()) {
      diagnostic::warning(
        "encountered more than one JSON object in a single NDJSON line")
        .note("skips remaining objects in line `{}`", json_line)
        .emit(this->ctrl.diagnostics());
      success = false;
    }
    if (not success) {
      // We already reported the issue.
      builder.remove_last();
    }
  }

  void validate_completion() const {
    // noop, just exists for easy of implementation
  }

private:
  std::size_t lines_processed_ = 0u;
};

class default_parser final : public parser_base {
public:
  default_parser(operator_control_plane& ctrl,
                 multi_series_builder_options options,
                 std::vector<type> schemas, bool arrays_of_objects)
    : parser_base{ctrl, std::move(options), std::move(schemas)},
      arrays_of_objects_{arrays_of_objects} {
  }

  auto parse(const chunk& json_chunk) -> void {
    buffer_.append(
      {reinterpret_cast<const char*>(json_chunk.data()), json_chunk.size()});
    auto view = buffer_.view();
    auto err
      = this->parser.iterate_many(view.data(), view.length(), max_object_size)
          .get(stream_);
    if (err) {
      // For the simdjson 3.1 it seems impossible to have an error
      // returned here so it is hard to understand if we can recover from
      // it somehow.
      buffer_.reset();
      diagnostic::warning("{}", error_message(err))
        .note("failed to parse")
        .emit(this->ctrl.diagnostics());
      return;
    }
    for (auto doc_it = stream_.begin(); doc_it != stream_.end(); ++doc_it) {
      // doc.error() will inherit all errors from *doc_it and get_value.
      // No need to check after each operation.
      auto doc = (*doc_it).get_value();
      if (auto err = doc.error()) {
        abort_requested = true;
        diagnostic::error("{}", error_message(err))
          .note("skips invalid JSON '{}'", view)
          .emit(this->ctrl.diagnostics());
        return;
      }
      if (arrays_of_objects_) {
        auto arr = doc.value_unsafe().get_array();
        if (arr.error()) {
          abort_requested = true;
          diagnostic::error("expected an array of objects")
            .note("got: {}", view)
            .emit(this->ctrl.diagnostics());
          return;
        }
        for (auto&& elem : arr.value_unsafe()) {
          auto row = builder.record();
          auto success = doc_parser{doc_it.source(), this->ctrl.diagnostics()}
                           .parse_object(elem.value_unsafe(), row);
          if (not success) {
            // We already reported the issue.
            builder.remove_last();
            continue;
          }
        }
      } else {
        auto row = builder.record();
        auto success
          = doc_parser{doc_it.source(), this->ctrl.diagnostics()}.parse_object(
            doc.value_unsafe(), row);
        if (not success) {
          // We already reported the issue.
          builder.remove_last();
          continue;
        }
      }
    }
    handle_truncated_bytes();
  }

  void validate_completion() {
    if (not buffer_.view().empty()) {
      diagnostic::error("parser input ended with incomplete object")
        .emit(ctrl.diagnostics());
      abort_requested = true;
    }
  }

private:
  auto handle_truncated_bytes() -> void {
    auto truncated_bytes = stream_.truncated_bytes();
    if (truncated_bytes == 0) {
      buffer_.reset();
      return;
    }
    // Likely not needed, but should be harmless. Needs additional
    // investigation in the future.
    if (truncated_bytes > buffer_.view().size()) {
      abort_requested = true;
      diagnostic::error("detected malformed JSON")
        .note("in input '{}'", buffer_.view())
        .emit(this->ctrl.diagnostics());
      return;
    }
    buffer_.truncate(truncated_bytes);
  }
  bool arrays_of_objects_;
  // The simdjson suggests to initialize the padding part to either 0s or
  // spaces.
  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  simdjson::ondemand::document_stream stream_;
};

template <class GeneratorValue>
auto parser_loop(generator<GeneratorValue> json_chunk_generator,
                 std::derived_from<parser_base> auto parser_impl)
  -> generator<table_slice> {
  for (auto chunk : json_chunk_generator) {
    // get all events that are ready (timeout, batch size, ordered mode
    // constraints)
    for (auto& slice : parser_impl.builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    for (auto& e : parser_impl.builder.last_errors()) {
      parser_impl.ctrl.diagnostics().emit(std::move(e));
    }
    if (not chunk or chunk->size() == 0u) {
      co_yield {};
      continue;
    }
    parser_impl.parse(*chunk);
    if (parser_impl.abort_requested) {
      co_return;
    }
  }
  parser_impl.validate_completion();
  if (parser_impl.abort_requested) {
    co_return;
  }
  for (auto& e : parser_impl.builder.last_errors()) {
    parser_impl.ctrl.diagnostics().emit(std::move(e));
  }
  // Get all remaining events
  for (auto& slice : parser_impl.builder.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

struct parser_args {
  multi_series_builder_options builder_options = {};
  bool arrays_of_objects = false;
  bool use_ndjson_mode = true; // TODO these two could be an enum
  bool use_gelf_mode = false;

  friend auto inspect(auto& f, parser_args& x) {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("builder_options", x.builder_options),
              f.field("arrays_of_objects", x.arrays_of_objects),
              f.field("use_ndjson_mode", x.use_ndjson_mode),
              f.field("use_gelf_mode", x.use_gelf_mode));
  }
};

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
    args.builder_options.settings.ordered = order == event_order::ordered;
    return std::make_unique<json_parser>(std::move(args));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto schemas = args_.builder_options.get_schemas();
    if (args_.use_ndjson_mode) {
      return parser_loop(split_at_crlf(std::move(input)),
                         ndjson_parser{
                           ctrl,
                           args_.builder_options,
                           std::move(schemas),
                         });
    }
    if (args_.use_gelf_mode) {
      return parser_loop(split_at_null(std::move(input), '\0'),
                         ndjson_parser{
                           ctrl,
                           args_.builder_options,
                           std::move(schemas),
                         });
    }
    return parser_loop(std::move(input), default_parser{
                                           ctrl,
                                           args_.builder_options,
                                           std::move(schemas),
                                           args_.arrays_of_objects,
                                         });
  }

  friend auto inspect(auto& f, json_parser& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
  }

private:
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
  std::optional<location> arrays_of_objects;

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
              f.field("omit_empty_lists", x.omit_empty_lists),
              f.field("arrays_of_objects", x.arrays_of_objects));
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
    if (args_.monochrome_output) {
      style = no_style();
    } else if (args_.color_output) {
      style = jq_style();
    }
    const auto omit_nulls
      = args_.omit_nulls.has_value() or args_.omit_empty.has_value();
    const auto omit_empty_objects
      = args_.omit_empty_objects.has_value() or args_.omit_empty.has_value();
    const auto omit_empty_lists
      = args_.omit_empty_lists.has_value() or args_.omit_empty.has_value();
    const auto arrays_of_objects = args_.arrays_of_objects.has_value();
    auto meta = chunk_metadata{.content_type = compact and not arrays_of_objects
                                                 ? "application/x-ndjson"
                                                 : "application/json"};
    return printer_instance::make(
      [compact, style, omit_nulls, omit_empty_objects, omit_empty_lists,
       arrays_of_objects,
       meta = std::move(meta)](table_slice slice) -> generator<chunk_ptr> {
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
        auto out_iter = std::back_inserter(buffer);
        auto rows = resolved_slice.values();
        auto row = rows.begin();
        if (not arrays_of_objects) {
          for (; row != rows.end(); ++row) {
            const auto ok = printer.print(out_iter, *row);
            TENZIR_ASSERT(ok);
            out_iter = fmt::format_to(out_iter, "\n");
          }
        } else {
          out_iter = fmt::format_to(out_iter, "[");
          if (row != rows.end()) {
            const auto ok = printer.print(out_iter, *row);
            TENZIR_ASSERT(ok);
            ++row;
          }
          for (; row != rows.end(); ++row) {
            *out_iter++ = ',';
            *out_iter++ = compact ? ' ' : '\n';
            const auto ok = printer.print(out_iter, *row);
            TENZIR_ASSERT(ok);
          }
          out_iter = fmt::format_to(out_iter, "]\n");
        }
        auto chunk = chunk::make(std::move(buffer), meta);
        co_yield std::move(chunk);
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  };

  auto prints_utf8() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, json_printer& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_));
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
    auto parser
      = argument_parser{name(), "https://docs.tenzir.com/formats/json"};
    parser_args args;
    multi_series_builder_argument_parser msb_parser{
      {.default_name = "tenzir.json"}};
    msb_parser.add_all_to_parser(parser);
    std::optional<location> legacy_precise;
    std::optional<location> use_ndjson_mode;
    std::optional<location> use_gelf_mode;
    std::optional<location> arrays_of_objects;
    parser.add("--precise", legacy_precise);
    parser.add("--ndjson", use_ndjson_mode);
    parser.add("--gelf", use_gelf_mode);
    parser.add("--arrays-of-objects", arrays_of_objects);
    parser.parse(p);
    if (use_ndjson_mode and use_gelf_mode) {
      diagnostic::error("`--ndjson` and `--gelf` are incompatible")
        .primary(*use_ndjson_mode)
        .primary(*use_gelf_mode)
        .throw_();
    }
    if (use_ndjson_mode and arrays_of_objects) {
      diagnostic::error("`--ndjson` and `--arrays-of-objects` are incompatible")
        .primary(*use_ndjson_mode)
        .primary(*arrays_of_objects)
        .throw_();
    }
    if (use_gelf_mode and arrays_of_objects) {
      diagnostic::error("`--gelf` and `--arrays-of-objects` are incompatible")
        .primary(*use_gelf_mode)
        .primary(*arrays_of_objects)
        .throw_();
    }
    args.use_ndjson_mode = use_ndjson_mode.has_value();
    args.use_gelf_mode = use_gelf_mode.has_value();
    args.arrays_of_objects = arrays_of_objects.has_value();
    args.builder_options = msb_parser.get_options();

    if (legacy_precise
        and std::get_if<multi_series_builder::policy_merge>(
          &args.builder_options.policy)) {
      diagnostic::error("`--precise` and `--merge` are incompatible.")
        .primary(*legacy_precise)
        .throw_();
    }

    return std::make_unique<json_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto args = printer_args{};
    auto parser
      = argument_parser{name(), "https://docs.tenzir.com/formats/json"};
    // We try to follow 'jq' option naming.
    parser.add("-c,--compact-output", args.compact_output);
    parser.add("-C,--color-output", args.color_output);
    parser.add("-M,--monochrome-output", args.color_output);
    parser.add("--omit-empty", args.omit_empty);
    parser.add("--omit-nulls", args.omit_nulls);
    parser.add("--omit-empty-objects", args.omit_empty_objects);
    parser.add("--omit-empty-lists", args.omit_empty_lists);
    parser.add("--arrays-of-objects", args.arrays_of_objects);
    parser.parse(p);
    return std::make_unique<json_printer>(std::move(args));
  }
};

class gelf_parser final : public virtual parser_parser_plugin {
public:
  auto name() const -> std::string override {
    return "gelf";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto msb_parser = multi_series_builder_argument_parser{
      multi_series_builder::settings_type{.default_name = "gelf"},
      multi_series_builder::policy_precise{},
    };
    msb_parser.add_all_to_parser(parser);

    parser.parse(p);
    auto args = parser_args{};
    args.builder_options = msb_parser.get_options();
    args.use_gelf_mode = true;
    return std::make_unique<json_parser>(std::move(args));
  }
};

template <detail::string_literal Name, detail::string_literal Selector,
          detail::string_literal Prefix, detail::string_literal Separator = "">
class selector_parser final : public virtual parser_parser_plugin {
public:
  auto name() const -> std::string override {
    return std::string{Name.str()};
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto args = parser_args{};
    auto msb_parser = multi_series_builder_argument_parser{
      multi_series_builder::settings_type{
        .default_name = std::string{Prefix.str()},
        .unnest_separator = std::string{Separator.str()},
      },
      multi_series_builder::policy_selector{
        .field_name = std::string{Selector.str()},
        .naming_prefix = std::string{Prefix.str()},
      },
    };
    msb_parser.add_settings_to_parser(parser, true);
    parser.parse(p);
    args.builder_options = msb_parser.get_options();
    args.use_ndjson_mode = true;

    return std::make_unique<json_parser>(std::move(args));
  }
};

using suricata_parser = selector_parser<"suricata", "event_type", "suricata">;
using zeek_parser = selector_parser<"zeek-json", "_path", "zeek", ".">;

class write_json final : public crtp_operator<write_json> {
public:
  write_json() = default;

  explicit write_json(printer_args args) : printer_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "tql2.write_json";
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // TODO: Expose a better API for this.
    auto printer = printer_.instantiate(type{}, ctrl);
    TENZIR_ASSERT(printer);
    TENZIR_ASSERT(*printer);
    for (auto&& slice : input) {
      auto yielded = false;
      for (auto&& chunk : (*printer)->process(slice)) {
        co_yield std::move(chunk);
        yielded = true;
      }
      if (not yielded) {
        co_yield {};
      }
    }
    for (auto&& chunk : (*printer)->finish()) {
      co_yield std::move(chunk);
    }
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, write_json& x) -> bool {
    return f.object(x).fields(f.field("printer", x.printer_));
  }

private:
  json_printer printer_;
};

class read_json_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    auto sep = std::optional<located<std::string>>{};
    std::optional<location> use_ndjson_mode;
    std::optional<location> use_gelf_mode;
    std::optional<location> arrays_of_objects;
    parser.add("sep", sep);
    parser.add("ndjson", use_ndjson_mode);
    parser.add("gelf", use_gelf_mode);
    parser.add("arrays-of-objects", arrays_of_objects);
    auto result = parser.parse(inv, ctx);
    auto args = parser_args{};
    try {
      args.builder_options = msb_parser.get_options();
      args.use_ndjson_mode = use_ndjson_mode.has_value();
      args.use_gelf_mode = use_gelf_mode.has_value();
      args.arrays_of_objects = arrays_of_objects.has_value();
    } catch (diagnostic& d) {
      ctx.dh().emit(std::move(d));
      result = failure::promise();
    }
    if (use_ndjson_mode and use_gelf_mode) {
      diagnostic::error("`ndjson` and `gelf` are incompatible")
        .primary(*use_ndjson_mode)
        .primary(*use_gelf_mode)
        .emit(ctx);
      result = failure::promise();
    }
    if (args.use_ndjson_mode and args.arrays_of_objects) {
      diagnostic::error("`ndjson` and `arrays-of-objects` are incompatible")
        .primary(*use_ndjson_mode)
        .primary(*arrays_of_objects)
        .emit(ctx);
      result = failure::promise();
    }
    if (args.use_gelf_mode and args.arrays_of_objects) {
      diagnostic::error("`gelf` and `arrays-of-objects` are incompatible")
        .primary(*use_gelf_mode)
        .primary(*arrays_of_objects)
        .emit(ctx);
      result = failure::promise();
    }
    if (sep) {
      auto& str = sep->inner;
      if (str == "\n") {
        args.use_ndjson_mode = true;
        if (args.use_gelf_mode) {
          diagnostic::error(
            "gelf mode is incompatible with a separator \"\\n\"")
            .primary(sep->source)
            .primary(*use_gelf_mode)
            .hint(R"(expected "\n" or "\0")")
            .emit(ctx);
          result = failure::promise();
        }
      } else if (str.size() == 1 && str[0] == '\0') {
        args.use_gelf_mode = true;
        if (args.use_ndjson_mode) {
          diagnostic::error(
            "ndjson mode is incompatible with a separator \"\\0\"")
            .primary(sep->source)
            .primary(*use_ndjson_mode)
            .hint(R"(expected "\n" or "\0")")
            .emit(ctx);
          result = failure::promise();
        }
      } else {
        diagnostic::error("unknown separator {:?}", str)
          .primary(sep->source)
          .hint(R"(expected "\n" or "\0")")
          .emit(ctx);
        result = failure::promise();
      }
    }

    TRY(result);
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }
};

class read_gelf_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto name() const -> std::string override {
    return "read_gelf";
  }
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    auto args = parser_args{};
    try {
      args.builder_options = msb_parser.get_options();
    } catch (diagnostic& d) {
      ctx.dh().emit(std::move(d));
      result = failure::promise();
    }
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }
};

template <detail::string_literal Name, detail::string_literal Selector,
          detail::string_literal Prefix, detail::string_literal Separator = "">
class configured_read_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto name() const -> std::string override {
    return fmt::format("read_{}", Name);
  }
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{
      multi_series_builder::settings_type{
        .default_name = std::string{Prefix.str()},
        .unnest_separator = std::string{Separator.str()},
      },
      multi_series_builder::policy_selector{
        .field_name = std::string{Selector.str()},
        .naming_prefix = std::string{Prefix.str()},
      },
    };
    msb_parser.add_settings_to_parser(parser, true);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    auto args = parser_args{};
    args.use_ndjson_mode = true;
    try {
      args.builder_options = msb_parser.get_options();
    } catch (diagnostic& d) {
      ctx.dh().emit(std::move(d));
      result = failure::promise();
    }
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }
};

using read_suricata_plugin
  = configured_read_plugin<"suricata", "event_type", "suricata">;
using read_zeek_plugin
  = configured_read_plugin<"zeek_json", "_path", "zeek", ".">;

class parse_json_plugin final : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_json";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // TODO: Consider adding a `many` option to expect multiple json values.
    // TODO: Consider adding a `precise` option (this needs evaluator support).
    TRY(argument_parser2::method("parse_json")
          .add(expr, "<string>")
          .parse(inv, ctx));
    return function_use::make(
      [call = inv.call.get_location(),
       expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray&) {
            return arg;
          },
          [&](const arrow::StringArray& arg) {
            auto parser = simdjson::ondemand::parser{};
            auto b = series_builder{};
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                b.null();
                continue;
              }
              auto str = std::string{arg.Value(i)};
              doc_parser doc_p = doc_parser(str, ctx);
              auto doc = parser.iterate(str);
              if (doc.error()) {
                diagnostic::warning("{}", error_message(doc.error()))
                  .primary(call)
                  .emit(ctx);
                continue;
              }
              const auto res
                = doc_p.parse_value(doc.get_value(), builder_ref{b}, 0);
              if (not res) {
                diagnostic::warning("could not parse json")
                  .primary(call)
                  .emit(ctx);
                b.remove_last();
                b.null();
                continue;
              }
            }
            auto result = b.finish();
            // TODO: Consider whether we need heterogeneous for this. If so,
            // then we must extend the evaluator accordingly.
            if (result.size() != 1) {
              diagnostic::warning("got incompatible JSON values")
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            }
            return std::move(result[0]);
          },
          [&](const auto&) {
            diagnostic::warning("`parse_json` expected `string`")
              .note("got `{}`", arg.type.kind())
              .primary(call)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

class write_json_plugin final : public virtual operator_plugin2<write_json> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    // TODO: More options, and consider `null_fields=false` as default.
    auto args = printer_args{};
    TRY(argument_parser2::operator_("write_json")
          // TODO: Perhaps "indent=0"?
          .add("ndjson", args.compact_output)
          .add("color", args.color_output)
          .parse(inv, ctx));
    return std::make_unique<write_json>(args);
  }
};

} // namespace

} // namespace tenzir::plugins::json

TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::gelf_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::suricata_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::zeek_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_gelf_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_zeek_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_suricata_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::parse_json_plugin)
