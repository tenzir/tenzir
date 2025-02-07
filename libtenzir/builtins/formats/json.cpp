//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
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
#include <tenzir/si_literals.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try_simdjson.hpp>

#include <arrow/record_batch.h>
#include <caf/detail/is_one_of.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <deque>
#include <simdjson.h>
#include <string_view>

namespace tenzir::plugins::json {

// this is up here to avoid a warning for an undefined static function if it
// where in the anon namespace
TENZIR_ENUM(split_at, none, newline, null);

namespace {

using namespace tenzir::si_literals;

/// This is the initial simdjson buffer size when *NOT* using NDJSON.
constexpr auto initial_simdjson_batch_size = 10_M;
/// This is the maximum size we increase the simdjson buffer when *NOT* using
/// NDJSON.
constexpr auto max_simdjson_batch_size = 2_G;
static_assert(initial_simdjson_batch_size <= max_simdjson_batch_size);
static_assert(max_simdjson_batch_size <= 4_G,
              "simdjson specifies 4G as an upper bound for the batch_size");

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

static auto
truncate(std::string_view text, const size_t N = 50) -> std::string {
  return std::string{text.substr(0, N)}
         + (text.size() > N ? " ... (truncated)" : "");
}

/// Parses simdjson objects into the given `series_builder` handles.
class doc_parser {
public:
  /// The result of a parsing operation
  enum class result {
    /// The parsing succeeded
    success,
    /// The parsing failed, but wrote elements into the builder
    failure_with_write,
    /// The parsing failed, but did not affect the builder
    failure_no_change,
  };

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
      auto value_parse_result = result::success;
      // this guards the base series_builder currently used by tql2 parse_json
      if constexpr (std::same_as<detail::multi_series_builder::record_generator,
                                 decltype(builder)>) {
        value_parse_result = parse_value(
          val.value_unsafe(), builder.unflattened_field(key), depth + 1);
      } else {
        value_parse_result
          = parse_value(val.value_unsafe(), builder.field(key), depth + 1);
      }
      if (value_parse_result != result::success) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] auto parse_value(simdjson::ondemand::value val, auto builder,
                                 size_t depth) -> result {
    TENZIR_ASSERT(depth <= defaults::max_recursion,
                  "nesting too deep in JSON parser");
    auto type = val.type();
    if (type.error()) {
      report_parse_err(val, "a value");
      return result::failure_no_change;
    }
    switch (type.value_unsafe()) {
      case simdjson::ondemand::json_type::null:
        builder.null();
        return result::success;
      case simdjson::ondemand::json_type::number:
        return parse_number(val, builder);
      case simdjson::ondemand::json_type::boolean: {
        auto result = val.get_bool();
        if (result.error()) {
          report_parse_err(val, "a boolean value");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::json_type::string:
        return parse_string(val, builder);
      case simdjson::ondemand::json_type::array: {
        const auto success = parse_array(val.get_array().value_unsafe(),
                                         builder.list(), depth + 1);
        return success ? result::success : result::failure_with_write;
      }
      case simdjson::ondemand::json_type::object: {
        const auto success = parse_object(val, builder.record(), depth + 1);
        return success ? result::success : result::failure_with_write;
      }
    }
    TENZIR_UNREACHABLE();
  }

private:
  [[nodiscard]] auto
  parse_number(simdjson::ondemand::value val, auto builder) -> result {
    auto kind = simdjson::ondemand::number_type{};
    auto result = val.get_number_type();
    if (result.error()) {
      report_parse_err(val, "a number");
      return result::failure_no_change;
    }
    kind = result.value_unsafe();
    switch (kind) {
      case simdjson::ondemand::number_type::floating_point_number: {
        auto result = val.get_double();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::signed_integer: {
        auto result = val.get_int64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::unsigned_integer: {
        auto result = val.get_uint64();
        if (result.error()) {
          report_parse_err(val, "a number");
          return result::failure_no_change;
        }
        builder.data(result.value_unsafe());
        return result::success;
      }
      case simdjson::ondemand::number_type::big_integer: {
        report_parse_err(val, "a big integer",
                         fmt::format("value `{}` does not fit into 64bits",
                                     truncate(val.raw_json_token())));
        // TODO is this a good idea?
        // from the users PoV this isnt an error/warning. its just a limitation
        // of the library/tenzir we could
        // * store null (current behaviour)
        // * store a double (i.e. as an approx value)
        // * store the value as a string
        // builder.null();
        builder.data(std::string{val.raw_json_token()});
        return result::success;
      }
    }
    TENZIR_UNREACHABLE();
  }

  [[nodiscard]] auto
  parse_string(simdjson::ondemand::value val, auto builder) -> result {
    auto maybe_str = val.get_string();
    if (maybe_str.error()) {
      report_parse_err(val, "a string");
      return result::failure_no_change;
    }
    // TODO because of this it would be better to adapt the multi_series_builder
    if constexpr (std::same_as<decltype(builder), builder_ref>) {
      auto res = detail::data_builder::non_number_parser(
        maybe_str.value_unsafe(), nullptr);
      auto& [value, diag] = res;
      if (diag) {
        diag_.emit(std::move(*diag));
      }
      if (value) {
        builder.data(std::move(*value));
      } else {
        builder.data(maybe_str.value_unsafe());
      }
    } else {
      builder.data_unparsed(std::string{maybe_str.value_unsafe()});
    }
    return result::success;
  }

  [[nodiscard]] auto parse_array(simdjson::ondemand::array arr, auto builder,
                                 size_t depth) -> bool {
    auto written_once = false;
    for (auto element : arr) {
      if (element.error()) {
        report_parse_err(element, "an array element");
        return false;
      }
      auto res = parse_value(element.value_unsafe(), builder, depth + 1);
      written_once |= res != result::failure_no_change;
      if (res != result::success) {
        return false;
      }
    }
    return true;
  }

  void emit_unparsed_json_diagnostics(
    std::string description,
    simdjson::simdjson_result<const char*> document_location,
    std::string note = {}) {
    auto document_to_truncate = parsed_document_;
    auto note_prefix = "somewhere in";
    if (not document_location.error()) {
      document_to_truncate = std::string_view{document_location.value_unsafe(),
                                              parsed_document_.end()};
      note_prefix = "at";
    }
    auto b = diagnostic::warning("failed to parse {} in the JSON document",
                                 std::move(description))
               .note("{} `{}`", note_prefix, truncate(document_to_truncate));
    if (not note.empty()) {
      b = std::move(b).note("{}", note);
    }
    std::move(b).emit(diag_);
  }

  void report_parse_err(auto& v, std::string expected, std::string note = {}) {
    if (parsed_lines_) {
      report_parse_err_with_parsed_lines(v, std::move(expected),
                                         std::move(note));
      return;
    }
    emit_unparsed_json_diagnostics(std::move(expected), v.current_location(),
                                   std::move(note));
  }

  void report_parse_err_with_parsed_lines(auto& v, std::string description,
                                          std::string note) {
    if (v.current_location().error()) {
      auto b = diagnostic::warning("failed to parse {} in the JSON document",
                                   std::move(description))
                 .note("line {}", *parsed_lines_);
      if (not note.empty()) {
        b = std::move(b).note("{}", note);
      }

      std::move(b).emit(diag_);
      return;
    }
    auto column = v.current_location().value_unsafe() - parsed_document_.data();
    auto b = diagnostic::warning("failed to parse {} in the JSON document",
                                 std::move(description))
               .note("line {} column {}", *parsed_lines_, column);
    if (not note.empty()) {
      b = std::move(b).note("{}", note);
    }
    std::move(b).emit(diag_);
  }

  std::string_view parsed_document_;
  diagnostic_handler& diag_;
  std::optional<std::size_t> parsed_lines_;
};

class parser_base {
public:
  parser_base(std::string name_, diagnostic_handler& dh_,
              multi_series_builder::options options)
    : dh{std::make_unique<transforming_diagnostic_handler>(
        dh_,
        [name = std::move(name_)](diagnostic d) {
          d.message = fmt::format("{} parser: {}", name, d.message);
          return d;
        })},
      builder{std::move(options), *dh, modules::schemas(),
              detail::data_builder::non_number_parser} {
  }
  // this has to be pointer stable because `builder` holds a reference to it
  // internally
  std::unique_ptr<transforming_diagnostic_handler> dh;
  multi_series_builder builder;
  simdjson::ondemand::parser json_parser;
  bool abort_requested = false;
};

class ndjson_parser final : public parser_base {
public:
  using parser_base::parser_base;

  auto parse(simdjson::padded_string_view json_line) -> void {
    ++lines_processed_;
    simdjson::ondemand::document_stream stream;
    if (auto err = this->json_parser
                     .iterate_many(json_line.data(), json_line.size(),
                                   initial_simdjson_batch_size)
                     .get(stream)) {
      diagnostic::warning("{}", error_message(err)).emit(*dh);
      return;
    }
    size_t objects_parsed = 0;
    size_t diags_emitted = 0;
    for (auto doc_it = stream.begin(); doc_it != stream.end();
         ++doc_it, ++objects_parsed) {
      if (auto err = doc_it.error()) {
        diagnostic::warning("{}", error_message(err))
          .note("line {}", lines_processed_)
          .note("skipped invalid JSON at index {}", doc_it.current_index())
          .emit(*dh);
        ++diags_emitted;
        break; // if the iterator itself errors, the document structure is
               // invalid.
      }
      auto doc = *doc_it;
      TENZIR_ASSERT(not doc.current_location().error());
      const auto source = std::string_view{
        doc.current_location().value_unsafe(),
        json_line.data() + json_line.size(),
      };
      if (auto err = doc.error()) {
        diagnostic::warning("{}", error_message(err))
          .note("line {}", lines_processed_)
          .note("skipped invalid JSON `{}`", truncate(source))
          .emit(*dh);
        ++diags_emitted;
        break;
      }
      auto val = doc.get_value();
      if (auto err = val.error()) {
        diagnostic::warning("{}", error_message(err))
          .note("line {}", lines_processed_)
          .note("skipped invalid JSON `{}`", truncate(source))
          .emit(*dh);
        ++diags_emitted;
        break;
      }
      auto parser = doc_parser{json_line, *dh, lines_processed_};
      auto success = parser.parse_object(val.value_unsafe(), builder.record());
      if (not success) {
        builder.remove_last();
        ++diags_emitted;
        break;
      }
    }
    if (objects_parsed == 0 and diags_emitted == 0) {
      diagnostic::warning("line did not contain a single valid JSON object")
        .note("line {}", lines_processed_)
        .note("skipped invalid JSON `{}`", truncate(json_line))
        .emit(*dh);
    } else if (objects_parsed > 1) {
      diagnostic::warning("more than one JSON object in line")
        .note("line {}", lines_processed_)
        .note("encountered a total of {} objects", objects_parsed)
        .emit(*dh);
    }
    auto truncated_count = stream.truncated_bytes();
    if (truncated_count > 0 and objects_parsed) {
      auto truncated_text = std::string_view{
        json_line.data() + json_line.size() - truncated_count, truncated_count};
      diagnostic::warning("skipped remaining invalid JSON bytes")
        .note("line {}", lines_processed_)
        .note("{} bytes remained", truncated_count)
        .note("skipped invalid JSON `{}`", truncate(truncated_text))
        .emit(*dh);
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
  default_parser(std::string name_, diagnostic_handler& dh,
                 multi_series_builder::options options, bool arrays_of_objects)
    : parser_base{std::move(name_), dh, std::move(options)},
      arrays_of_objects_{arrays_of_objects} {
  }

  auto parse(const chunk& json_chunk) -> void {
    // Whether to retry on a capacity error
    auto retry_capacity_failure = false;
    // How many documents passed the simdjson batch_size.
    // Those documents must be skipped in order to not duplicate events.
    auto completed_documents = size_t{0};
    buffer_.append(
      {reinterpret_cast<const char*>(json_chunk.data()), json_chunk.size()});
    auto view = buffer_.view();
    do {
      retry_capacity_failure = false;
      auto err = json_parser
                   .iterate_many(view.data(), view.length(), current_batch_size)
                   .get(stream_);
      if (err) {
        // For the simdjson 3.1 it seems impossible to have an error
        // returned here so it is hard to understand if we can recover from
        // it somehow.
        buffer_.reset();
        diagnostic::warning("{}", error_message(err))
          .note("failed to parse")
          .emit(*dh);
        return;
      }
      auto current_document = size_t{};
      for (auto doc_it = stream_.begin(); doc_it != stream_.end(); ++doc_it) {
        // Skip documents that passed the simdjson batch_size limits previously
        if (current_document < completed_documents) {
          continue;
        }
        ++current_document;
        // doc.error() will inherit all errors from *doc_it and get_value.
        // No need to check after each operation.
        auto doc = (*doc_it).get_value();
        if (auto err = doc.error()) {
          if (err == simdjson::CAPACITY) {
            current_batch_size *= 2;
            retry_capacity_failure
              = current_batch_size < max_simdjson_batch_size;
            if (retry_capacity_failure) {
              break;
            }
          }
          abort_requested = true;
          diagnostic::error("{}", error_message(err))
            .note("skips invalid JSON '{}'", view)
            .emit(*dh);
          return;
        }
        TENZIR_ASSERT(not doc.current_location().error());
        auto const doc_source = std::string_view{
          doc.current_location().value_unsafe(),
          view.data() + view.size(),
        };
        ++completed_documents;
        if (arrays_of_objects_) {
          auto arr = doc.value_unsafe().get_array();
          if (arr.error()) {
            abort_requested = true;
            diagnostic::error("expected an array of objects")
              .note("got: {}", truncate(doc_source))
              .emit(*dh);
            return;
          }
          for (auto&& elem : arr.value_unsafe()) {
            if (auto err = elem.error()) {
              diagnostic::error("{}", error_message(err))
                .note("skips invalid JSON array '{}'", truncate(doc_source))
                .emit(*dh);
              return;
            }
            TENZIR_ASSERT(not elem.current_location().error());
            const auto source = std::string_view{
              elem.current_location().value_unsafe(),
              view.data() + view.size(),
            };
            auto row = builder.record();
            auto success
              = doc_parser{source, *dh}.parse_object(elem.value_unsafe(), row);
            if (not success) {
              builder.remove_last();
              // It should be fine to continue here, because at least the array
              // structure we are iterating is valid. That is ensured by the
              // elem.error() check above
              continue;
            }
          }
        } else {
          TENZIR_ASSERT(not doc.current_location().error());
          const auto source = std::string_view{
            doc.current_location().value_unsafe(),
            view.data() + view.size(),
          };
          const auto type = check(doc.type());
          if (type != simdjson::ondemand::json_type::object) {
            auto diag = diagnostic::error("expected an object")
                          .note("got: {}", truncate(view));
            if (type == simdjson::ondemand::json_type::array) {
              diag
                = std::move(diag).hint("use the `--arrays-of-objects` option");
            }
            std::move(diag).emit(*dh);
            return;
          }
          auto row = builder.record();
          auto success
            = doc_parser{source, *dh}.parse_object(doc.value_unsafe(), row);
          if (not success) {
            builder.remove_last();
            break;
          }
        }
      }
    } while (retry_capacity_failure);
    handle_truncated_bytes();
  }

  void validate_completion() {
    if (not buffer_.view().empty()) {
      diagnostic::error("parser input ended with incomplete object").emit(*dh);
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
        .emit(*dh);
      return;
    }
    buffer_.truncate(truncated_bytes);
  }
  bool arrays_of_objects_;
  // The simdjson suggests to initialize the padding part to either 0s or
  // spaces.
  detail::padded_buffer<simdjson::SIMDJSON_PADDING, '\0'> buffer_;
  size_t current_batch_size = initial_simdjson_batch_size;
  simdjson::ondemand::document_stream stream_;
};

template <class Parser, class GeneratorValue>
  requires std::derived_from<std::remove_cvref_t<Parser>, parser_base>
auto parser_loop(generator<GeneratorValue> json_chunk_generator,
                 Parser parser_impl) -> generator<table_slice> {
  for (auto chunk : json_chunk_generator) {
    // get all events that are ready (timeout, batch size, ordered mode
    // constraints)
    for (auto& slice : parser_impl.builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
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
  // Get all remaining events
  for (auto& slice : parser_impl.builder.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

struct parser_args {
  std::string parser_name;
  multi_series_builder::options builder_options = {};
  bool arrays_of_objects = false;
  split_at split_mode = split_at::none;
  uint64_t jobs = 0;

  friend auto inspect(auto& f, parser_args& x) {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("parser_name", x.parser_name),
              f.field("builder_options", x.builder_options),
              f.field("arrays_of_objects", x.arrays_of_objects),
              f.field("mode", x.split_mode), f.field("jobs", x.jobs));
  }
};

/// Split the incoming byte stream at newlines such that the concatenation of
/// each resulting chunk vector is a self-contained unit for parallelization.
///
/// Only yields an empty vector if the input yielded an empty chunk, which means
/// that the operator's input buffer is exhausted.
auto split_for_parallelization(generator<chunk_ptr> input)
  -> generator<std::vector<chunk_ptr>> {
  // Split at the next newline after the given number of bytes.
  constexpr auto split_after_size = size_t{1'000'000};
  // The duration after which to yield incoming lines at the latest.
  constexpr auto timeout = defaults::import::batch_timeout;
  // Accumulates all chunks that should be part of the next chunk group. This is
  // for example needed in case the last newline is in the middle of a batch.
  auto current = std::vector<chunk_ptr>{};
  // The total size of all batches in `current`.
  auto current_size = size_t{0};
  auto next_timeout = time::clock::now() + timeout;
  auto pop_before_last_linebreak
    = [&]() -> std::optional<std::vector<chunk_ptr>> {
    // We have to search all chunks here because the last newline is not
    // necessarily in the last chunk.
    for (auto& chunk : std::views::reverse(current)) {
      auto bytes = as_bytes(chunk);
      for (const auto& byte : std::views::reverse(bytes)) {
        if (byte == std::byte{'\n'}) {
          auto end = detail::narrow<size_t>(&byte - bytes.data());
          auto rest = std::vector<chunk_ptr>{};
          if (end != bytes.size() - 1) {
            rest.push_back(chunk->slice(end + 1, bytes.size()));
          }
          auto chunk_index = &chunk - current.data();
          rest.insert(rest.end(),
                      std::move_iterator{current.begin() + chunk_index + 1},
                      std::move_iterator{current.end()});
          current.erase(current.begin() + chunk_index + 1, current.end());
          return std::move(current);
          current = std::move(rest);
        }
      }
    }
    return std::nullopt;
  };
  for (auto&& chunk : input) {
    auto now = time::clock::now();
    if (now > next_timeout) {
      if (auto pop = pop_before_last_linebreak()) {
        co_yield std::move(*pop);
        next_timeout = now + timeout;
      }
    }
    if (not chunk) {
      // This means that the operator has no more input. We propagate that
      // information up by yielding an empty vector.
      co_yield {};
      continue;
    }
    TENZIR_ASSERT(chunk->size() != 0);
    if (current.empty()) {
      next_timeout = now + timeout;
    }
    // If we are under our splitting minimum, we just have to insert the batch.
    if (current_size + chunk->size() < split_after_size
        and now < next_timeout) {
      current.push_back(std::move(chunk));
      current_size += current.back()->size();
      continue;
    }
    // Otherwise, we find the last linebreak and yield everything before that.
    auto yielded = false;
    auto bytes = as_bytes(chunk);
    for (const auto& byte : std::views::reverse(bytes)) {
      // This handles both LF and CRLF. In the latter case, the CR becomes part
      // of the chunk but is ignored later.
      if (byte == std::byte{'\n'}) {
        auto end = detail::narrow<size_t>(&byte - bytes.data());
        current.push_back(chunk->slice(0, end));
        current_size += current.back()->size();
        co_yield std::move(current);
        yielded = true;
        current.clear();
        current_size = 0;
        // Remember the rest of the current chunk, if there is any.
        if (end != bytes.size() - 1) {
          current.push_back(chunk->slice(end + 1, bytes.size()));
          current_size += current.back()->size();
        }
        next_timeout = now + timeout;
        break;
      }
    }
    // If there was no linebreak, we have to insert the entire chunk.
    if (not yielded) {
      current.push_back(std::move(chunk));
      current_size += current.back()->size();
      // We do not yield here. Instead, we decided to very quickly drain the
      // input buffer if there are no newlines in the current input buffer. Once
      // it is drained, we get an empty chunk, which then leads to a yield.
    }
  }
  // There can be remaining chunks if the last one didn't end with a newline.
  if (not current.empty()) {
    co_yield std::move(current);
  }
}

/// Parse the incoming NDJSON byte stream in multiple threads.
///
/// The current implementation always assumes that it can reorder the output.
auto parse_parallelized(generator<chunk_ptr> input, parser_args args,
                        operator_control_plane& ctrl)
  -> generator<table_slice> {
  caf::detail::set_thread_name("read_json");
  // TODO: We assume here that we can reorder outputs. However, even if we
  // maintain the order if we are not allowed to reorder, the output can
  // slightly change because we use separate builders.
  args.builder_options.settings.ordered = false;
  // We use a single input queue to communicate with all worker threads. Putting
  // the empty vector in here tells the thread to stop.
  auto inputs = std::deque<std::vector<chunk_ptr>>{};
  auto inputs_mutex = std::mutex{};
  auto inputs_cv = std::condition_variable{};
  // All worker threads write to the same output queue. Note that there is no
  // condition variable for the output. This is because we need to run the
  // distributing thread if we get new input from the preceding operator. We
  // would thus need to block on a combination of getting new input and
  // receiving output from one of our workers, but that doesn't seem to be
  // possible within the constraints of the current implementation.
  auto outputs = std::deque<table_slice>{};
  auto outputs_mutex = std::mutex{};
  auto work = [&](shared_diagnostic_handler dh) {
    caf::detail::set_thread_name("read_work");
    // We reuse the parser throughout all iterations.
    auto parser = ndjson_parser{args.parser_name, dh, args.builder_options};
    while (true) {
      auto inputs_lock = std::unique_lock{inputs_mutex};
      inputs_cv.wait(inputs_lock, [&] {
        return not inputs.empty();
      });
      auto stop = inputs.front().empty();
      if (stop) {
        // We intentionally don't pop the element so that the other threads can
        // also get to see it.
        return;
      }
      auto input = std::move(inputs.front());
      inputs.pop_front();
      inputs_lock.unlock();
      auto parsed = parser_loop<ndjson_parser&>(
        split_at_crlf(std::invoke(
          [](std::vector<chunk_ptr> input) -> generator<chunk_ptr> {
            for (auto& chunk : input) {
              co_yield std::move(chunk);
            }
          },
          std::move(input))),
        parser);
      for (auto slice : parsed) {
        if (slice.rows() == 0) {
          // We don't care, because our input is already fully there.
          continue;
        }
        auto outputs_lock = std::unique_lock{outputs_mutex};
        outputs.push_back(std::move(slice));
      }
    }
  };
  // Set up the threads.
  TENZIR_ASSERT(args.jobs > 0);
  auto threads = std::vector<std::thread>{};
  for (auto i = uint64_t{0}; i < args.jobs; ++i) {
    threads.emplace_back(work, ctrl.shared_diagnostics());
  }
  // With the current execution model, the generator can be destroyed at any
  // yield. Because we are running threads, we need to protect against that.
  auto guard = detail::scope_guard{[&]() noexcept {
    auto inputs_lock = std::unique_lock{inputs_mutex};
    // We clear the inputs here because we don't care about the output anymore.
    inputs.clear();
    inputs.emplace_back();
    inputs_lock.unlock();
    inputs_cv.notify_all();
    for (auto& thread : threads) {
      thread.join();
    }
  }};
  auto pop_output = [&]() -> std::optional<table_slice> {
    auto outputs_lock = std::unique_lock{outputs_mutex};
    if (outputs.empty()) {
      return std::nullopt;
    }
    auto output = std::move(outputs.front());
    outputs.pop_front();
    return output;
  };
  for (auto split : split_for_parallelization(std::move(input))) {
    auto yielded = false;
    if (split.empty()) {
      // We got a signal that there is no more input. Thus, we'd like to sleep.
      while (auto output = pop_output()) {
        co_yield std::move(*output);
        yielded = true;
      }
      // If we had some output above, we already gave the execution node a
      // chance to refill our input buffer. Hence, we directly try again.
      if (not yielded) {
        co_yield {};
      }
      continue;
    }
    auto inputs_lock = std::unique_lock{inputs_mutex};
    // If this is already too full, wait for a bit to provide backpressure.
    while (inputs.size() > 3 * args.jobs) {
      inputs_lock.unlock();
      while (auto output = pop_output()) {
        co_yield std::move(*output);
        yielded = true;
      }
      if (not yielded) {
        co_yield {};
      }
      inputs_lock.lock();
    }
    inputs.push_back(std::move(split));
    inputs_lock.unlock();
    inputs_cv.notify_one();
    while (auto output = pop_output()) {
      co_yield std::move(*output);
      yielded = true;
    }
    if (not yielded) {
      co_yield {};
    }
  }
  // Once we reach this, the task of joining the threads is not longer handled
  // by the guard. Note that no yield come in between this and joining the
  // threads, so we can be sure that we join all threads before the next yield.
  guard.disable();
  auto inputs_lock = std::unique_lock{inputs_mutex};
  inputs.emplace_back();
  inputs_lock.unlock();
  inputs_cv.notify_all();
  // Wait for completion.
  for (auto& thread : threads) {
    thread.join();
  }
  // Should be done now.
  TENZIR_ASSERT(inputs.size() == 1);
  TENZIR_ASSERT(inputs[0].empty());
  // Yield the remaining outputs.
  for (auto& output : outputs) {
    co_yield std::move(output);
  }
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
    args.builder_options.settings.ordered = order == event_order::ordered;
    return std::make_unique<json_parser>(std::move(args));
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    switch (args_.split_mode) {
      case split_at::newline: {
        if (args_.jobs > 0) {
          return parse_parallelized(std::move(input), args_, ctrl);
        }
        return parser_loop(split_at_crlf(std::move(input)),
                           ndjson_parser{
                             args_.parser_name,
                             ctrl.diagnostics(),
                             args_.builder_options,
                           });
      }
      case split_at::null: {
        return parser_loop(split_at_null(std::move(input), '\0'),
                           ndjson_parser{
                             args_.parser_name,
                             ctrl.diagnostics(),
                             args_.builder_options,
                           });
      }
      case split_at::none: {
        return parser_loop(std::move(input), default_parser{
                                               args_.parser_name,
                                               ctrl.diagnostics(),
                                               args_.builder_options,
                                               args_.arrays_of_objects,
                                             });
      }
    }
    TENZIR_UNREACHABLE();
    return {};
  }

  auto idle_after() const -> duration override {
    return args_.jobs == 0 ? duration::zero() : duration::max();
  }

  friend auto inspect(auto& f, json_parser& x) -> bool {
    return f.apply(x.args_);
  }

private:
  parser_args args_;
};

struct printer_args {
  std::optional<location> compact_output;
  std::optional<location> color_output;
  std::optional<location> monochrome_output;
  std::optional<location> omit_all;
  std::optional<location> omit_null_fields;
  std::optional<location> omit_nulls_in_lists;
  std::optional<location> omit_empty_objects;
  std::optional<location> omit_empty_lists;
  std::optional<location> arrays_of_objects;
  bool tql = false;

  template <class Inspector>
  friend auto inspect(Inspector& f, printer_args& x) -> bool {
    return f.object(x)
      .pretty_name("printer_args")
      .fields(f.field("compact_output", x.compact_output),
              f.field("color_output", x.color_output),
              f.field("monochrome_output", x.monochrome_output),
              f.field("omit_empty", x.omit_all),
              f.field("omit_null_fields", x.omit_null_fields),
              f.field("omit_nulls_in_lists", x.omit_nulls_in_lists),
              f.field("omit_empty_objects", x.omit_empty_objects),
              f.field("omit_empty_lists", x.omit_empty_lists),
              f.field("arrays_of_objects", x.arrays_of_objects),
              f.field("tql", x.tql));
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
    } else if (args_.color_output and args_.tql) {
      style = tql_style();
    } else if (args_.color_output) {
      style = jq_style();
    }
    const auto omit_null_fields
      = args_.omit_null_fields.has_value() or args_.omit_all.has_value();
    const auto omit_nulls_in_lists
      = args_.omit_nulls_in_lists.has_value() or args_.omit_all.has_value();
    const auto omit_empty_objects
      = args_.omit_empty_objects.has_value() or args_.omit_all.has_value();
    const auto omit_empty_lists
      = args_.omit_empty_lists.has_value() or args_.omit_all.has_value();
    const auto arrays_of_objects = args_.arrays_of_objects.has_value();
    auto meta = chunk_metadata{.content_type = compact and not arrays_of_objects
                                                 ? "application/x-ndjson"
                                                 : "application/json"};
    return printer_instance::make(
      [compact, style, omit_null_fields, omit_nulls_in_lists,
       omit_empty_objects, omit_empty_lists, arrays_of_objects, tql = args_.tql,
       meta = std::move(meta)](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer = tenzir::json_printer{{
          .tql = tql,
          .style = style,
          .oneline = compact,
          .omit_null_fields = omit_null_fields,
          .omit_nulls_in_lists = omit_nulls_in_lists,
          .omit_empty_records = omit_empty_objects,
          .omit_empty_lists = omit_empty_lists,
        }};
        // TODO: Since this printer is per-schema we can write an optimized
        // version of it that gets the schema ahead of time and only expects
        // data corresponding to exactly that schema.
        auto buffer = std::vector<char>{};
        auto resolved_slice = resolve_enumerations(slice);
        auto out_iter = std::back_inserter(buffer);
        auto rows = values3(resolved_slice);
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
    auto parser
      = argument_parser{name(), "https://docs.tenzir.com/formats/json"};
    auto args = parser_args{"json"};
    multi_series_builder_argument_parser msb_parser{
      {.default_schema_name = "tenzir.json"},
      multi_series_builder::policy_default{},
    };
    msb_parser.add_all_to_parser(parser);
    std::optional<location> legacy_precise;
    std::optional<location> legacy_no_infer;
    std::optional<location> use_ndjson_mode;
    std::optional<location> use_gelf_mode;
    std::optional<location> arrays_of_objects;
    parser.add("--precise", legacy_precise);
    parser.add("--no-infer", legacy_no_infer);
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
    if (use_ndjson_mode) {
      args.split_mode = split_at::newline;
    } else if (use_gelf_mode) {
      args.split_mode = split_at::null;
    }
    args.arrays_of_objects = arrays_of_objects.has_value();
    auto dh = collecting_diagnostic_handler{};
    auto opts = msb_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    args.builder_options = *opts;
    if (legacy_precise) {
      if (args.builder_options.settings.merge) {
        diagnostic::error("`--precise` and `--merge` incompatible")
          .primary(*legacy_precise)
          .note("`--precise` is a legacy option and and should not be used")
          .throw_();
      }
    }
    if (legacy_no_infer) {
      if (args.builder_options.settings.schema_only) {
        diagnostic::error("`--no-infer` and `--schema-only` are equivalent")
          .primary(*legacy_no_infer)
          .primary(*msb_parser.schema_only_)
          .note("`--no-infer` is a legacy option and should not be used")
          .throw_();
      }
      if (msb_parser.schema_only_) {
        diagnostic::error("`--schema-only` is the new name for `--no-infer`")
          .primary(*legacy_no_infer)
          .primary(*msb_parser.schema_only_)
          .note("`--no-infer` is a legacy option and should be dropped")
          .throw_();
      }
      args.builder_options.settings.schema_only = true;
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
    parser.add("--omit-empty", args.omit_all);
    parser.add("--omit-nulls", args.omit_null_fields);
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
      multi_series_builder::settings_type{.default_schema_name = "gelf"},
      multi_series_builder::policy_default{},
    };
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto args = parser_args{"gelf"};
    auto dh = collecting_diagnostic_handler{};
    auto opts = msb_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    args.builder_options = *opts;
    args.split_mode = split_at::null;
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
    auto args = parser_args{std::string{Name.str()}};
    auto msb_parser = multi_series_builder_argument_parser{
      multi_series_builder::settings_type{
        .default_schema_name = std::string{Prefix.str()},
        .unnest_separator = std::string{Separator.str()},
      },
      multi_series_builder::policy_selector{
        .field_name = std::string{Selector.str()},
        .naming_prefix = std::string{Prefix.str()},
      },
    };
    msb_parser.add_settings_to_parser(parser, false, true);
    std::optional<location> legacy_no_infer;
    parser.add("--no-infer", legacy_no_infer);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto opts = msb_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    args.builder_options = *opts;
    args.split_mode = split_at::newline;
    if (legacy_no_infer) {
      if (args.builder_options.settings.schema_only) {
        diagnostic::error("`--no-infer` and `--schema-only` are incompatible.")
          .primary(*legacy_no_infer)
          .primary(*msb_parser.schema_only_)
          .note("`--no-infer` is a legacy option and should not be used")
          .throw_();
      }
      args.builder_options.settings.schema_only = true;
    }
    return std::make_unique<json_parser>(std::move(args));
  }
};

using suricata_parser = selector_parser<"suricata", "event_type", "suricata">;
using zeek_parser = selector_parser<"zeek-json", "_path", "zeek", ".">;

class write_json final : public crtp_operator<write_json> {
public:
  write_json() = default;

  explicit write_json(printer_args args, uint64_t n_jobs)
    : n_jobs_{n_jobs}, printer_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "tql2.write_json";
  }

  auto detached() const -> bool override {
    return true;
  }

  struct input_t {
    uint64_t index;
    table_slice slice;
  };

  auto
  parallel_operator(generator<table_slice> input,
                    operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto inputs_mut = std::mutex{};
    auto inputs = std::deque<input_t>{};
    auto inputs_cv = std::condition_variable{};
    auto outputs_mut = std::mutex{};
    auto outputs = std::unordered_map<size_t, std::vector<chunk_ptr>>{};
    auto input_index = size_t{0};
    auto output_index = size_t{0};
    auto work = [&]() {
      auto printer = printer_.instantiate(type{}, ctrl);
      TENZIR_ASSERT(printer);
      TENZIR_ASSERT(*printer);
      while (true) {
        auto inputs_lock = std::unique_lock{inputs_mut};
        inputs_cv.wait(inputs_lock, [&]() {
          return not inputs.empty();
        });
        // An empty slice is our sentinel to shut down.
        if (inputs.front().slice.rows() == 0) {
          return;
        }
        auto my_work = std::move(inputs.front());
        inputs.pop_front();
        inputs_lock.unlock();
        auto result = std::vector<chunk_ptr>{};
        for (auto&& chunk : (*printer)->process(std::move(my_work.slice))) {
          result.emplace_back(std::move(chunk));
        }
        auto output_lock = std::scoped_lock{outputs_mut};
        auto [it, success]
          = outputs.try_emplace(my_work.index, std::move(result));
        TENZIR_ASSERT(success);
      }
    };
    auto pool = std::vector<std::thread>(n_jobs_);
    for (auto& t : pool) {
      t = std::thread{work};
    }
    auto guard = detail::scope_guard{[&]() noexcept {
      auto inputs_lock = std::unique_lock{inputs_mut};
      // We clear the inputs here because we don't care about the output anymore.
      inputs.clear();
      inputs.emplace_back(input_index, table_slice{});
      inputs_lock.unlock();
      inputs_cv.notify_all();
      for (auto& t : pool) {
        t.join();
      }
    }};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      {
        // Create some sort of backpressure.
        auto input_lock = std::unique_lock{inputs_mut};
        while (inputs.size() > 1.5 * n_jobs_) {
          input_lock.unlock();
          co_yield {};
          input_lock.lock();
        }
        // TODO Consider actually cutting the slice to ensure more balanced
        // dispatching.
        inputs.emplace_back(input_index, std::move(slice));
        ++input_index;
        inputs_cv.notify_one();
      }
      {
        auto output_lock = std::scoped_lock{outputs_mut};
        if (not ordered_) {
          for (auto& [_, chunks] : outputs) {
            for (auto& c : chunks) {
              co_yield std::move(c);
            }
          }
          outputs.clear();
        } else {
          for (; true; ++output_index) {
            auto it = outputs.find(output_index);
            if (it != outputs.end()) {
              for (auto& c : it->second) {
                co_yield std::move(c);
              }
              outputs.erase(it);
              continue;
            }
            break;
          }
        }
      }
    }
    guard.disable();
    {
      // Emplace an empty sentinel into the queue and wake up all workers
      auto input_lock = std::scoped_lock{inputs_mut};
      inputs.emplace_back(input_index, table_slice{});
      inputs_cv.notify_all();
    }
    // wait for the workers to finish
    for (auto& t : pool) {
      t.join();
    }
    // Only the sentinel should remain
    TENZIR_ASSERT(inputs.size() == 1);
    TENZIR_ASSERT(inputs.front().index == input_index);
    auto output_lock = std::scoped_lock{outputs_mut};
    if (not ordered_) {
      for (auto& [_, chunks] : outputs) {
        for (auto& c : chunks) {
          co_yield std::move(c);
        }
      }
      outputs.clear();
    } else {
      for (; output_index < input_index; ++output_index) {
        auto it = outputs.find(output_index);
        TENZIR_ASSERT(it != outputs.end());
        for (auto& c : it->second) {
          co_yield std::move(c);
        }
      }
    }
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // TODO: Expose a better API for this.
    caf::detail::set_thread_name("PRINTER");
    auto printer = printer_.instantiate(type{}, ctrl);
    TENZIR_ASSERT(printer);
    TENZIR_ASSERT(*printer);
    if (n_jobs_ > 1) {
      for (auto&& o : parallel_operator(std::move(input), ctrl)) {
        co_yield std::move(o);
      }
      co_return;
    }
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      for (auto&& chunk : (*printer)->process(slice)) {
        co_yield std::move(chunk);
      }
    }
    for (auto&& chunk : (*printer)->finish()) {
      co_yield std::move(chunk);
    }
  }

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    auto replacement = std::make_unique<write_json>(*this);
    replacement->ordered_ = order == event_order::ordered;
    return optimize_result{std::nullopt, order, std::move(replacement)};
  }

  friend auto inspect(auto& f, write_json& x) -> bool {
    return f.object(x).fields(f.field("ordered", x.ordered_),
                              f.field("n_jobs", x.n_jobs_),
                              f.field("printer", x.printer_));
  }

private:
  bool ordered_ = true;
  uint64_t n_jobs_;
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
    std::optional<location> arrays_of_objects;
    parser.named("arrays_of_objects", arrays_of_objects);
    auto result = parser.parse(inv, ctx);
    auto args = parser_args{"json"};
    TRY(args.builder_options, msb_parser.get_options(ctx.dh()));
    args.arrays_of_objects = arrays_of_objects.has_value();
    TRY(result);
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"json"}};
  }
};

class read_ndjson_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto name() const -> std::string override {
    return "read_ndjson";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    auto jobs = uint64_t{0};
    parser.named_optional("_jobs", jobs);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    auto args = parser_args{"ndjson"};
    TRY(args.builder_options, msb_parser.get_options(ctx.dh()));
    args.split_mode = split_at::newline;
    args.jobs = jobs;
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"ndjson", "jsonl"}};
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
    auto args = parser_args{"gelf"};
    args.split_mode = split_at::null;
    TRY(args.builder_options, msb_parser.get_options(ctx.dh()));
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
        .default_schema_name = std::string{Prefix.str()},
        .unnest_separator = std::string{Separator.str()},
      },
      multi_series_builder::policy_selector{
        .field_name = std::string{Selector.str()},

        .naming_prefix = std::string{Prefix.str()},
      },
    };
    msb_parser.add_settings_to_parser(parser, false, false);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    auto args = parser_args{std::string{Name.str()}};
    args.split_mode = split_at::newline;
    TRY(args.builder_options, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }
};

using read_suricata_plugin
  = configured_read_plugin<"suricata", "event_type", "suricata">;
using read_zeek_plugin
  = configured_read_plugin<"zeek_json", "_path", "zeek", ".">;

class parse_json_plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_json";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // TODO: Consider adding a `many` option to expect multiple json values.
    // TODO: Consider adding a `precise` option (this needs evaluator support).
    TRY(argument_parser2::function("parse_json")
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [call = inv.call.get_location(), expr = std::move(expr)](evaluator eval,
                                                               session ctx) {
        return map_series(eval(expr), [&](series arg) {
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
                  b.null();
                  continue;
                }
                const auto result
                  = doc_p.parse_value(doc.get_value(), builder_ref{b}, 0);
                switch (result) {
                  case doc_parser::result::failure_with_write:
                    b.remove_last();
                    [[fallthrough]];
                  case doc_parser::result::failure_no_change:
                    diagnostic::warning("could not parse json")
                      .primary(call)
                      .emit(ctx);
                    b.null();
                    break;
                  case doc_parser::result::success: /*no op*/;
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
              diagnostic::warning("`parse_json` expected `string`, got `{}`",
                                  arg.type.kind())
                .primary(call)
                .emit(ctx);
              return series::null(null_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }
};

class write_json_plugin final : public virtual operator_plugin2<write_json> {
public:
  explicit write_json_plugin(bool tql) : tql_{tql} {
  }

  auto name() const -> std::string override {
    return tql_ ? "write_tql" : "tql2.write_json";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    // TODO: More options, and consider `null_fields=false` as default.
    auto args = printer_args{};
    auto n_jobs = uint64_t{};
    args.tql = tql_;
    auto parser = argument_parser2::operator_("write_json");
    parser.named("color", args.color_output);
    parser.named("strip", args.omit_all);
    parser.named("strip_null_fields", args.omit_null_fields);
    parser.named("strip_nulls_in_lists", args.omit_nulls_in_lists);
    parser.named("strip_empty_records", args.omit_empty_objects);
    parser.named("strip_empty_lists", args.omit_empty_lists);
    parser.named_optional("_jobs", n_jobs);
    if (tql_) {
      parser.named("compact", args.compact_output);
    }
    TRY(parser.parse(inv, ctx));
    return std::make_unique<write_json>(args, n_jobs);
  }

  auto write_properties() const -> write_properties_t override {
    if (tql_) {
      return {};
    }
    return {.extensions = {"json"}};
  }

  bool tql_ = false;
};

class write_ndjson_plugin final : public virtual operator_plugin2<write_json> {
public:
  auto name() const -> std::string override {
    return "write_ndjson";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = printer_args{};
    args.compact_output = location::unknown;
    auto n_jobs = uint64_t{};
    TRY(argument_parser2::operator_(name())
          .named("color", args.color_output)
          .named("strip", args.omit_all)
          .named("strip_null_fields", args.omit_null_fields)
          .named("strip_nulls_in_lists", args.omit_nulls_in_lists)
          .named("strip_empty_records", args.omit_empty_objects)
          .named("strip_empty_lists", args.omit_empty_lists)
          .named_optional("_jobs", n_jobs)
          .parse(inv, ctx));
    return std::make_unique<write_json>(args, n_jobs);
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {"ndjson", "jsonl"}};
  }
};

} // namespace

} // namespace tenzir::plugins::json

TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::gelf_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::suricata_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::zeek_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_ndjson_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_gelf_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_zeek_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_suricata_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_json_plugin{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_json_plugin{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::parse_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_ndjson_plugin)
