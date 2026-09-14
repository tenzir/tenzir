//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"
#include "tenzir/json_parser.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/async/pusher.hpp>
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
#include <tenzir/ir.hpp>
#include <tenzir/modules.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/try_simdjson.hpp>

#include <arrow/record_batch.h>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Collect.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/executors/GlobalExecutor.h>

#include <atomic>
#include <deque>
#include <initializer_list>
#include <simdjson.h>
#include <string_view>

namespace tenzir::plugins::json {

// this is up here to avoid a warning for an undefined static function if it
// were in the anon namespace
TENZIR_ENUM(split_at, none, newline, null);

namespace {

using namespace tenzir::json;

using detection_state = read_detection_result::result_state;

enum class json_probe_state {
  incomplete,
  invalid,
  complete,
};

struct json_probe_result {
  json_probe_state state = json_probe_state::incomplete;
  simdjson::dom::element_type type = simdjson::dom::element_type::NULL_VALUE;
};

auto json_error_state(simdjson::error_code error, bool eof)
  -> json_probe_state {
  if (not eof
      and (error == simdjson::EMPTY or error == simdjson::UNCLOSED_STRING
           or error == simdjson::TAPE_ERROR
           or error == simdjson::INCOMPLETE_ARRAY_OR_OBJECT)) {
    return json_probe_state::incomplete;
  }
  return json_probe_state::invalid;
}

auto probe_json_document(read_detection_input input) -> json_probe_result {
  auto view = detail::trim_front(input.bytes);
  if (view.empty()) {
    return {.state = input.eof ? json_probe_state::invalid
                               : json_probe_state::incomplete};
  }
  if (view.front() != '{' and view.front() != '[') {
    return {.state = json_probe_state::invalid};
  }
  auto parser = simdjson::dom::parser{};
  auto bytes = std::string{view};
  auto doc = parser.parse(bytes);
  if (auto error = doc.error()) {
    return {.state = json_error_state(error, input.eof)};
  }
  auto element = doc.value_unsafe();
  auto type = element.type();
  return {
    .state = json_probe_state::complete,
    .type = type,
  };
}

auto json_object_has_keys(std::string_view input,
                          std::initializer_list<std::string_view> keys)
  -> bool {
  auto parser = simdjson::dom::parser{};
  auto bytes = std::string{detail::trim_front(input)};
  auto doc = parser.parse(bytes);
  if (doc.error()) {
    return false;
  }
  auto object = doc.value_unsafe().get_object();
  if (object.error()) {
    return false;
  }
  for (auto key : keys) {
    if (object.value_unsafe().at_key(key).error()) {
      return false;
    }
  }
  return true;
}

auto detect_json_object(read_detection_input input) -> read_detection_result {
  auto probe = probe_json_document(input);
  if (probe.state == json_probe_state::invalid) {
    return read_detection::reject();
  }
  if (probe.state == json_probe_state::incomplete) {
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  return probe.type == simdjson::dom::element_type::OBJECT
           ? read_detection::match()
           : read_detection::reject();
}

auto json_stream_error(simdjson::error_code error, bool eof)
  -> read_detection_result {
  return json_error_state(error, eof) == json_probe_state::incomplete
           ? read_detection::need_more()
           : read_detection::reject();
}

template <class Predicate>
auto detect_json_stream(read_detection_input input, char initial_byte,
                        Predicate predicate) -> read_detection_result {
  auto view = detail::trim_front(input.bytes);
  if (view.empty()) {
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  if (view.front() != initial_byte) {
    return read_detection::reject();
  }
  auto parser = simdjson::ondemand::parser{};
  auto bytes = simdjson::padded_string{view};
  auto stream = simdjson::ondemand::document_stream{};
  if (auto error = parser.iterate_many(bytes, view.size()).get(stream)) {
    return json_stream_error(error, input.eof);
  }
  auto documents = size_t{};
  for (auto doc_result : stream) {
    auto doc = doc_result.get_value();
    if (auto error = doc.error()) {
      return json_stream_error(error, input.eof);
    }
    auto result = predicate(doc);
    if (result.state != detection_state::match) {
      return result;
    }
    ++documents;
  }
  if (stream.truncated_bytes() != 0) {
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  if (documents == 0) {
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  return read_detection::match();
}

auto is_complete_json_object(std::string_view line) -> bool {
  return detect_json_object({
                              .bytes = line,
                              .eof = true,
                            })
           .state
         == detection_state::match;
}

enum class single_line_json {
  accept,
  reject,
};

auto detect_json_object_lines(read_detection_input input,
                              single_line_json single_line)
  -> read_detection_result {
  auto sample = read_detection::sample_lines(input, 3);
  std::erase_if(sample.complete, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  for (auto line : sample.complete) {
    if (not is_complete_json_object(line)) {
      return read_detection::reject();
    }
  }
  auto partial = detail::trim_front(sample.partial);
  auto partial_is_complete_object = false;
  if (not partial.empty()) {
    auto probe = detect_json_object({
      .bytes = partial,
      .eof = false,
    });
    if (probe.state == detection_state::reject) {
      return read_detection::reject();
    }
    partial_is_complete_object = probe.state == detection_state::match;
  }
  if (sample.complete.empty()) {
    if (single_line == single_line_json::accept
        and partial_is_complete_object) {
      return read_detection::match();
    }
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  if (single_line == single_line_json::reject and sample.complete.size() == 1
      and partial.empty()) {
    return input.eof ? read_detection::reject() : read_detection::need_more();
  }
  return read_detection::match();
}

auto json_object_lines_have_keys(read_detection_input input,
                                 std::initializer_list<std::string_view> keys)
  -> bool {
  auto sample = read_detection::sample_lines(input, 3);
  std::erase_if(sample.complete, [](std::string_view& line) {
    line = detail::trim_front(line);
    return line.empty();
  });
  for (auto line : sample.complete) {
    if (json_object_has_keys(line, keys)) {
      return true;
    }
  }
  auto partial = detail::trim_front(sample.partial);
  if (not partial.empty() and json_object_has_keys(partial, keys)) {
    return true;
  }
  return false;
}

auto detect_ndjson(read_detection_input input) -> read_detection_result {
  return detect_json_object_lines(input, single_line_json::reject);
}

auto detect_json_object_stream(read_detection_input input)
  -> read_detection_result {
  auto view = detail::trim_front(input.bytes);
  if (not view.empty() and view.front() != '{') {
    return read_detection::reject();
  }
  if (detect_ndjson(input).state == detection_state::match) {
    return read_detection::reject();
  }
  return detect_json_stream(input, '{', [eof = input.eof](auto& doc) {
    auto type = doc.type();
    if (auto error = type.error()) {
      return json_stream_error(error, eof);
    }
    if (type.value_unsafe() != simdjson::ondemand::json_type::object) {
      return read_detection::reject();
    }
    return read_detection::match();
  });
}

auto detect_json_array_stream(read_detection_input input)
  -> read_detection_result {
  return detect_json_stream(input, '[', [eof = input.eof](auto& doc) {
    auto array = doc.value_unsafe().get_array();
    if (auto error = array.error()) {
      return json_stream_error(error, eof);
    }
    auto first_element_is_object = true;
    for (auto element : array.value_unsafe()) {
      if (auto error = element.error()) {
        return json_stream_error(error, eof);
      }
      auto type = element.value_unsafe().type();
      if (auto error = type.error()) {
        return json_stream_error(error, eof);
      }
      first_element_is_object
        = type.value_unsafe() == simdjson::ondemand::json_type::object;
      break;
    }
    if (not first_element_is_object) {
      return read_detection::reject();
    }
    return read_detection::match();
  });
}

auto detect_json_objects(read_detection_input input) -> read_detection_result {
  auto object = detect_json_object(input);
  auto lines = detect_ndjson(input);
  if (object.state == detection_state::match
      or lines.state == detection_state::match) {
    return read_detection::match();
  }
  if (object.state == detection_state::need_more
      or lines.state == detection_state::need_more) {
    return read_detection::need_more();
  }
  return read_detection::reject();
}

auto first_null_delimited_frame(read_detection_input input)
  -> read_detection_input {
  auto end = input.bytes.find('\0');
  if (end == std::string_view::npos) {
    return input;
  }
  return {
    .bytes = input.bytes.substr(0, end),
    .eof = true,
  };
}

auto detect_json_field(read_detection_input input, std::string_view key)
  -> read_detection_result {
  auto shape = detect_json_object_lines(input, single_line_json::accept);
  if (shape.state == detection_state::match
      and json_object_lines_have_keys(input, {key})) {
    return read_detection::match();
  }
  return shape.state == detection_state::need_more ? read_detection::need_more()
                                                   : read_detection::reject();
}

auto detect_gelf(read_detection_input input) -> read_detection_result {
  auto frame = first_null_delimited_frame(input);
  auto shape = detect_json_objects(frame);
  if (shape.state == detection_state::match
      and json_object_has_keys(frame.bytes,
                               {"version", "host", "short_message"})) {
    return read_detection::match();
  }
  return shape.state == detection_state::need_more ? read_detection::need_more()
                                                   : read_detection::reject();
}

template <class Parser, class GeneratorValue>
  requires std::derived_from<std::remove_cvref_t<Parser>,
                             tenzir::json::parser_base>
auto parser_loop(generator<GeneratorValue> json_chunk_generator,
                 Parser parser_impl) -> generator<table_slice> {
  for (const auto& chunk : json_chunk_generator) {
    // get all events that are ready (timeout, batch size, ordered mode
    // constraints)
    for (auto& slice : parser_impl.builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    if (not chunk or chunk->size() == 0u) {
      co_yield {};
      continue;
    }
    if constexpr (std::same_as<chunk_ptr, GeneratorValue>) {
      parser_impl.parse(as_bytes(chunk));
    } else {
      parser_impl.parse(*chunk);
    }
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

struct printer_args {
  Option<location> compact_output;
  Option<location> color_output;
  Option<location> monochrome_output;
  Option<location> omit_all;
  Option<location> omit_null_fields;
  Option<location> omit_nulls_in_lists;
  Option<location> omit_empty_objects;
  Option<location> omit_empty_lists;
  Option<location> arrays_of_objects;
  bool tql = false;

  auto add(argument_parser2& parser, bool add_compact, bool add_arrays,
           bool add_color) -> void {
    parser.named("strip", omit_all);
    parser.named("strip_null_fields", omit_null_fields);
    parser.named("strip_nulls_in_lists", omit_nulls_in_lists);
    parser.named("strip_empty_records", omit_empty_objects);
    parser.named("strip_empty_lists", omit_empty_lists);
    if (add_compact) {
      parser.named("compact", compact_output);
    }
    if (add_arrays) {
      parser.named("arrays_of_objects", arrays_of_objects);
    }
    if (add_color) {
      parser.named("color", color_output);
    }
  }

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

struct ReadJsonArgs {
  std::string parser_name = "json";
  bool arrays_of_objects = false;
  split_at split_mode = split_at::none;
  uint64_t jobs = 0;
  OptimizationArgs<opt::Order> optimization = {};
  multi_series_builder::options msb_options;
};

class ReadJson final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadJson(ReadJsonArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await Operator<chunk_ptr, table_slice>::start(ctx);
    args_.msb_options.settings.ordered
      = args_.optimization.order == EventOrder::ordered;
    parser_ = std::make_unique<default_parser>(
      args_.parser_name, ctx.dh(), args_.msb_options, args_.arrays_of_objects);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(parser_);
    co_await pusher_.push(parser_->builder.yield_ready_as_table_slice(), push);
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(parser_);
    parser_->parse(as_bytes(input));
    if (parser_->abort_requested) {
      co_return;
    }
    co_await pusher_.push(parser_->builder.yield_ready_as_table_slice(), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(parser_);
    parser_->validate_completion();
    if (parser_->abort_requested) {
      co_return FinalizeBehavior::done;
    }
    for (auto& slice : parser_->builder.finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

private:
  ReadJsonArgs args_;
  SeriesPusher pusher_;
  std::unique_ptr<default_parser> parser_;
};

class ReadNdjson final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadNdjson(ReadJsonArgs args) : args_{std::move(args)} {
  }
  auto start(OpCtx& ctx) -> Task<void> override {
    co_await Operator<chunk_ptr, table_slice>::start(ctx);
    if (args_.jobs > 0 and args_.optimization.order != EventOrder::unordered) {
      diagnostic::error("`_jobs` requires unordered downstream")
        .hint("wrap this operator in `unordered {{ ... }}`")
        .emit(ctx.dh());
      co_return;
    }
    if (args_.jobs > 0 and finished_workers_ < args_.jobs) {
      // Parallel mode: workers have their own parsers.
      auto capacity = static_cast<uint32_t>(args_.jobs * 2);
      read_input_queue_ = std::make_shared<ReadInputQueue>(capacity);
      read_output_queue_ = std::make_shared<ReadOutputQueue>();
      // Workers use unordered output since they parse independently.
      auto msb_options = args_.msb_options;
      msb_options.settings.ordered = false;
      for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
        ctx.spawn_task(
          read_worker_loop(args_.parser_name, msb_options, ctx.dh()));
      }
      co_return;
    }
    auto msb_options = args_.msb_options;
    msb_options.settings.ordered
      = args_.optimization.order == EventOrder::ordered;
    parser_ = std::make_unique<ndjson_parser>(args_.parser_name, ctx.dh(),
                                              msb_options);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (args_.jobs > 0) {
      co_return co_await read_output_queue_->dequeue();
    } else {
      co_await pusher_.wait();
      co_return {};
    }
  }

  auto state() -> OperatorState override {
    if (not draining_) {
      return OperatorState::normal;
    }
    return finished_workers_ == args_.jobs ? OperatorState::done
                                           : OperatorState::normal;
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs > 0) {
      // Parallel mode: output from workers.
      auto slice = std::move(result).as<table_slice>();
      if (slice.rows() == 0) {
        ++finished_workers_;
        co_return;
      }
      co_await push(std::move(slice));
    } else {
      // Non-parallel mode: periodic flush.
      TENZIR_ASSERT(parser_);
      co_await pusher_.push(parser_->builder.yield_ready_as_table_slice(),
                            push);
      co_return;
    }
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs > 0) {
      co_await process_parallel(std::move(input));
    } else {
      process_sequential(std::move(input));
      TENZIR_ASSERT(parser_);
      co_await pusher_.push(parser_->builder.yield_ready_as_table_slice(),
                            push);
    }
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    draining_ = true;
    if (args_.jobs > 0) {
      // The executor may call finalize() again after all workers have finished
      // and state() returned done. In that case we are truly done.
      if (finished_workers_ >= args_.jobs) {
        co_return FinalizeBehavior::done;
      }
      // Send any remaining buffered data to a worker.
      if (not buffer_.empty()) {
        auto batch = make_padded_chunk(buffer_);
        buffer_.clear();
        co_await read_input_queue_->enqueue(std::move(batch));
      }
      // Close the input queue and drain until all workers signaled completion.
      co_await read_input_queue_->enqueue(None{});
      co_return FinalizeBehavior::continue_;
    } else {
      // Non-parallel code path.
      TENZIR_UNUSED(ctx);
      TENZIR_ASSERT(parser_);
      if (not buffer_.empty()) {
        buffer_.reserve(buffer_.size() + simdjson::SIMDJSON_PADDING);
        parser_->parse(simdjson::padded_string_view{buffer_});
        buffer_.clear();
      }
      parser_->validate_completion();
      if (parser_->abort_requested) {
        co_return FinalizeBehavior::done;
      }
      for (auto& slice : parser_->builder.finalize_as_table_slice()) {
        co_await push(std::move(slice));
      }
    }
    co_return FinalizeBehavior::done;
  }

private:
  struct PeriodicTick {};

  /// Create a chunk with simdjson padding from a string.
  static auto make_padded_chunk(std::string_view data) -> chunk_ptr {
    auto buffer
      = std::vector<std::byte>(data.size() + simdjson::SIMDJSON_PADDING);
    std::memcpy(buffer.data(), data.data(), data.size());
    std::memset(buffer.data() + data.size(), 0, simdjson::SIMDJSON_PADDING);
    return chunk::make(std::move(buffer),
                       chunk_metadata{.content_type = "application/x-ndjson"})
      ->slice(0, data.size());
  }

  /// Create a chunk with simdjson padding from a prefix + data.
  static auto make_padded_chunk(std::string_view prefix, std::string_view data)
    -> chunk_ptr {
    auto total = prefix.size() + data.size();
    auto buffer = std::vector<std::byte>(total + simdjson::SIMDJSON_PADDING);
    std::memcpy(buffer.data(), prefix.data(), prefix.size());
    std::memcpy(buffer.data() + prefix.size(), data.data(), data.size());
    std::memset(buffer.data() + total, 0, simdjson::SIMDJSON_PADDING);
    return chunk::make(std::move(buffer),
                       chunk_metadata{.content_type = "application/x-ndjson"})
      ->slice(0, total);
  }

  /// Sequential (non-parallel) processing of a chunk.
  auto process_sequential(chunk_ptr input) -> void {
    TENZIR_ASSERT(parser_);
    auto const* begin = reinterpret_cast<char const*>(input->data());
    auto const* const end = begin + input->size();
    // Handle case where previous chunk ended on carriage return.
    if (args_.split_mode == split_at::newline and ended_on_carriage_return_
        and begin != end and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (auto const* current = begin; current != end; ++current) {
      if (args_.split_mode == split_at::newline) {
        if (*current != '\n' and *current != '\r') {
          continue;
        }
      } else {
        if (*current != '\0') {
          continue;
        }
      }
      // Found delimiter: extract line.
      auto const size = static_cast<size_t>(current - begin);
      auto const capacity = static_cast<size_t>(end - begin);
      if (buffer_.empty() and capacity >= size + simdjson::SIMDJSON_PADDING) {
        parser_->parse(simdjson::padded_string_view{begin, size, capacity});
      } else {
        buffer_.append(begin, current);
        buffer_.reserve(buffer_.size() + simdjson::SIMDJSON_PADDING);
        parser_->parse(simdjson::padded_string_view{buffer_});
        buffer_.clear();
      }
      if (parser_->abort_requested) {
        return;
      }
      // Handle \r\n for newline mode.
      if (args_.split_mode == split_at::newline and *current == '\r') {
        auto const* next = current + 1;
        if (next == end) {
          ended_on_carriage_return_ = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    // Buffer remaining data for the next chunk.
    buffer_.append(begin, end);
  }

  /// Parallel processing: split at delimiter boundaries and dispatch to workers.
  auto process_parallel(chunk_ptr input) -> Task<void> {
    auto data = std::string_view{reinterpret_cast<char const*>(input->data()),
                                 input->size()};
    // Handle case where previous chunk ended on carriage return.
    if (args_.split_mode == split_at::newline and ended_on_carriage_return_
        and not data.empty() and data.front() == '\n') {
      data.remove_prefix(1);
    }
    ended_on_carriage_return_ = false;
    // Find the last delimiter in the new data (buffer_ never contains one).
    auto delim = args_.split_mode == split_at::newline ? '\n' : '\0';
    auto last_delim = data.rfind(delim);
    // Also check for \r in newline mode.
    if (args_.split_mode == split_at::newline
        and last_delim == std::string_view::npos) {
      last_delim = data.rfind('\r');
    }
    if (last_delim == std::string_view::npos) {
      // No complete line yet; keep buffering.
      buffer_.append(data);
      co_return;
    }
    // Split data into complete lines and the leftover tail.
    auto complete = data.substr(0, last_delim + 1);
    auto tail = data.substr(last_delim + 1);
    // Check for trailing \r\n split across the boundary.
    if (args_.split_mode == split_at::newline and not tail.empty()
        and data[last_delim] == '\r' and tail.front() == '\n') {
      tail.remove_prefix(1);
    }
    // Build padded chunk from buffered prefix + complete lines (one copy).
    auto batch = make_padded_chunk(buffer_, complete);
    buffer_.assign(tail.data(), tail.size());
    co_await read_input_queue_->enqueue(std::move(batch));
  }

  /// Worker coroutine that parses ndjson chunks on the CPU executor.
  /// @param dh The diagnostic handler, required to be thread-safe.
  auto read_worker_loop(std::string parser_name,
                        multi_series_builder::options msb_options,
                        diagnostic_handler& dh) const -> Task<void> {
    co_await folly::coro::co_reschedule_on_current_executor;
    auto ct = co_await folly::coro::co_current_cancellation_token;
    auto parser = ndjson_parser{parser_name, dh, msb_options};
    auto line_buffer = std::string{};
    try {
      while (true) {
        co_await folly::coro::co_reschedule_on_current_executor;
        // Use a timed dequeue so we periodically flush buffered events even
        // when input is slow. On timeout, yield_ready_as_table_slice() emits
        // any slices whose MSB timeout has expired.
        auto next = Option<chunk_ptr>{};
        try {
          next = co_await read_input_queue_->co_try_dequeue_for(
            std::chrono::duration_cast<folly::Duration>(
              msb_options.settings.timeout));
        } catch (folly::OperationCancelled const&) {
          if (ct.isCancellationRequested()) {
            throw; // real shutdown — let outer catch handle it
          }
          // Dequeue timeout: flush whatever is ready and loop.
          for (auto& slice : parser.builder.yield_ready_as_table_slice()) {
            read_output_queue_->enqueue(std::move(slice));
          }
          continue;
        }
        if (not next) {
          // Pass the stop sentinel to the next worker.
          co_await read_input_queue_->enqueue(None{});
          break;
        }
        auto const* begin = reinterpret_cast<char const*>((*next)->data());
        auto const* const end = begin + (*next)->size();
        for (auto const* current = begin; current != end; ++current) {
          if (args_.split_mode == split_at::newline) {
            if (*current != '\n' and *current != '\r') {
              continue;
            }
          } else {
            if (*current != '\0') {
              continue;
            }
          }
          auto const size = static_cast<size_t>(current - begin);
          auto const capacity = static_cast<size_t>(end - begin);
          if (line_buffer.empty()
              and capacity >= size + simdjson::SIMDJSON_PADDING) {
            parser.parse(simdjson::padded_string_view{begin, size, capacity});
          } else {
            line_buffer.append(begin, current);
            line_buffer.reserve(line_buffer.size()
                                + simdjson::SIMDJSON_PADDING);
            parser.parse(simdjson::padded_string_view{line_buffer});
            line_buffer.clear();
          }
          // Handle \r\n for newline mode.
          if (args_.split_mode == split_at::newline and *current == '\r') {
            auto const* after = current + 1;
            if (after != end and *after == '\n') {
              ++current;
            }
          }
          begin = current + 1;
        }
        // Yield ready results to the output queue.
        for (auto&& slice : parser.builder.yield_ready_as_table_slice()) {
          read_output_queue_->enqueue(std::move(slice));
        }
      }
    } catch (folly::OperationCancelled const&) {
    }
    if (not line_buffer.empty()) {
      line_buffer.reserve(line_buffer.size() + simdjson::SIMDJSON_PADDING);
      parser.parse(simdjson::padded_string_view{line_buffer});
      line_buffer.clear();
    }
    // Finalize: flush remaining data.
    for (auto& slice : parser.builder.finalize_as_table_slice()) {
      read_output_queue_->enqueue(std::move(slice));
    }
    read_output_queue_->enqueue(table_slice{});
  }

  auto drain_parallel_output(Push<table_slice>& push) -> Task<void> {
    auto seen_stop_tokens = uint64_t{0};
    while (seen_stop_tokens < args_.jobs) {
      auto next = co_await read_output_queue_->dequeue();
      if (next.rows() == 0) {
        ++seen_stop_tokens;
        continue;
      }
      co_await push(std::move(next));
    }
  }

  using ReadInputQueue = folly::coro::BoundedQueue<Option<chunk_ptr>>;
  /// The output queue is unbounded. This is intentional to avoid a theoretical
  /// deadlock, where the "main thread" wants to push to a full input queue and
  /// hence waits, while all workers want to push to a full output queue and
  /// hence wait - thus never taking any input.
  /// Making the output queue unbounded is safe here insofar that its maximum
  /// volume it could actually hold is bounded by how much we could put into the
  /// input queue. If we have backpressure from downstream though, we will not
  /// be able to push events downstream in `process_task`, hence we dont run
  /// `process` and dont accept events from upstream, not making any new work
  /// available for the workers.
  using ReadOutputQueue = folly::coro::UnboundedQueue<table_slice>;

  ReadJsonArgs args_;
  SeriesPusher pusher_;
  bool draining_ = false;
  size_t finished_workers_ = 0;
  // Non-parallel mode state:
  std::unique_ptr<ndjson_parser> parser_;
  // Shared state:
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  // Parallel mode state:
  std::shared_ptr<ReadInputQueue> read_input_queue_;
  std::shared_ptr<ReadOutputQueue> read_output_queue_;
};

class read_json_plugin final : public virtual operator_factory_plugin,
                               public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_json";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadJsonArgs, ReadJson>{};
    d.named("arrays_of_objects", &ReadJsonArgs::arrays_of_objects);
    d.optimization(&ReadJsonArgs::optimization);
    d.validate(add_msb_to_describer(d, &ReadJsonArgs::msb_options));
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"json"},
      .mime_types = {"application/json"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate("read_json arrays_of_objects=true",
                                read_detection::specificity::structured,
                                detect_json_array_stream),
      read_detection::candidate("read_json",
                                read_detection::specificity::structured,
                                detect_json_object_stream),
    };
  }
};

class read_ndjson_plugin final : public virtual operator_factory_plugin,
                                 public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_ndjson";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadJsonArgs, ReadNdjson>{
      ReadJsonArgs{.parser_name = "ndjson",
                   .split_mode = split_at::newline,
                   .msb_options = {}}};
    auto msb = add_msb_to_describer(d, &ReadJsonArgs::msb_options);
    auto jobs = d.named_optional("_jobs", &ReadJsonArgs::jobs);
    d.optimization(&ReadJsonArgs::optimization);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      msb(ctx);
      if (auto j = ctx.get(jobs); j and *j == 0) {
        diagnostic::error("`_jobs` must be greater than zero")
          .primary(ctx.get_location(jobs).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"ndjson", "jsonl", "jsonld"},
      .mime_types = {"application/x-ndjson", "application/ld+json"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_ndjson", read_detection::specificity::structured, detect_ndjson),
    };
  }
};

class read_gelf_plugin final : public virtual operator_factory_plugin,
                               public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_gelf";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadJsonArgs, ReadNdjson>{ReadJsonArgs{
      .parser_name = "gelf",
      .split_mode = split_at::null,
      .msb_options = {.settings = {.default_schema_name = "gelf"}},
    }};
    auto msb = add_msb_to_describer(d, &ReadJsonArgs::msb_options);
    auto jobs = d.named_optional("_jobs", &ReadJsonArgs::jobs);
    d.optimization(&ReadJsonArgs::optimization);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      msb(ctx);
      if (auto j = ctx.get(jobs); j and *j == 0) {
        diagnostic::error("`_jobs` must be greater than zero")
          .primary(ctx.get_location(jobs).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"gelf"},
      .mime_types = {"application/gelf", "application/x-gelf"},
    };
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_gelf", read_detection::specificity::dialect, detect_gelf),
    };
  }
};

template <detail::string_literal Name, detail::string_literal Selector,
          detail::string_literal Prefix, detail::string_literal Separator = "">
class configured_read_plugin final : public virtual operator_factory_plugin,
                                     public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return fmt::format("read_{}", Name);
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadJsonArgs, ReadNdjson>{ReadJsonArgs{
      .parser_name = std::string{Name.str()},
      .split_mode = split_at::newline,
      .msb_options =
        {
          .policy = multi_series_builder::policy_selector{
            .field_name = std::string{Selector.str()},
            .naming_prefix = std::string{Prefix.str()},
          },
          .settings =
            {
              .default_schema_name = std::string{Prefix.str()},
              .unnest_separator = std::string{Separator.str()},
            },
        },
    }};
    auto msb = add_msb_to_describer(d, &ReadJsonArgs::msb_options,
                                    {.merge = merge_option::no,
                                     .add_schema = false,
                                     .add_selector = false,
                                     .add_unflatten = false,
                                     .schema_only_requires_schema_or_selector
                                     = false});
    auto jobs = d.named_optional("_jobs", &ReadJsonArgs::jobs);
    d.optimization(&ReadJsonArgs::optimization);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      msb(ctx);
      if (ctx.get(jobs)) {
        diagnostic::error("`_jobs` is not supported for this operator in neo")
          .primary(ctx.get_location(jobs).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    if constexpr (std::string_view{Name.str()} == "suricata") {
      return {.extensions = {"eve.json"}};
    }
    if constexpr (std::string_view{Name.str()} == "zeek_json") {
      return {.extensions = {"zeek.json"}};
    }
    return {};
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    if constexpr (std::string_view{Name.str()} == "suricata") {
      return {
        read_detection::candidate(name(), read_detection::specificity::dialect,
                                  [](read_detection_input input) {
                                    return detect_json_field(input,
                                                             "event_type");
                                  }),
      };
    }
    if constexpr (std::string_view{Name.str()} == "zeek_json") {
      return {
        read_detection::candidate(name(), read_detection::specificity::dialect,
                                  [](read_detection_input input) {
                                    return detect_json_field(input, "_path");
                                  }),
      };
    }
    return {};
  }
};

using read_suricata_plugin
  = configured_read_plugin<"suricata", "event_type", "suricata">;
using read_zeek_plugin
  = configured_read_plugin<"zeek_json", "_path", "zeek", ".">;

class parse_json_plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_json";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // TODO: Consider adding a `many` option to expect multiple json values.
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make(
      [call = inv.call.get_location(), msb_opts = std::move(msb_opts),
       expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          auto f = detail::overload{
            [&](const arrow::NullArray&) -> multi_series {
              return arg;
            },
            [&](const arrow::StringArray& arg) -> multi_series {
              auto parser = simdjson::ondemand::parser{};
              /// TODO: consider keeping this builder alive
              auto builder = multi_series_builder{
                msb_opts,
                ctx,
                modules::get_schema,
                detail::data_builder::non_number_parser,
              };
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  builder.null();
                  continue;
                }
                const auto view = arg.Value(i);
                if (view.empty()) {
                  builder.null();
                  continue;
                }
                auto str = std::string{view};
                auto doc = parser.iterate(str);
                if (doc.error()) {
                  diagnostic::warning("{}", error_message(doc.error()))
                    .primary(call)
                    .emit(ctx);
                  builder.null();
                  continue;
                }
                auto doc_p = doc_parser(str, ctx);
                const auto result
                  = doc_p.parse_value(doc.value_unsafe(), builder, 0);
                switch (result) {
                  case doc_parser::result::failure_with_write:
                    builder.remove_last();
                    [[fallthrough]];
                  case doc_parser::result::failure_no_change:
                    diagnostic::warning("could not parse json")
                      .primary(call)
                      .emit(ctx);
                    builder.null();
                    break;
                  case doc_parser::result::success: /*no op*/;
                }
              }
              return multi_series{builder.finalize()};
            },
            [&](const auto&) -> multi_series {
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

struct WriteJsonArgs {
  bool color = false;
  bool compact = false;
  bool strip = false;
  bool strip_null_fields = false;
  bool strip_nulls_in_lists = false;
  bool strip_empty_records = false;
  bool strip_empty_lists = false;
  bool arrays_of_objects = false;
  bool tql = false;
  uint64_t jobs = 0;
};

class WriteJson final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteJson(WriteJsonArgs args) : args_{args} {
  }
  WriteJson(WriteJson&& other) noexcept
    : args_{std::move(other.args_)},
      opts_{other.opts_},
      array_open_written_{other.array_open_written_},
      write_input_queue_{std::move(other.write_input_queue_)},
      write_output_queue_{std::move(other.write_output_queue_)} {
    opts_.tql = args_.tql;
    if (args_.color and args_.tql) {
      opts_.style = tql_style();
    } else if (args_.color) {
      opts_.style = jq_style();
    } else {
      opts_.style = no_style();
    }
    opts_.oneline = args_.compact;
    opts_.omit_null_fields = args_.strip_null_fields or args_.strip;
    opts_.omit_nulls_in_lists = args_.strip_nulls_in_lists or args_.strip;
    opts_.omit_empty_records = args_.strip_empty_records or args_.strip;
    opts_.omit_empty_lists = args_.strip_empty_lists or args_.strip;
  }

  auto print_slice(table_slice const& input) const -> chunk_ptr {
    auto printer = tenzir::json_printer{opts_};
    // TODO: Since this printer is per-schema we can write an optimized
    // version of it that gets the schema ahead of time and only expects
    // data corresponding to exactly that schema.
    auto buffer = std::vector<char>{};
    auto resolved_slice = resolve_enumerations(input);
    auto out_iter = std::back_inserter(buffer);
    auto rows = values3(resolved_slice);
    auto row = rows.begin();
    if (args_.arrays_of_objects) {
      if (array_open_written_) {
        *out_iter++ = ',';
        if (not opts_.oneline) {
          *out_iter++ = '\n';
        }
      } else {
        out_iter = fmt::format_to(out_iter, "[");
        array_open_written_ = true;
      }
    }
    if (row != rows.end()) {
      auto const ok = printer.print(out_iter, *row);
      TENZIR_ASSERT(ok);
      ++row;
    }
    for (; row != rows.end(); ++row) {
      if (args_.arrays_of_objects) {
        *out_iter++ = ',';
        if (not opts_.oneline) {
          *out_iter++ = '\n';
        }
      } else {
        out_iter = fmt::format_to(out_iter, "\n");
      }
      auto const ok = printer.print(out_iter, *row);
      TENZIR_ASSERT(ok);
    }
    if (not args_.arrays_of_objects) {
      *out_iter++ = '\n';
    }
    auto meta = chunk_metadata{
      .content_type = opts_.oneline and not args_.arrays_of_objects
                        ? "application/x-ndjson"
                        : "application/json",
    };
    return chunk::make(std::move(buffer), meta);
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.jobs == 0) {
      co_return;
    }
    auto capacity = static_cast<uint32_t>(args_.jobs * 2);
    write_input_queue_ = std::make_shared<WriteInputQueue>(capacity);
    write_output_queue_ = std::make_shared<WriteOutputQueue>();
    for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
      ctx.spawn_task(write_worker_loop());
    }
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs == 0) {
      co_await push(print_slice(input));
      co_return;
    }
    co_await write_input_queue_->enqueue(std::move(input));
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (args_.jobs == 0) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await write_output_queue_->dequeue();
  }

  auto state() -> OperatorState override {
    if (not draining_) {
      return OperatorState::normal;
    }
    return finished_workers_ == args_.jobs ? OperatorState::done
                                           : OperatorState::normal;
  }

  auto process_task(Any result, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto next = std::move(result).as<chunk_ptr>();
    if (not next) {
      ++finished_workers_;
      co_return;
    }
    co_await push(std::move(next));
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    draining_ = true;
    if (args_.jobs > 0 and finished_workers_ < args_.jobs) {
      // Close the input queue and drain until all workers signaled completion.
      co_await write_input_queue_->enqueue(None{});
      co_return FinalizeBehavior::continue_;
    }
    TENZIR_UNUSED(ctx);
    if (not args_.arrays_of_objects) {
      co_return FinalizeBehavior::done;
    }
    auto meta = chunk_metadata{.content_type = "application/json"};
    if (not array_open_written_) {
      co_await push(chunk::copy(std::string_view{"[]"}, meta));
      co_return FinalizeBehavior::done;
    }
    co_await push(chunk::copy(std::string_view{"]"}, meta));
    co_return FinalizeBehavior::done;
  }

private:
  /// Worker coroutine that prints table slices on the CPU executor.
  auto write_worker_loop() const -> Task<void> {
    try {
      while (true) {
        co_await folly::coro::co_reschedule_on_current_executor;
        auto next = co_await write_input_queue_->dequeue();
        if (not next) {
          // Pass the stop sentinel to the next worker.
          co_await write_input_queue_->enqueue(None{});
          break;
        }
        write_output_queue_->enqueue(print_slice(*next));
      }
    } catch (folly::OperationCancelled const&) {
    }
    write_output_queue_->enqueue(chunk_ptr{});
  }

  using WriteInputQueue = folly::coro::BoundedQueue<Option<table_slice>>;
  /// @ref ReadOutputQueue
  using WriteOutputQueue = folly::coro::UnboundedQueue<chunk_ptr>;

  WriteJsonArgs args_;
  json_printer_options opts_ = {};
  mutable bool array_open_written_ = false;
  std::shared_ptr<WriteInputQueue> write_input_queue_;
  std::shared_ptr<WriteOutputQueue> write_output_queue_;
  bool draining_ = false;
  uint64_t finished_workers_ = 0;
};

class write_json_plugin final : public virtual operator_factory_plugin,
                                public virtual OperatorPlugin {
public:
  explicit write_json_plugin(bool tql) : tql_{tql} {
  }

  auto name() const -> std::string override {
    return tql_ ? "write_tql" : "write_json";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteJsonArgs, WriteJson>{WriteJsonArgs{.tql = tql_}};
    d.named("strip", &WriteJsonArgs::strip);
    d.named("strip_null_fields", &WriteJsonArgs::strip_null_fields);
    d.named("strip_nulls_in_lists", &WriteJsonArgs::strip_nulls_in_lists);
    d.named("strip_empty_records", &WriteJsonArgs::strip_empty_records);
    d.named("strip_empty_lists", &WriteJsonArgs::strip_empty_lists);
    d.named("color", &WriteJsonArgs::color);
    d.named("compact", &WriteJsonArgs::compact);
    auto jobs_arg = d.named_optional("_jobs", &WriteJsonArgs::jobs);
    if (not tql_) {
      d.named("arrays_of_objects", &WriteJsonArgs::arrays_of_objects);
      d.validate([=](DescribeCtx& ctx) -> Empty {
        if (ctx.get(jobs_arg)) {
          diagnostic::error("`_jobs` is not supported for `write_json` in neo")
            .primary(ctx.get_location(jobs_arg).value_or(location::unknown))
            .emit(ctx);
        }
        return {};
      });
    } else {
      d.validate([=](DescribeCtx& ctx) -> Empty {
        if (ctx.get(jobs_arg)) {
          diagnostic::error("`_jobs` is not supported for `write_tql` in neo")
            .primary(ctx.get_location(jobs_arg).value_or(location::unknown))
            .emit(ctx);
        }
        return {};
      });
    }
    return d.without_optimize();
  }

  auto write_properties() const -> write_properties_t override {
    if (tql_) {
      return {};
    }
    return {.extensions = {"json"}};
  }

  bool tql_ = false;
};

class write_ndjson_plugin final : public virtual operator_factory_plugin,
                                  public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_ndjson";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<WriteJsonArgs, WriteJson>{WriteJsonArgs{.compact = true}};
    d.named("strip", &WriteJsonArgs::strip);
    d.named("strip_null_fields", &WriteJsonArgs::strip_null_fields);
    d.named("strip_nulls_in_lists", &WriteJsonArgs::strip_nulls_in_lists);
    d.named("strip_empty_records", &WriteJsonArgs::strip_empty_records);
    d.named("strip_empty_lists", &WriteJsonArgs::strip_empty_lists);
    auto arrays_arg
      = d.named("arrays_of_objects", &WriteJsonArgs::arrays_of_objects);
    d.named("color", &WriteJsonArgs::color);
    auto jobs_arg = d.named_optional("_jobs", &WriteJsonArgs::jobs);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto jobs = ctx.get(jobs_arg); jobs and *jobs == 0) {
        diagnostic::error("`_jobs` must be greater than zero")
          .primary(ctx.get_location(jobs_arg).value_or(location::unknown))
          .emit(ctx);
      }
      if (ctx.get(jobs_arg) and ctx.get(arrays_arg)) {
        diagnostic::error("`arrays_of_objects` is incompatible with `_jobs`")
          .primary(ctx.get_location(jobs_arg).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {"ndjson", "jsonl"}};
  }
};

class print_json_plugin : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return compact_ ? "print_ndjson" : "print_json";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  print_json_plugin(bool compact) : compact_{compact} {
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto args = printer_args{};
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "any");
    args.add(parser, false, false, false);
    TRY(parser.parse(inv, ctx));
    auto opts = json_printer_options{
      .tql = false,
      .style = no_style(),
      .oneline = compact_,
      .omit_null_fields = args.omit_null_fields or args.omit_all,
      .omit_nulls_in_lists = args.omit_nulls_in_lists or args.omit_all,
      .omit_empty_records = args.omit_empty_objects or args.omit_all,
      .omit_empty_lists = args.omit_empty_lists or args.omit_all,
    };
    return function_use::make(
      [call = inv.call.get_location(), printer = tenzir::json_printer{opts},
       expr = std::move(expr)](evaluator eval, session) {
        return map_series(eval(expr), [&](series values) -> multi_series {
          if (values.type.kind().is<null_type>()) {
            auto builder = type_to_arrow_builder_t<string_type>{};
            for (int64_t i = 0; i < values.length(); ++i) {
              check(builder.Append("null"));
            }
            return series{string_type{}, check(builder.Finish())};
          }
          const auto work = [&](const auto& arg) -> multi_series {
            auto buffer = std::string{};
            auto builder = type_to_arrow_builder_t<string_type>{};
            for (auto row : values3(arg)) {
              if (not row) {
                check(builder.Append("null"));
                continue;
              }
              buffer.clear();
              auto it = std::back_inserter(buffer);
              printer.print(it, *row);
              check(builder.Append(buffer));
            }
            return series{string_type{}, check(builder.Finish())};
          };
          const auto resolved = resolve_enumerations(std::move(values));
          return match(*resolved.array, work);
        });
      });
  }

private:
  bool compact_;
};
} // namespace

} // namespace tenzir::plugins::json

TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_ndjson_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_gelf_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_zeek_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::read_suricata_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::parse_json_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_json_plugin{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_json_plugin{true})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::write_ndjson_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::print_json_plugin{false})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::print_json_plugin{true})
