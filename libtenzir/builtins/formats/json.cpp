//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/chunk.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/json_parser.hpp"
#include "tenzir/substitute_ctx.hpp"

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
#include <tenzir/ir.hpp>
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
// were in the anon namespace
TENZIR_ENUM(split_at, none, newline, null);

namespace {

using namespace tenzir::json;

inline auto split_at_crlf(generator<chunk_ptr> input)
  -> generator<std::optional<simdjson::padded_string_view>> {
  auto buffer = std::string{};
  bool ended_on_carriage_return = false;
  for (auto&& chunk : input) {
    if (! chunk || chunk->size() == 0) {
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
  if (! buffer.empty()) {
    buffer.reserve(buffer.size() + simdjson::SIMDJSON_PADDING);
    co_yield simdjson::padded_string_view{buffer};
  }
}
inline auto split_at_null(generator<chunk_ptr> input)
  -> generator<std::optional<simdjson::padded_string_view>> {
  auto buffer = std::string{};
  for (auto&& chunk : input) {
    if (! chunk || chunk->size() == 0) {
      co_yield std::nullopt;
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\0') {
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
  if (! buffer.empty()) {
    buffer.reserve(buffer.size() + simdjson::SIMDJSON_PADDING);
    co_yield simdjson::padded_string_view{buffer};
  }
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

/// Split the incoming byte stream at newlines such that the concatenation of
/// each resulting chunk vector is a self-contained unit for parallelization.
///
/// Only yields an empty vector if the input yielded an empty chunk, which means
/// that the operator's input buffer is exhausted.
auto split_for_parallelization(generator<chunk_ptr> input, std::byte splitter)
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
        if (byte == splitter) {
          auto end = detail::narrow<size_t>(&byte - bytes.data());
          auto rest = std::vector<chunk_ptr>{};
          // Move the remainder of the chunk where the newline is in.
          if (end + 1 != bytes.size()) {
            rest.push_back(chunk->slice(end + 1, bytes.size()));
          }
          if (end != 0) {
            chunk = chunk->slice(0, end);
          }
          // Move the subsequent chunks.
          auto chunk_index = &chunk - current.data();
          rest.insert(rest.end(),
                      std::move_iterator{current.begin() + chunk_index + 1},
                      std::move_iterator{current.end()});
          current.erase(current.begin() + chunk_index + 1, current.end());
          // Return everything up the newline and continue with the rest.
          auto result = std::move(current);
          current = std::move(rest);
          current_size = 0;
          for (auto& chunk : current) {
            current_size += chunk->size();
          }
          return result;
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
      }
      // Even if we couldn't pop anything, we still reset the timeout to prevent
      // looping there over and over again.
      next_timeout = now + timeout;
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
      if (byte == splitter) {
        auto end = detail::narrow<size_t>(&byte - bytes.data());
        if (end != 0) {
          current.push_back(chunk->slice(0, end));
          current_size += current.back()->size();
        }
        co_yield std::move(current);
        yielded = true;
        current.clear();
        current_size = 0;
        // Remember the rest of the current chunk, if there is any.
        if (end + 1 != bytes.size()) {
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
      auto input_gen = std::invoke(
        [](std::vector<chunk_ptr> input) -> generator<chunk_ptr> {
          for (auto& chunk : input) {
            co_yield std::move(chunk);
          }
        },
        std::move(input));
      auto split_gen = std::invoke([&] {
        switch (args.split_mode) {
          case split_at::newline:
            return split_at_crlf(std::move(input_gen));
          case split_at::null:
            return split_at_null(std::move(input_gen));
          case split_at::none:
            TENZIR_UNREACHABLE();
        }
        TENZIR_UNREACHABLE();
      });
      auto parsed = parser_loop<ndjson_parser&>(std::move(split_gen), parser);
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
  auto splitter = std::invoke([&] {
    switch (args.split_mode) {
      case split_at::newline:
        return std::byte{'\n'};
      case split_at::null:
        return std::byte{'\0'};
      case split_at::none:
        TENZIR_UNREACHABLE();
    }
    TENZIR_UNREACHABLE();
  });
  for (auto split : split_for_parallelization(std::move(input), splitter)) {
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
    if (args_.jobs > 0) {
      return parse_parallelized(std::move(input), args_, ctrl);
    }
    switch (args_.split_mode) {
      case split_at::newline: {
        return parser_loop(split_at_crlf(std::move(input)),
                           ndjson_parser{
                             args_.parser_name,
                             ctrl.diagnostics(),
                             args_.builder_options,
                           });
      }
      case split_at::null: {
        return parser_loop(split_at_null(std::move(input)),
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

  auto detached() const -> bool override {
    return args_.jobs > 0;
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

class json_printer final : public plugin_printer {
public:
  json_printer() = default;

  explicit json_printer(printer_args args) : args_{args} {
  }

  auto name() const -> std::string override {
    return "json";
  }

  class instance : public printer_instance {
  public:
    instance(json_printer_options opts, bool arrays_of_objects)
      : opts_{opts}, arrays_of_objects_{arrays_of_objects} {
    }

    auto make_meta() const {
      return chunk_metadata{.content_type
                            = opts_.oneline and not arrays_of_objects_
                                ? "application/x-ndjson"
                                : "application/json"};
    }

    auto process(table_slice slice) -> generator<chunk_ptr> override {
      if (slice.rows() == 0) {
        co_yield {};
        co_return;
      }
      auto printer = tenzir::json_printer{opts_};
      // TODO: Since this printer is per-schema we can write an optimized
      // version of it that gets the schema ahead of time and only expects
      // data corresponding to exactly that schema.
      auto buffer = std::vector<char>{};
      auto resolved_slice = resolve_enumerations(slice);
      auto out_iter = std::back_inserter(buffer);
      auto rows = values3(resolved_slice);
      auto row = rows.begin();
      if (arrays_of_objects_) {
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
        const auto ok = printer.print(out_iter, *row);
        TENZIR_ASSERT(ok);
        ++row;
      }
      for (; row != rows.end(); ++row) {
        if (arrays_of_objects_) {
          *out_iter++ = ',';
          if (not opts_.oneline) {
            *out_iter++ = '\n';
          }
        } else {
          out_iter = fmt::format_to(out_iter, "\n");
        }
        const auto ok = printer.print(out_iter, *row);
        TENZIR_ASSERT(ok);
      }
      if (not arrays_of_objects_) {
        *out_iter++ = '\n';
      }
      auto chunk = chunk::make(std::move(buffer), make_meta());
      co_yield std::move(chunk);
    }

    auto finish() -> generator<chunk_ptr> override {
      if (not arrays_of_objects_) {
        co_return;
      }
      if (not array_open_written_) {
        // For empty arrays, yield the entire empty array at once
        co_yield chunk::copy(std::string_view{"[]"}, make_meta());
        co_return;
      }
      co_yield chunk::copy(std::string_view{"]"}, make_meta());
    }

  private:
    const json_printer_options opts_;
    const bool arrays_of_objects_ = false;
    bool array_open_written_ = false;
  };

  auto instantiate_impl() const
    -> caf::expected<std::unique_ptr<printer_instance>> {
    auto style = default_style();
    if (args_.monochrome_output) {
      style = no_style();
    } else if (args_.color_output and args_.tql) {
      style = tql_style();
    } else if (args_.color_output) {
      style = jq_style();
    }
    return std::make_unique<instance>(
      json_printer_options{
        .tql = args_.tql,
        .style = style,
        .oneline = args_.compact_output.has_value(),
        .omit_null_fields = args_.omit_null_fields or args_.omit_all,
        .omit_nulls_in_lists = args_.omit_nulls_in_lists or args_.omit_all,
        .omit_empty_records = args_.omit_empty_objects or args_.omit_all,
        .omit_empty_lists = args_.omit_empty_lists or args_.omit_all,
      },
      args_.arrays_of_objects.has_value());
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return instantiate_impl();
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
    return std::make_unique<json_printer>(args);
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

  struct input_t {
    uint64_t index;
    table_slice slice;
  };

  auto detached() const -> bool override {
    return n_jobs_ > 0;
  }

  auto idle_after() const -> duration override {
    return n_jobs_ == 0 ? duration::zero() : duration::max();
  }

  auto parallel_operator(generator<table_slice> input) const
    -> generator<chunk_ptr> {
    auto inputs_mut = std::mutex{};
    auto inputs = std::deque<input_t>{};
    auto inputs_cv = std::condition_variable{};
    auto outputs_mut = std::mutex{};
    auto outputs = std::unordered_map<size_t, std::vector<chunk_ptr>>{};
    auto input_index = size_t{0};
    auto output_index = size_t{0};
    auto work = [&]() {
      caf::detail::set_thread_name("write_work");
      auto printer = printer_.instantiate_impl();
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
    TENZIR_ASSERT(n_jobs_ > 0);
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
        // NOLINTNEXTLINE(misc-coroutine-hostile-raii)
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
    // NOLINTNEXTLINE(misc-coroutine-hostile-raii)
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
    auto printer = printer_.instantiate(type{}, ctrl);
    TENZIR_ASSERT(printer);
    TENZIR_ASSERT(*printer);
    if (n_jobs_ > 0) {
      for (auto&& o : parallel_operator(std::move(input))) {
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

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
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
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
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
    return {
      .extensions = {"json"},
      .mime_types = {"application/json"},
    };
  }
};

class read_ndjson_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto name() const -> std::string override {
    return "read_ndjson";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{"ndjson"};
    args.split_mode = split_at::newline;
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.named_optional("_jobs", args.jobs);
    TRY(parser.parse(inv, ctx));
    TRY(args.builder_options, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<json_parser>>(
      json_parser{std::move(args)});
  }

  auto read_properties() const -> read_properties_t override {
    return {
      .extensions = {"ndjson", "jsonl", "jsonld"},
      .mime_types = {"application/x-ndjson", "application/ld+json"},
    };
  }
};

class read_gelf_plugin final
  : public virtual operator_plugin2<parser_adapter<json_parser>> {
public:
  auto name() const -> std::string override {
    return "read_gelf";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{"gelf"};
    args.split_mode = split_at::null;
    auto parser = argument_parser2::operator_(name());
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.named_optional("_jobs", args.jobs);
    TRY(parser.parse(inv, ctx));
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{std::string{Name.str()}};
    args.split_mode = split_at::newline;
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
    parser.named_optional("_jobs", args.jobs);
    TRY(parser.parse(inv, ctx));
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

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    // TODO: Consider adding a `many` option to expect multiple json values.
    auto parser = argument_parser2::function(name());
    parser.positional("x", expr, "string");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(parser, true, false);
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

class WriteJson final : public Operator<table_slice, chunk_ptr> {
public:
  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> {
    TENZIR_WARN("got table slice in JsonImpl");
    auto opts = json_printer_options{};
    opts.tql = true;
    opts.style = tql_style();
    auto printer = tenzir::json_printer{opts};
    // TODO: Since this printer is per-schema we can write an optimized
    // version of it that gets the schema ahead of time and only expects
    // data corresponding to exactly that schema.
    auto buffer = std::vector<char>{};
    auto resolved_slice = resolve_enumerations(input);
    auto out_iter = std::back_inserter(buffer);
    auto rows = values3(resolved_slice);
    auto row = rows.begin();
    if (row != rows.end()) {
      const auto ok = printer.print(out_iter, *row);
      TENZIR_ASSERT(ok);
      ++row;
    }
    for (; row != rows.end(); ++row) {
      out_iter = fmt::format_to(out_iter, "\n");
      const auto ok = printer.print(out_iter, *row);
      TENZIR_ASSERT(ok);
    }
    *out_iter++ = '\n';
    auto chunk = chunk::make(std::move(buffer));
    co_await push(std::move(chunk));
  }
};

class WriteJsonPlan final : public plan::operator_base {
public:
  auto name() const -> std::string override {
    return "WriteJsonPlan";
  }

  auto spawn() && -> AnyOperator override {
    return WriteJson{};
  }
};

class JsonIr final : public ir::operator_base {
public:
  auto name() const -> std::string override {
    return "json_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    return {};
  }

  auto finalize(element_type_tag input,
                finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(input.is<table_slice>());
    return std::make_unique<WriteJsonPlan>();
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // TODO
    return tag_v<chunk_ptr>;
  }

  friend auto inspect(auto& f, JsonIr& x) -> bool {
    return f.object(x).fields();
  }
};

class write_json_plugin final : public virtual operator_plugin2<write_json>,
                                public virtual operator_compiler_plugin {
public:
  explicit write_json_plugin(bool tql) : tql_{tql} {
  }

  auto name() const -> std::string override {
    return tql_ ? "write_tql" : "tql2.write_json";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    return std::make_unique<JsonIr>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    // TODO: More options, and consider `null_fields=false` as default.
    auto args = printer_args{};
    auto n_jobs = std::optional<located<uint64_t>>{};
    args.tql = tql_;
    auto parser = argument_parser2::operator_("write_json");
    args.add(parser, tql_, not tql_, true);
    parser.named("_jobs", n_jobs);
    TRY(parser.parse(inv, ctx));
    if (n_jobs and n_jobs->inner == 0) {
      diagnostic::error("`_jobs` must be larger than 0")
        .primary(*n_jobs)
        .emit(ctx);
      return failure::promise();
    }
    if (n_jobs and args.arrays_of_objects) {
      diagnostic::error("`arrays_of_objects` is incompatible with `_jobs`")
        .primary(*n_jobs)
        .primary(*args.arrays_of_objects)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<write_json>(args, n_jobs ? n_jobs->inner : 0);
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

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = printer_args{};
    args.compact_output = location::unknown;
    auto n_jobs = std::optional<located<uint64_t>>{};
    auto parser = argument_parser2::operator_(name());
    args.add(parser, false, true, true);
    parser.named("_jobs", n_jobs);
    TRY(parser.parse(inv, ctx));
    if (n_jobs and n_jobs->inner == 0) {
      diagnostic::error("`_jobs` must be larger than 0")
        .primary(*n_jobs)
        .emit(ctx);
      return failure::promise();
    }
    if (n_jobs and args.arrays_of_objects) {
      diagnostic::error("`arrays_of_objects` is incompatible with `_jobs`")
        .primary(*n_jobs)
        .primary(*args.arrays_of_objects)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<write_json>(args, n_jobs ? n_jobs->inner : 0);
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

  auto make_function(invocation inv, session ctx) const
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::gelf_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::suricata_parser)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::json::zeek_parser)
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
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<tenzir::ir::operator_base,
                                                 tenzir::plugins::json::JsonIr>);
