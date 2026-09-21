//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/option.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/location.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/split_at_regex.hpp>
#include <tenzir/split_at_string.hpp>
#include <tenzir/split_nulls.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/utf8.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/UnboundedQueue.h>

namespace tenzir::plugins::lines {

namespace {

struct parser_args {
  parser_args() = default;

  explicit parser_args(location self) : self{self} {
  }

  location self;
  bool binary{false};
  Option<location> skip_empty;
  Option<location> null;
  Option<located<std::string>> split_at_regex;
  Option<located<std::string>> split_at_string;
  bool include_separator{false};
  std::string field_name{"line"};

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("self", x.self), f.field("skip_empty", x.skip_empty),
              f.field("null", x.null),
              f.field("split_at_regex", x.split_at_regex),
              f.field("split_at_string", x.split_at_string),
              f.field("include_separator", x.include_separator),
              f.field("field_name", x.field_name));
  }
};

struct lines_printer_impl {
  template <typename It>
  auto print_values(It& out, const view3<record>& x) const -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (is<caf::none_t>(v)) {
        continue;
      }
      if (not first) {
        ++out = ' ';
      } else {
        first = false;
      }
      match(v, visitor{out});
    }
    return true;
  }

  template <class Iterator>
  struct visitor {
    visitor(Iterator& out) : out{out} {
    }

    auto operator()(caf::none_t) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const auto& x) -> bool {
      sequence_empty = false;
      make_printer<std::remove_cvref_t<decltype(x)>> p;
      return p.print(out, x);
    }

    auto operator()(const view3<pattern>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const view3<map>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const view3<record>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view3<std::string> x) -> bool {
      sequence_empty = false;
      out = std::copy(x.begin(), x.end(), out);
      return true;
    }

    auto operator()(view3<blob> x) -> bool {
      return (*this)(detail::base64::encode(x));
    }

    auto operator()(const view3<list>& x) -> bool {
      sequence_empty = true;
      for (const auto& v : x) {
        if (is<caf::none_t>(v)) {
          continue;
        }
        if (not sequence_empty) {
          ++out = ',';
        }
        if (not match(v, *this)) {
          return false;
        }
      }
      return true;
    }

    Iterator& out;
    bool sequence_empty{true};
  };
};

/// Arguments for the read_lines operator (new async API).
struct ReadLinesArgs {
  bool binary = false;
  bool skip_empty = false;
  uint64_t jobs = 0;

  template <class Inspector>
  friend auto inspect(Inspector& f, ReadLinesArgs& x) -> bool {
    return f.object(x)
      .pretty_name("ReadLinesArgs")
      .fields(f.field("binary", x.binary), f.field("skip_empty", x.skip_empty),
              f.field("jobs", x.jobs));
  }
};

/// The read_lines operator using the new async execution API.
/// Transforms bytes into events by splitting on newlines.
template <class Output>
class ReadLines final : public Operator<chunk_ptr, Output> {
  using Builder
    = std::conditional_t<std::same_as<Output, nova::Events>,
                         nova::ArrayBuilder<nova::Record>, series_builder>;

public:
  explicit ReadLines(ReadLinesArgs args) : args_{args} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.jobs > 0) {
      auto capacity = static_cast<uint32_t>(args_.jobs * 2);
      read_input_queue_ = std::make_shared<ReadInputQueue>(capacity);
      read_output_queue_ = std::make_shared<ReadOutputQueue>();
      for (auto i = uint64_t{0}; i < args_.jobs; ++i) {
        ctx.spawn_task(read_worker_loop(ctx.dh()));
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (args_.jobs > 0) {
      co_return co_await read_output_queue_->dequeue();
    } else {
      co_await timeout_.wait();
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

  auto process_task(Any result, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs > 0) {
      auto slice = std::move(result).as<Option<Output>>();
      if (not slice) {
        ++finished_workers_;
        co_return;
      }
      co_await push(std::move(*slice));
    } else {
      co_await push_ready(push);
      co_return;
    }
  }

  auto process(chunk_ptr input, Push<Output>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      co_return;
    }
    if (args_.jobs > 0) {
      co_await process_parallel(std::move(input));
    } else {
      process_sequential(std::move(input), ctx);
      co_await push_ready(push);
    }
  }

  auto finalize(Push<Output>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    draining_ = true;
    if (args_.jobs > 0) {
      if (finished_workers_ >= args_.jobs) {
        co_return FinalizeBehavior::done;
      }
      // Send any remaining buffered data to a worker.
      if (not buffer_.empty()) {
        auto batch = chunk::make(std::string{buffer_},
                                 chunk_metadata{.content_type = "text/plain"});
        buffer_.clear();
        co_await read_input_queue_->enqueue(std::move(batch));
      }
      co_await read_input_queue_->enqueue(None{});
      co_return FinalizeBehavior::continue_;
    }
    // Non-parallel: emit any remaining buffered data as the final line.
    if (not buffer_.empty()) {
      emit_line(buffer_, ctx);
      buffer_.clear();
    }
    co_await flush_non_parallel(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<Output>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs == 0) {
      co_await flush_non_parallel(push);
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
  }

private:
  static auto finish(Builder& builder) -> Output {
    if constexpr (std::same_as<Output, nova::Events>) {
      auto data = builder.finish();
      builder = Builder{};
      auto const length = data.length();
      return nova::Events{std::move(data), nova::storage::BitMap{length, true},
                          nova::Events::Meta::make_empty(length, TNAME)};
    } else {
      return builder.finish_assert_one_slice(TNAME);
    }
  }

  auto push_ready(Push<Output>& push) -> Task<void> {
    if (static_cast<uint64_t>(builder_.length())
          >= defaults::import::table_slice_size
        or timeout_.poll(builder_.length())) {
      co_await flush_non_parallel(push);
    }
  }

  auto flush_non_parallel(Push<Output>& push) -> Task<void> {
    if (builder_.length() > 0) {
      auto output = finish(builder_);
      timeout_.reset();
      co_await push(std::move(output));
    }
  }

  auto emit_line(std::string_view line, OpCtx& ctx) -> void {
    emit_line(line, ctx.dh(), builder_);
  }

  auto emit_line(std::string_view line, diagnostic_handler& dh,
                 Builder& builder) const -> void {
    if (line.empty() and args_.skip_empty) {
      return;
    }
    if (args_.binary) {
      builder.record().field("line").data(blob_view{as_bytes(line)});
    } else {
      if (not arrow::util::ValidateUTF8(line)) {
        diagnostic::warning("got invalid UTF-8")
          .hint("use `binary=true` if you are reading binary data")
          .emit(dh);
        return;
      }
      builder.record().field("line").data(line);
    }
  }

  /// Sequential (non-parallel) processing of a chunk.
  auto process_sequential(chunk_ptr input, OpCtx& ctx) -> void {
    auto const* begin = reinterpret_cast<char const*>(input->data());
    auto const* const end = begin + input->size();
    // Handle case where previous chunk ended on carriage return.
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      // Found a line ending.
      auto line = std::string_view{};
      if (buffer_.empty()) {
        line = std::string_view{begin, current};
      } else {
        buffer_.append(begin, current);
        line = buffer_;
      }
      emit_line(line, ctx);
      if (not buffer_.empty()) {
        buffer_.clear();
      }
      // Handle \r\n sequence.
      if (*current == '\r') {
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

  /// Find the last newline (\n or \r) position in a string view.
  static auto rfind_newline(std::string_view sv)
    -> std::string_view::size_type {
    auto pos = sv.rfind('\n');
    if (pos == std::string_view::npos) {
      pos = sv.rfind('\r');
    }
    return pos;
  }

  /// Determine the batch end accounting for \r\n pairs and set the
  /// ended_on_carriage_return_ flag when the delimiter is a bare \r at the
  /// very end of the available data.
  auto batch_end_for(std::string_view data, std::string_view::size_type delim)
    -> std::string_view::size_type {
    auto batch_end = delim + 1;
    if (data[delim] == '\r' and batch_end < data.size()
        and data[batch_end] == '\n') {
      ++batch_end;
    } else if (data[delim] == '\r' and batch_end == data.size()) {
      ended_on_carriage_return_ = true;
    }
    return batch_end;
  }

  /// Parallel processing: split at newline boundaries and dispatch to workers.
  auto process_parallel(chunk_ptr input) -> Task<void> {
    auto data = std::string_view{reinterpret_cast<char const*>(input->data()),
                                 input->size()};
    // Handle case where previous chunk ended on carriage return.
    if (ended_on_carriage_return_ and not data.empty()
        and data.front() == '\n') {
      data.remove_prefix(1);
    }
    ended_on_carriage_return_ = false;
    if (data.empty()) {
      co_return;
    }
    // Compute offset of data within the original chunk.
    auto offset = static_cast<size_t>(
      data.data() - reinterpret_cast<char const*>(input->data()));
    if (not buffer_.empty()) {
      // Find the first newline in the new data to complete the buffered line.
      auto first_delim = data.find('\n');
      if (first_delim == std::string_view::npos) {
        first_delim = data.find('\r');
      }
      if (first_delim == std::string_view::npos) {
        // No newline in this chunk at all; keep accumulating.
        buffer_.append(data);
        co_return;
      }
      // Complete the buffered line with data up to the first delimiter.
      auto first_end = first_delim + 1;
      if (data[first_delim] == '\r' and first_end < data.size()
          and data[first_end] == '\n') {
        ++first_end;
      } else if (data[first_delim] == '\r' and first_end == data.size()) {
        ended_on_carriage_return_ = true;
      }
      buffer_.append(data.substr(0, first_end));
      auto head = chunk::make(std::exchange(buffer_, {}),
                              chunk_metadata{.content_type = "text/plain"});
      co_await read_input_queue_->enqueue(std::move(head));
      // Continue with the remainder of data as the fast path.
      data.remove_prefix(first_end);
      offset += first_end;
      if (data.empty()) {
        co_return;
      }
    }
    // Fast path: slice the chunk directly.
    auto last_delim = rfind_newline(data);
    if (last_delim == std::string_view::npos) {
      buffer_.append(data);
      co_return;
    }
    auto end = batch_end_for(data, last_delim);
    auto batch = input->slice(offset, end);
    // Buffer the incomplete tail after the last delimiter.
    if (end < data.size()) {
      buffer_.append(data.substr(end));
    }
    co_await read_input_queue_->enqueue(std::move(batch));
  }

  /// Worker coroutine that splits lines on the CPU executor.
  auto read_worker_loop(diagnostic_handler& dh) const -> Task<void> {
    auto builder = Builder{};
    try {
      while (true) {
        co_await folly::coro::co_reschedule_on_current_executor;
        auto next = co_await read_input_queue_->dequeue();
        if (not next) {
          // Pass the stop sentinel to the next worker.
          co_await read_input_queue_->enqueue(None{});
          break;
        }
        auto const* begin = reinterpret_cast<char const*>((*next)->data());
        auto const* const end = begin + (*next)->size();
        for (auto const* current = begin; current != end; ++current) {
          if (*current != '\n' and *current != '\r') {
            continue;
          }
          auto line = std::string_view{begin, current};
          emit_line(line, dh, builder);
          // Handle \r\n.
          if (*current == '\r') {
            auto const* after = current + 1;
            if (after != end and *after == '\n') {
              ++current;
            }
          }
          begin = current + 1;
        }
        // Emit any trailing data (incomplete line within this chunk).
        if (begin != end) {
          auto line = std::string_view{begin, end};
          emit_line(line, dh, builder);
        }
        // Yield ready results to the output queue.
        if (builder.length() > 0) {
          read_output_queue_->enqueue(finish(builder));
        }
      }
    } catch (folly::OperationCancelled const&) {
    }
    // Finalize: flush remaining data.
    if (builder.length() > 0) {
      read_output_queue_->enqueue(finish(builder));
    }
    read_output_queue_->enqueue(None{});
  }

  constexpr static const auto TNAME = "tenzir.line";

  using ReadInputQueue = folly::coro::BoundedQueue<Option<chunk_ptr>>;
  /// The output queue is unbounded to avoid a theoretical deadlock where the
  /// main thread wants to push to a full input queue while all workers want to
  /// push to a full output queue.
  using ReadOutputQueue = folly::coro::UnboundedQueue<Option<Output>>;

  ReadLinesArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  bool draining_ = false;
  size_t finished_workers_ = 0;
  // Non-parallel mode state.
  Builder builder_;
  BatchTimeout timeout_{defaults::import::batch_timeout};
  // Parallel mode state.
  std::shared_ptr<ReadInputQueue> read_input_queue_;
  std::shared_ptr<ReadOutputQueue> read_output_queue_;
};

} // namespace

class read_lines_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_lines";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadLinesArgs, ReadLines<table_slice>,
                       ReadLines<nova::Events>>{};
    d.named("binary", &ReadLinesArgs::binary);
    d.named("skip_empty", &ReadLinesArgs::skip_empty);
    auto jobs = d.named_optional("_jobs", &ReadLinesArgs::jobs);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto j = ctx.get(jobs); j and *j == 0) {
        diagnostic::error("`_jobs` must be greater than zero")
          .primary(ctx.get_location(jobs).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

struct WriteLinesArgs {
  uint64_t jobs = 0;

  template <class Inspector>
  friend auto inspect(Inspector& f, WriteLinesArgs& x) -> bool {
    return f.object(x)
      .pretty_name("WriteLinesArgs")
      .fields(f.field("jobs", x.jobs));
  }
};

auto print_nova_value(nova::RowView<nova::Data> const& value,
                      std::vector<char>& output, char separator, bool& first)
  -> void {
  auto const start = output.size();
  if (not first) {
    output.push_back(separator);
  }
  auto const printed = match(
    value,
    [](nova::RowView<nova::Null>) {
      return false;
    },
    [&](nova::RowView<nova::Record> const& record) {
      auto first_field = true;
      for (auto const& [name, field] : record) {
        print_nova_value(field, output, ' ', first_field);
      }
      return not first_field;
    },
    [&](nova::RowView<nova::List> const& list) {
      auto first_element = true;
      for (auto element : list) {
        print_nova_value(element, output, ',', first_element);
      }
      return not first_element;
    },
    [&](auto scalar) {
      auto out = std::back_inserter(output);
      auto printer = lines_printer_impl::visitor{out};
      auto const ok = printer(*scalar);
      TENZIR_ASSERT(ok);
      return true;
    });
  if (printed) {
    first = false;
  } else {
    output.resize(start);
  }
}

template <class Input>
class WriteLines final : public Operator<Input, chunk_ptr> {
public:
  explicit WriteLines(WriteLinesArgs args) : args_{args} {
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

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    if (args_.jobs == 0) {
      co_return co_await Operator<Input, chunk_ptr>::await_task(dh);
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

  auto process(Input input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (args_.jobs > 0) {
      co_await write_input_queue_->enqueue(std::move(input));
      co_return;
    }
    co_await push(print_slice(input));
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    draining_ = true;
    if (args_.jobs > 0) {
      if (finished_workers_ >= args_.jobs) {
        co_return FinalizeBehavior::done;
      }
      co_await write_input_queue_->enqueue(None{});
      co_return FinalizeBehavior::continue_;
    }
    co_return FinalizeBehavior::done;
  }

private:
  static auto print_slice(Input const& input) -> chunk_ptr {
    auto buffer = std::vector<char>{};
    if constexpr (std::same_as<Input, nova::Events>) {
      for (auto row : nova::storage::true_bits(input.mask)) {
        auto first = true;
        print_nova_value(input.data.get(row), buffer, ' ', first);
        buffer.push_back('\n');
      }
    } else {
      auto printer = lines_printer_impl{};
      auto out_iter = std::back_inserter(buffer);
      auto resolved_slice = flatten(resolve_enumerations(input)).slice;
      auto array = check(to_record_batch(resolved_slice)->ToStructArray());
      for (auto const& row : values3(*array)) {
        TENZIR_ASSERT(row);
        auto const ok = printer.print_values(out_iter, *row);
        TENZIR_ASSERT(ok);
        out_iter = fmt::format_to(out_iter, "\n");
      }
    }
    return chunk::make(std::move(buffer),
                       chunk_metadata{.content_type = "text/plain"});
  }

  auto write_worker_loop() const -> Task<void> {
    try {
      while (true) {
        co_await folly::coro::co_reschedule_on_current_executor;
        auto next = co_await write_input_queue_->dequeue();
        if (not next) {
          co_await write_input_queue_->enqueue(None{});
          break;
        }
        write_output_queue_->enqueue(print_slice(*next));
      }
    } catch (folly::OperationCancelled const&) {
    }
    write_output_queue_->enqueue(chunk_ptr{});
  }

  using WriteInputQueue = folly::coro::BoundedQueue<Option<Input>>;
  using WriteOutputQueue = folly::coro::UnboundedQueue<chunk_ptr>;

  WriteLinesArgs args_;
  bool draining_ = false;
  uint64_t finished_workers_ = 0;
  std::shared_ptr<WriteInputQueue> write_input_queue_;
  std::shared_ptr<WriteOutputQueue> write_output_queue_;
};

class write_lines final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_lines";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteLinesArgs, WriteLines<table_slice>,
                       WriteLines<nova::Events>>{};
    auto jobs = d.named_optional("_jobs", &WriteLinesArgs::jobs);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto j = ctx.get(jobs); j and *j == 0) {
        diagnostic::error("`_jobs` must be greater than zero")
          .primary(ctx.get_location(jobs).value_or(location::unknown))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace tenzir::plugins::lines

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_lines_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::write_lines)
