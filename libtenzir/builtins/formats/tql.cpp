//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder_argument_parser.hpp"
#include "tenzir/option.hpp"

#include <tenzir/async/pusher.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova_flag.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace tenzir::plugins::tql {
namespace {

// Maximum buffer size to prevent DoS from malformed input (64 MiB).
constexpr auto max_record_size = size_t{64} * 1024 * 1024;

// Maximum nesting depth to prevent stack overflow from deeply nested structures.
constexpr auto max_nesting_depth = size_t{256};

enum class append_result : uint8_t {
  success,
  not_record,
  unsupported_expression,
  nesting_too_deep,
};

auto is_only_whitespace(std::string_view input) -> bool {
  return std::all_of(input.begin(), input.end(), [](char c) {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
  });
}

namespace legacy {

template <class Generator>
auto append_expression_to_builder(const ast::expression& expr, Generator& gen,
                                  size_t depth) -> append_result;

template <class Generator>
auto append_constant_to_builder(const ast::constant& constant, Generator& gen)
  -> append_result {
  return constant.value.match([&](const auto& value) -> append_result {
    using value_type = std::remove_cvref_t<decltype(value)>;
    if constexpr (std::is_same_v<value_type, map>
                  or std::is_same_v<value_type, secret>) {
      return append_result::unsupported_expression;
    } else {
      gen.data(value);
      return append_result::success;
    }
  });
}

auto append_record_to_builder(const ast::record& record_expr,
                              multi_series_builder::record_generator& rec_gen,
                              size_t depth) -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  for (const auto& item : record_expr.items) {
    auto* field = try_as<ast::record::field>(item);
    if (not field) {
      return append_result::unsupported_expression;
    }
    auto field_gen = rec_gen.exact_field(field->name.name);
    auto status
      = append_expression_to_builder(field->expr, field_gen, depth + 1);
    if (status != append_result::success) {
      return status;
    }
  }
  return append_result::success;
}

auto append_list_to_builder(const ast::list& list_expr,
                            multi_series_builder::list_generator& list_gen,
                            size_t depth) -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  for (const auto& item : list_expr.items) {
    auto* expr = try_as<ast::expression>(item);
    if (not expr) {
      return append_result::unsupported_expression;
    }
    auto status = append_expression_to_builder(*expr, list_gen, depth + 1);
    if (status != append_result::success) {
      return status;
    }
  }
  return append_result::success;
}

template <class Generator>
auto append_unary_to_builder(const ast::unary_expr& unary_expr, Generator& gen,
                             size_t depth) -> append_result {
  if (unary_expr.op == ast::unary_op::pos) {
    return append_expression_to_builder(unary_expr.expr, gen, depth + 1);
  }
  auto* constant = try_as<ast::constant>(unary_expr.expr);
  if (not constant) {
    return append_result::unsupported_expression;
  }
  if (unary_expr.op == ast::unary_op::not_) {
    auto* value = try_as<bool>(constant->value);
    if (not value) {
      return append_result::unsupported_expression;
    }
    gen.data(not *value);
    return append_result::success;
  }
  if (unary_expr.op != ast::unary_op::neg) {
    return append_result::unsupported_expression;
  }
  return constant->value.match(detail::overload{
    [&](int64_t value) -> append_result {
      if (value == std::numeric_limits<int64_t>::min()) {
        return append_result::unsupported_expression;
      }
      gen.data(-value);
      return append_result::success;
    },
    [&](uint64_t value) -> append_result {
      constexpr auto max_plus_one
        = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
      if (value > max_plus_one) {
        return append_result::unsupported_expression;
      }
      if (value == max_plus_one) {
        gen.data(std::numeric_limits<int64_t>::min());
      } else {
        gen.data(-static_cast<int64_t>(value));
      }
      return append_result::success;
    },
    [&](double value) -> append_result {
      gen.data(-value);
      return append_result::success;
    },
    [&](duration value) -> append_result {
      gen.data(-value);
      return append_result::success;
    },
    [&](const auto&) -> append_result {
      return append_result::unsupported_expression;
    },
  });
}

template <class Generator>
auto append_expression_to_builder(const ast::expression& expr, Generator& gen,
                                  size_t depth) -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  if (const auto* constant = try_as<ast::constant>(expr)) {
    return append_constant_to_builder(*constant, gen);
  }
  if (const auto* unary = try_as<ast::unary_expr>(expr)) {
    return append_unary_to_builder(*unary, gen, depth);
  }
  if (const auto* list_expr = try_as<ast::list>(expr)) {
    auto list_gen = gen.list();
    return append_list_to_builder(*list_expr, list_gen, depth + 1);
  }
  if (const auto* record_expr = try_as<ast::record>(expr)) {
    auto rec_gen = gen.record();
    return append_record_to_builder(*record_expr, rec_gen, depth + 1);
  }
  return append_result::unsupported_expression;
}

auto add_record_expression_to_builder(const ast::expression& expr,
                                      multi_series_builder& builder)
  -> append_result {
  auto* record_expr = try_as<ast::record>(expr);
  if (not record_expr) {
    return append_result::not_record;
  }
  auto rec_gen = builder.record();
  auto status = append_record_to_builder(*record_expr, rec_gen, 0);
  if (status != append_result::success) {
    builder.remove_last();
  }
  return status;
}

} // namespace legacy

/// A builder stand-in that accepts everything and builds nothing. Array
/// builders cannot drop a partially built row, so we first check whether an
/// expression is representable by appending it here.
struct Discard {
  auto null() -> void {
  }

  auto data(auto const&) -> void {
  }

  auto record() -> Discard {
    return {};
  }

  auto list() -> Discard {
    return {};
  }

  auto field(std::string_view) -> Discard {
    return {};
  }
};

auto append_expression(ast::expression const& expr, auto&& out, size_t depth)
  -> append_result;

auto append_constant(ast::constant const& constant, auto&& out)
  -> append_result {
  return constant.value.match(detail::overload{
    [&](caf::none_t) -> append_result {
      out.null();
      return append_result::success;
    },
    [&](std::string const& value) -> append_result {
      out.data(std::string_view{value});
      return append_result::success;
    },
    [&](blob const& value) -> append_result {
      out.data(blob_view{value});
      return append_result::success;
    },
    [&]<class T>(T const& value) -> append_result
      requires(concepts::one_of<T, bool, int64_t, uint64_t, double, duration,
                                time, ip, subnet>)
    {
      out.data(value);
      return append_result::success;
    },
    [&](auto const&) -> append_result {
      return append_result::unsupported_expression;
    },
    });
}

auto append_unary(ast::unary_expr const& unary_expr, auto&& out, size_t depth)
  -> append_result {
  if (unary_expr.op == ast::unary_op::pos) {
    return append_expression(unary_expr.expr, out, depth + 1);
  }
  auto const* constant = try_as<ast::constant>(unary_expr.expr);
  if (not constant) {
    return append_result::unsupported_expression;
  }
  if (unary_expr.op == ast::unary_op::not_) {
    auto const* value = try_as<bool>(constant->value);
    if (not value) {
      return append_result::unsupported_expression;
    }
    out.data(not *value);
    return append_result::success;
  }
  if (unary_expr.op != ast::unary_op::neg) {
    return append_result::unsupported_expression;
  }
  return constant->value.match(detail::overload{
    [&](int64_t value) -> append_result {
      if (value == std::numeric_limits<int64_t>::min()) {
        return append_result::unsupported_expression;
      }
      out.data(int64_t{-value});
      return append_result::success;
    },
    [&](uint64_t value) -> append_result {
      constexpr auto max_plus_one
        = static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1;
      if (value > max_plus_one) {
        return append_result::unsupported_expression;
      }
      out.data(value == max_plus_one ? std::numeric_limits<int64_t>::min()
                                     : -static_cast<int64_t>(value));
      return append_result::success;
    },
    [&](double value) -> append_result {
      out.data(-value);
      return append_result::success;
    },
    [&](duration value) -> append_result {
      out.data(-value);
      return append_result::success;
    },
    [&](auto const&) -> append_result {
      return append_result::unsupported_expression;
    },
  });
}

auto append_record(ast::record const& record_expr, auto&& out, size_t depth)
  -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  for (auto const& item : record_expr.items) {
    auto const* field = try_as<ast::record::field>(item);
    if (not field) {
      return append_result::unsupported_expression;
    }
    auto status
      = append_expression(field->expr, out.field(field->name.name), depth + 1);
    if (status != append_result::success) {
      return status;
    }
  }
  return append_result::success;
}

auto append_list(ast::list const& list_expr, auto&& out, size_t depth)
  -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  for (auto const& item : list_expr.items) {
    auto const* expr = try_as<ast::expression>(item);
    if (not expr) {
      return append_result::unsupported_expression;
    }
    auto status = append_expression(*expr, out, depth + 1);
    if (status != append_result::success) {
      return status;
    }
  }
  return append_result::success;
}

auto append_expression(ast::expression const& expr, auto&& out, size_t depth)
  -> append_result {
  if (depth > max_nesting_depth) {
    return append_result::nesting_too_deep;
  }
  if (auto const* constant = try_as<ast::constant>(expr)) {
    return append_constant(*constant, out);
  }
  if (auto const* unary = try_as<ast::unary_expr>(expr)) {
    return append_unary(*unary, out, depth);
  }
  if (auto const* list_expr = try_as<ast::list>(expr)) {
    return append_list(*list_expr, out.list(), depth + 1);
  }
  if (auto const* record_expr = try_as<ast::record>(expr)) {
    return append_record(*record_expr, out.record(), depth + 1);
  }
  return append_result::unsupported_expression;
}

/// Appends a top-level record expression as one row of `builder`, leaving the
/// builder untouched if the expression cannot be represented.
auto add_record_expression(ast::expression const& expr,
                           nova::ArrayBuilder<nova::Record>& builder)
  -> append_result {
  auto const* record_expr = try_as<ast::record>(expr);
  if (not record_expr) {
    return append_result::not_record;
  }
  auto status = append_record(*record_expr, Discard{}, 0);
  if (status != append_result::success) {
    return status;
  }
  status = append_record(*record_expr, builder.record(), 0);
  TENZIR_ASSERT(status == append_result::success);
  return status;
}

// Empty args struct for read_tql (no arguments needed)
struct ReadTqlArgs {
  multi_series_builder::options msb_options;
};

namespace legacy {

class ReadTql final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadTql(ReadTqlArgs args) : opts_{std::move(args.msb_options)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    builder_.emplace(opts_, ctx);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(builder_);
    co_await pusher_.push(builder_->yield_ready_as_table_slice(), push);
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    // Append new data to buffer
    buffer_.append(reinterpret_cast<const char*>(input->data()), input->size());
    // Check buffer size limit
    if (buffer_.size() - buffer_offset_ > max_record_size) {
      diagnostic::error("input buffer exceeds maximum size of 64 MiB").emit(ctx);
      // Prevent unbounded growth for malformed/unterminated input.
      buffer_.clear();
      buffer_offset_ = 0;
      done_ = true;
      co_return;
    }
    // Process complete records from buffer
    process_buffer(ctx, false);
    if (done_ or not builder_) {
      co_return;
    }
    co_await pusher_.push(builder_->yield_ready_as_table_slice(), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    // Ensure builder exists even if no data was processed
    if (not builder_) {
      builder_.emplace(multi_series_builder::options{}, ctx);
    }
    // Process any remaining complete records in buffer
    process_buffer(ctx, true);
    // Finalize and yield remaining slices
    for (auto& slice : builder_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  /// Compacts the buffer by removing already-processed data at the front.
  void compact_buffer() {
    if (buffer_offset_ > 0) {
      buffer_.erase(0, buffer_offset_);
      buffer_offset_ = 0;
    }
  }

  /// Returns a view of the unprocessed portion of the buffer.
  auto buffer_view() const -> std::string_view {
    return std::string_view{buffer_}.substr(buffer_offset_);
  }

  /// Advances the buffer offset, compacting periodically to avoid unbounded
  /// memory growth. The compaction threshold is set at half the buffer size.
  void advance_buffer(size_t n) {
    buffer_offset_ += n;
    // Compact when offset exceeds half the buffer size
    if (buffer_offset_ > buffer_.size() / 2) {
      compact_buffer();
    }
  }

  /// Processes complete records from the buffer.
  /// If is_final is true, emits a warning for incomplete trailing records.
  auto process_buffer(OpCtx& ctx, bool is_final) -> void {
    auto sp = session_provider::make(ctx.dh());
    while (buffer_offset_ < buffer_.size()) {
      auto view = buffer_view();
      if (view.empty()) {
        compact_buffer();
        break;
      }
      auto parsed = parse_expression_stream_with_location_override(
        view, location::unknown, sp.as_session());
      if (not parsed) {
        buffer_.clear();
        buffer_offset_ = 0;
        done_ = true;
        break;
      }
      if (parsed->bytes_consumed == 0) {
        if (is_final and not is_only_whitespace(view)) {
          diagnostic::warning("incomplete record at end of input").emit(ctx);
        }
        break;
      }
      for (const auto& expr : parsed->expressions) {
        auto status = add_record_expression_to_builder(expr, *builder_);
        switch (status) {
          case append_result::success:
            break;
          case append_result::not_record:
            diagnostic::warning("expected record at top level, got other type")
              .emit(ctx);
            break;
          case append_result::unsupported_expression:
            diagnostic::warning("expected constant record expression in stream")
              .emit(ctx);
            break;
          case append_result::nesting_too_deep:
            diagnostic::warning("record nesting exceeds limit of {} levels",
                                max_nesting_depth)
              .emit(ctx);
            break;
        }
      }
      // Advance past the parsed prefix.
      advance_buffer(parsed->bytes_consumed);
      if (parsed->has_error) {
        buffer_.clear();
        buffer_offset_ = 0;
        done_ = true;
        break;
      }
    }
  }

  multi_series_builder::options opts_;
  SeriesPusher pusher_;
  size_t buffer_offset_ = 0;
  std::string buffer_;
  Option<multi_series_builder> builder_;
  bool done_ = false;
};

} // namespace legacy

class ReadTql final : public Operator<chunk_ptr, nova::Events> {
public:
  explicit ReadTql(ReadTqlArgs args)
    : opts_{std::move(args.msb_options)}, timeout_{opts_.settings.timeout} {
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await timeout_.wait();
    co_return {};
  }

  auto process_task(Any, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    if (timeout_.poll(rows())) {
      co_await flush(push);
    }
  }

  auto process(chunk_ptr input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    buffer_.append(reinterpret_cast<char const*>(input->data()), input->size());
    if (buffer_.size() - buffer_offset_ > max_record_size) {
      diagnostic::error("input buffer exceeds maximum size of 64 MiB").emit(ctx);
      // Prevent unbounded growth for malformed/unterminated input.
      buffer_.clear();
      buffer_offset_ = 0;
      done_ = true;
      co_return;
    }
    co_await process_buffer(push, ctx, false);
    if (timeout_.poll(rows())) {
      co_await flush(push);
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    co_await process_buffer(push, ctx, true);
    co_await flush(push);
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    compact_buffer();
    serde("buffer", buffer_);
    serde("done", done_);
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto rows() const -> size_t {
    return static_cast<size_t>(builder_.length());
  }

  auto flush(Push<nova::Events>& push) -> Task<void> {
    if (rows() == 0) {
      co_return;
    }
    auto data = builder_.finish();
    builder_ = nova::ArrayBuilder<nova::Record>{};
    timeout_.reset();
    auto const length = data.length();
    co_await push(nova::Events{std::move(data),
                               nova::storage::BitMap{length, true},
                               nova::Events::Meta::make_empty(
                                 length, opts_.settings.default_schema_name)});
  }

  /// Removes already-processed data at the front of the buffer.
  auto compact_buffer() -> void {
    if (buffer_offset_ > 0) {
      buffer_.erase(0, buffer_offset_);
      buffer_offset_ = 0;
    }
  }

  /// Advances the buffer offset, compacting once it exceeds half the buffer.
  auto advance_buffer(size_t n) -> void {
    buffer_offset_ += n;
    if (buffer_offset_ > buffer_.size() / 2) {
      compact_buffer();
    }
  }

  /// Processes complete records from the buffer, flushing whenever a batch is
  /// full. If `is_final` is true, warns about an incomplete trailing record.
  auto process_buffer(Push<nova::Events>& push, OpCtx& ctx, bool is_final)
    -> Task<void> {
    auto sp = session_provider::make(ctx.dh());
    while (buffer_offset_ < buffer_.size()) {
      auto view = std::string_view{buffer_}.substr(buffer_offset_);
      auto parsed = parse_expression_stream_with_location_override(
        view, location::unknown, sp.as_session());
      if (not parsed) {
        buffer_.clear();
        buffer_offset_ = 0;
        done_ = true;
        break;
      }
      if (parsed->bytes_consumed == 0) {
        if (is_final and not is_only_whitespace(view)) {
          diagnostic::warning("incomplete record at end of input").emit(ctx);
        }
        break;
      }
      for (auto const& expr : parsed->expressions) {
        switch (add_record_expression(expr, builder_)) {
          case append_result::success:
            break;
          case append_result::not_record:
            diagnostic::warning("expected record at top level, got other type")
              .emit(ctx);
            break;
          case append_result::unsupported_expression:
            diagnostic::warning("expected constant record expression in stream")
              .emit(ctx);
            break;
          case append_result::nesting_too_deep:
            diagnostic::warning("record nesting exceeds limit of {} levels",
                                max_nesting_depth)
              .emit(ctx);
            break;
        }
        if (rows() >= opts_.settings.desired_batch_size) {
          co_await flush(push);
        }
      }
      advance_buffer(parsed->bytes_consumed);
      if (parsed->has_error) {
        buffer_.clear();
        buffer_offset_ = 0;
        done_ = true;
        break;
      }
    }
  }

  multi_series_builder::options opts_;
  BatchTimeout timeout_;
  size_t buffer_offset_ = 0;
  std::string buffer_;
  nova::ArrayBuilder<nova::Record> builder_;
  bool done_ = false;
};

class read_tql_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_tql";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadTqlArgs, legacy::ReadTql, ReadTql>{};
    auto msb = add_msb_to_describer(d, &ReadTqlArgs::msb_options);
    d.validate([msb](DescribeCtx& ctx) -> Empty {
      msb(ctx);
      if (not nova_enabled()) {
        return {};
      }
      // The default implementation keeps every record as written and does not
      // implement the schema policies yet.
      auto reject = [&](auto const& arg, std::string_view name) {
        if (auto loc = ctx.get_location(arg)) {
          diagnostic::error("`{}` is not supported with `--nova` yet", name)
            .primary(*loc)
            .emit(ctx);
        }
      };
      reject(msb.schema, "schema");
      reject(msb.selector, "selector");
      reject(msb.schema_only, "schema_only");
      reject(msb.merge, "merge");
      reject(msb.unflatten_separator, "unflatten_separator");
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::tql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tql::read_tql_plugin)
