//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder_argument_parser.hpp"

#include <tenzir/async/pusher.hpp>
#include <tenzir/detail/overload.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
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
  if (unary_expr.op.inner == ast::unary_op::pos) {
    return append_expression_to_builder(unary_expr.expr, gen, depth + 1);
  }
  auto* constant = try_as<ast::constant>(unary_expr.expr);
  if (not constant) {
    return append_result::unsupported_expression;
  }
  if (unary_expr.op.inner == ast::unary_op::not_) {
    auto* value = try_as<bool>(constant->value);
    if (not value) {
      return append_result::unsupported_expression;
    }
    gen.data(not *value);
    return append_result::success;
  }
  if (unary_expr.op.inner != ast::unary_op::neg) {
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

// Empty args struct for read_tql (no arguments needed)
struct ReadTqlArgs {
  multi_series_builder::options msb_options;
};

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
    if (done_ or not input or input->size() == 0) {
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
    return done_ ? OperatorState::done : OperatorState::unspecified;
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
      auto parsed
        = parse_expression_stream_with_bad_diagnostics(view, sp.as_session());
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
  std::optional<multi_series_builder> builder_;
  bool done_ = false;
};

class read_tql_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_tql";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadTqlArgs, ReadTql>{};
    auto msb = add_msb_to_describer(d, &ReadTqlArgs::msb_options);
    d.validate(msb);
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::tql

TENZIR_REGISTER_PLUGIN(tenzir::plugins::tql::read_tql_plugin)
