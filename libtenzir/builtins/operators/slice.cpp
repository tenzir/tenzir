//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>

#include <ranges>

namespace tenzir::plugins::slice {

namespace {

struct SliceArgs {
  std::optional<int64_t> begin;
  std::optional<int64_t> end;
  std::optional<int64_t> stride;
};

class Slice final : public Operator<table_slice, table_slice> {
public:
  explicit Slice(SliceArgs args)
    : begin_{args.begin}, end_{args.end}, stride_{args.stride} {
    // Buffering is required when any index is negative or stride is negative
    needs_buffering_ = (stride_ && *stride_ < 0) || (begin_ && *begin_ < 0)
                       || (end_ && *end_ < 0);
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (needs_buffering_) {
      offset_ += static_cast<int64_t>(input.rows());
      buffer_.push_back(std::move(input));
      co_return;
    }
    // Streaming mode: positive begin, positive end, positive stride
    auto result = process_streaming(std::move(input));
    if (result.rows() > 0) {
      co_await push(std::move(result));
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (not needs_buffering_) {
      co_return;
    }
    // Resolve negative indices using total row count (offset_)
    auto begin = begin_.value_or(0);
    auto end = end_.value_or(offset_);
    if (begin < 0) {
      begin = offset_ + begin;
    }
    if (end < 0) {
      end = offset_ + end;
    }
    // Clamp to valid range
    begin = std::max(begin, int64_t{0});
    end = std::clamp(end, int64_t{0}, offset_);
    if (end <= begin) {
      co_return;
    }
    // Apply slice to buffer
    auto stride = stride_.value_or(1);
    if (stride > 0) {
      co_await finalize_positive_stride(push, begin, end, stride);
    } else {
      co_await finalize_negative_stride(push, begin, end, stride);
    }
  }

  auto state() -> OperatorState override {
    if (not needs_buffering_ && done_) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("offset", offset_);
    serde("stride_offset", stride_offset_);
    serde("done", done_);
  }

private:
  auto process_streaming(table_slice input) -> table_slice {
    const auto rows = static_cast<int64_t>(input.rows());
    if (rows == 0) {
      return {};
    }
    auto begin = begin_.value_or(0);
    auto end = end_.value_or(std::numeric_limits<int64_t>::max());
    auto stride = stride_.value_or(1);
    // Compute clamped range for this slice
    const auto clamped_begin = std::clamp(begin - offset_, int64_t{0}, rows);
    const auto clamped_end = std::clamp(end - offset_, int64_t{0}, rows);
    offset_ += rows;
    // Check if we're past the end
    if (offset_ >= end) {
      done_ = true;
    }
    if (clamped_end <= clamped_begin) {
      return {};
    }
    auto result = subslice(input, clamped_begin, clamped_end);
    // Apply stride if needed
    if (stride > 1) {
      result = apply_positive_stride(std::move(result), stride);
    }
    return result;
  }

  auto apply_positive_stride(table_slice input, int64_t stride) -> table_slice {
    if (input.rows() == 0) {
      return input;
    }
    const auto rows = static_cast<int64_t>(input.rows());
    auto b = int64_type::make_arrow_builder(arrow_memory_pool());
    check(b->Reserve((rows + stride - 1) / stride));
    for (auto i = stride_offset_ % stride; i < rows; i += stride) {
      check(b->Append(i));
    }
    stride_offset_ += rows;
    auto stride_index = finish(*b);
    if (stride_index->length() == 0) {
      return {};
    }
    auto batch = to_record_batch(input);
    auto take_result = arrow::compute::Take(batch, stride_index);
    if (not take_result.ok()) {
      diagnostic::error("{}", take_result.status().ToString())
        .note("failed to apply stride")
        .throw_();
    }
    const auto datum = take_result.MoveValueUnsafe();
    TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
    return table_slice{datum.record_batch(), input.schema()};
  }

  auto finalize_positive_stride(Push<table_slice>& push, int64_t begin,
                                int64_t end, int64_t stride) -> Task<void> {
    auto current_offset = int64_t{0};
    for (auto& slice : buffer_) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin
        = std::clamp(begin - current_offset, int64_t{0}, rows);
      const auto clamped_end
        = std::clamp(end - current_offset, int64_t{0}, rows);
      current_offset += rows;
      if (clamped_end <= clamped_begin) {
        continue;
      }
      auto result = subslice(slice, clamped_begin, clamped_end);
      if (stride > 1) {
        result = apply_positive_stride(std::move(result), stride);
      }
      if (result.rows() > 0) {
        co_await push(std::move(result));
      }
    }
  }

  auto finalize_negative_stride(Push<table_slice>& push, int64_t begin,
                                int64_t end, int64_t stride) -> Task<void> {
    // First, collect the sliced data
    auto sliced = std::vector<table_slice>{};
    auto current_offset = int64_t{0};
    for (auto& slice : buffer_) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin
        = std::clamp(begin - current_offset, int64_t{0}, rows);
      const auto clamped_end
        = std::clamp(end - current_offset, int64_t{0}, rows);
      current_offset += rows;
      if (clamped_end <= clamped_begin) {
        continue;
      }
      sliced.push_back(subslice(slice, clamped_begin, clamped_end));
    }
    // Process in reverse order with negative stride
    auto reverse_offset = int64_t{0};
    for (auto& slice : sliced | std::ranges::views::reverse) {
      const auto rows = static_cast<int64_t>(slice.rows());
      auto result
        = apply_negative_stride(std::move(slice), stride, reverse_offset);
      reverse_offset += rows;
      if (result.rows() > 0) {
        co_await push(std::move(result));
      }
    }
  }

  auto apply_negative_stride(table_slice input, int64_t stride,
                             int64_t reverse_offset) -> table_slice {
    if (input.rows() == 0) {
      return input;
    }
    const auto rows = static_cast<int64_t>(input.rows());
    const auto abs_stride = -stride;
    auto b = int64_type::make_arrow_builder(arrow_memory_pool());
    check(b->Reserve((rows + abs_stride - 1) / abs_stride));
    for (auto i = reverse_offset % abs_stride; i < rows; i += abs_stride) {
      check(b->Append(rows - i - 1));
    }
    auto stride_index = finish(*b);
    if (stride_index->length() == 0) {
      return {};
    }
    auto batch = to_record_batch(input);
    auto take_result = arrow::compute::Take(batch, stride_index);
    if (not take_result.ok()) {
      diagnostic::error("{}", take_result.status().ToString())
        .note("failed to apply stride")
        .throw_();
    }
    const auto datum = take_result.MoveValueUnsafe();
    TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
    return table_slice{datum.record_batch(), input.schema()};
  }

  // Immutable (from args)
  std::optional<int64_t> begin_;
  std::optional<int64_t> end_;
  std::optional<int64_t> stride_;
  bool needs_buffering_ = false;

  // Mutable state (for snapshot)
  std::vector<table_slice> buffer_;
  int64_t offset_ = 0;
  int64_t stride_offset_ = 0;
  bool done_ = false;
};

class Plugin final : public virtual operator_parser_plugin,
                     public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "slice";
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto begin = std::optional<int64_t>{};
    auto end = std::optional<int64_t>{};
    auto stride = std::optional<int64_t>{};
    const auto parse_int
      = [&](bool must_have_prefix) -> std::optional<int64_t> {
      if (p.at_end()) {
        return {};
      }
      if (must_have_prefix) {
        if (not p.accept_char(':')) {
          diagnostic::error("expected `:`")
            .primary(p.current_span())
            .hint("syntax: slice [<begin>]:[<end>][:<stride>]")
            .docs("https://docs.tenzir.com/operators/slice")
            .throw_();
        }
      }
      if (p.at_end() or p.peek_char(':')) {
        return {};
      }
      auto data = p.parse_int();
      return data.inner;
    };
    begin = parse_int(false);
    end = parse_int(true);
    stride = parse_int(true);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice begin={} end={} stride={}", begin.value_or(0),
                  end.value_or(0), stride.value_or(1)));
    if (not result) {
      diagnostic::error("failed to parse slice operator: {}", result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice>{};
    d.named("begin", &SliceArgs::begin);
    d.named("end", &SliceArgs::end);
    d.named("stride", &SliceArgs::stride);
    return d.without_optimize();
  }
};

class ReversePlugin final : public virtual operator_parser_plugin,
                            public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "reverse";
  }

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"reverse", "https://docs.tenzir.com/"
                                             "operators/reverse"};
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator("slice ::-1");
    if (not result) {
      diagnostic::error("failed to transform `reverse` into `slice`: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice>{SliceArgs{.stride = -1}};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::slice

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::Plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::ReversePlugin)
