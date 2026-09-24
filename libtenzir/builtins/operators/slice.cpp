//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>

#include <limits>
#include <ranges>

namespace tenzir::plugins::slice {

namespace {

// New executor implementation
struct SliceArgs {
  Option<int64_t> begin;
  Option<int64_t> end;
  Option<int64_t> stride;
  location operator_location;
};

class Slice final : public Operator<table_slice, table_slice> {
public:
  explicit Slice(SliceArgs args)
    : begin_{std::move(args.begin)},
      end_{std::move(args.end)},
      stride_{std::move(args.stride)} {
    // Buffering is required when any index is negative or stride is negative
    needs_buffering_ = (stride_ and *stride_ < 0) or (begin_ and *begin_ < 0)
                       or (end_ and *end_ < 0);
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

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (not needs_buffering_) {
      co_return FinalizeBehavior::done;
    }
    // Resolve negative indices using total row count (offset_)
    auto begin = begin_.unwrap_or(0);
    auto end = end_.unwrap_or(offset_);
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
      co_return FinalizeBehavior::done;
    }
    // Apply slice to buffer
    auto stride = stride_.unwrap_or(1);
    if (stride > 0) {
      co_await finalize_positive_stride(push, begin, end, stride);
    } else {
      co_await finalize_negative_stride(push, begin, end, stride);
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    if (not needs_buffering_ and done_) {
      return OperatorState::done;
    }
    return OperatorState::normal;
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
    auto begin = begin_.unwrap_or(0);
    auto end = end_.unwrap_or(std::numeric_limits<int64_t>::max());
    auto stride = stride_.unwrap_or(1);
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
    TENZIR_ASSERT(take_result.ok(), take_result.status().ToString().c_str());
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
    TENZIR_ASSERT(take_result.ok(), take_result.status().ToString().c_str());
    const auto datum = take_result.MoveValueUnsafe();
    TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
    return table_slice{datum.record_batch(), input.schema()};
  }

  // Immutable (from args)
  Option<int64_t> begin_;
  Option<int64_t> end_;
  Option<int64_t> stride_;
  bool needs_buffering_ = false;

  // Mutable state (for snapshot)
  std::vector<table_slice> buffer_;
  int64_t offset_ = 0;
  int64_t stride_offset_ = 0;
  bool done_ = false;
};

class SliceNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit SliceNova(SliceArgs args)
    : begin_{args.begin},
      end_{args.end},
      stride_{args.stride},
      needs_buffering_{(stride_ and *stride_ < 0) or (begin_ and *begin_ < 0)
                       or (end_ and *end_ < 0)} {
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx&)
    -> Task<void> override {
    if (needs_buffering_) {
      total_ += input.active_count();
      buffer_.push_back(std::move(input));
      co_return;
    }
    auto mask = select_forward(input.mask, begin_.unwrap_or(0),
                               end_.unwrap_or(max_index()),
                               stride_.unwrap_or(1), offset_);
    offset_ += input.active_count();
    if (end_ and offset_ >= *end_) {
      done_ = true;
    }
    if (mask.any()) {
      input.mask = std::move(mask);
      co_await push(std::move(input));
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    if (not needs_buffering_) {
      co_return FinalizeBehavior::done;
    }
    auto begin = begin_.unwrap_or(0);
    auto end = end_.unwrap_or(total_);
    if (begin < 0) {
      begin += total_;
    }
    if (end < 0) {
      end += total_;
    }
    begin = std::max(begin, int64_t{0});
    end = std::clamp(end, int64_t{0}, total_);
    if (end <= begin) {
      co_return FinalizeBehavior::done;
    }
    auto const stride = stride_.unwrap_or(1);
    if (stride > 0) {
      auto position = int64_t{0};
      for (auto& input : buffer_) {
        auto mask = select_forward(input.mask, begin, end, stride, position);
        position += input.active_count();
        if (mask.any()) {
          input.mask = std::move(mask);
          co_await push(std::move(input));
        }
      }
    } else {
      auto const magnitude = uint64_t{0} - static_cast<uint64_t>(stride);
      co_await push_reversed(push, begin, end, magnitude);
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return not needs_buffering_ and done_ ? OperatorState::done
                                          : OperatorState::normal;
  }

  auto snapshot(Serde& serde) -> void override {
    if (needs_buffering_) {
      // Buffered Nova events are not serializable yet.
      TENZIR_TODO();
    }
    serde("offset", offset_);
    serde("done", done_);
  }

private:
  static auto max_index() -> int64_t {
    return std::numeric_limits<int64_t>::max();
  }

  static auto select_forward(nova::storage::BitMap const& input, int64_t begin,
                             int64_t end, int64_t stride, int64_t offset)
    -> nova::storage::BitMap {
    auto result = nova::storage::BitMap::Mutable{input.length()};
    auto position = offset;
    for (auto row : nova::storage::bitmap_iteration(input)) {
      if (not row) {
        continue;
      }
      if (position >= begin and position < end
          and (position - begin) % stride == 0) {
        result.set(*row, true);
      }
      ++position;
    }
    return std::move(result).finish();
  }

  auto push_reversed(Push<nova::Events>& push, int64_t begin, int64_t end,
                     uint64_t stride) -> Task<void> {
    auto builder = EventsBuilder{};
    auto position = total_;
    for (auto& input : buffer_ | std::ranges::views::reverse) {
      for (auto row = input.length(); row-- > 0;) {
        if (not input.mask.get(row)) {
          continue;
        }
        --position;
        if (position < begin or position >= end
            or static_cast<uint64_t>(end - 1 - position) % stride != 0) {
          continue;
        }
        builder.append(input, row);
        if (builder.length()
            == static_cast<nova::storage::Index>(
              defaults::import::table_slice_size)) {
          co_await push(std::exchange(builder, EventsBuilder{}).finish());
        }
      }
    }
    if (builder.length() > 0) {
      co_await push(std::move(builder).finish());
    }
  }

  struct EventsBuilder {
    auto append(nova::Events const& input, nova::storage::Index row) -> void {
      auto output = data.record();
      for (auto [name, value] : input.data.get(row)) {
        nova::append_row(output.field(name), value);
      }
      names.data(*input.meta.name.get(row));
      import_times.data(*input.meta.import_time.get(row));
      internal.data(*input.meta.internal.get(row));
    }

    auto length() const -> nova::storage::Index {
      return data.length();
    }

    auto finish() && -> nova::Events {
      auto result = data.finish();
      auto mask = nova::storage::BitMap{result.length(), true};
      return {
        std::move(result),
        std::move(mask),
        {
          .name = names.finish(),
          .import_time = import_times.finish(),
          .internal = internal.finish(),
        },
      };
    }

    nova::ArrayBuilder<nova::Record> data;
    nova::ArrayBuilder<nova::String> names;
    nova::ArrayBuilder<nova::Time> import_times;
    nova::ArrayBuilder<nova::Bool> internal;
  };

  Option<int64_t> begin_;
  Option<int64_t> end_;
  Option<int64_t> stride_;
  bool needs_buffering_ = false;
  std::vector<nova::Events> buffer_;
  int64_t offset_ = 0;
  int64_t total_ = 0;
  bool done_ = false;
};

/// Returns how many leading input events `slice` needs to produce its output,
/// or its first `output_limit` output events if given. Returns `None` if the
/// output depends on events beyond a fixed prefix: a negative index resolves
/// against the total event count, and a negative stride starts from the end.
auto slice_input_bound(Option<int64_t> begin, Option<int64_t> end,
                       Option<int64_t> stride, Option<uint64_t> output_limit)
  -> Option<uint64_t> {
  auto b = begin.unwrap_or(0);
  auto s = stride.unwrap_or(1);
  if (b < 0 or s <= 0 or (end and *end < 0)) {
    return None{};
  }
  auto result = Option<uint64_t>{};
  if (end) {
    result = static_cast<uint64_t>(*end);
  }
  if (output_limit) {
    // The first N outputs are the inputs at b, b + s, ..., b + (N - 1) * s.
    auto needed = Option<uint64_t>{uint64_t{0}};
    if (*output_limit > 0) {
      auto steps = *output_limit - 1;
      auto su = static_cast<uint64_t>(s);
      auto max = std::numeric_limits<uint64_t>::max();
      if (steps > (max - static_cast<uint64_t>(b) - 1) / su) {
        needed = None{};
      } else {
        needed = static_cast<uint64_t>(b) + steps * su + 1;
      }
    }
    if (needed) {
      result = result ? std::min(*result, *needed) : *needed;
    }
  }
  return result;
}

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "slice";
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice, SliceNova>{};
    d.operator_location(&SliceArgs::operator_location);
    auto begin = d.named("begin", &SliceArgs::begin);
    auto end = d.named("end", &SliceArgs::end);
    auto stride = d.named("stride", &SliceArgs::stride);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      TRY(auto value, ctx.get(stride));
      if (value == 0) {
        diagnostic::error("stride must not be zero")
          .primary(ctx.get_location(stride).value())
          .emit(ctx);
      }
      return {};
    });
    return d.optimize([=](DescribeCtx& ctx,
                          ir::OptimizeRequest req) -> Optimization {
      // `slice` selects events by position, so it needs ordered input and
      // all predicates stay behind it. The projection passes through,
      // because `slice` reads no fields. With non-negative indexes and a
      // positive stride, the output depends only on a prefix of the input,
      // which bounds the events that upstream must produce. A downstream
      // limit tightens that bound only if no predicate runs in between.
      // An argument that is present but cannot be evaluated yields no hint.
      auto known = [&](Argument<SliceArgs, int64_t> arg) {
        return ctx.get(arg) or not ctx.get_location(arg);
      };
      auto limit = Option<uint64_t>{};
      if (known(begin) and known(end) and known(stride)) {
        auto output_limit = req.filter.empty() ? req.limit : Option<uint64_t>{};
        limit = slice_input_bound(ctx.get(begin), ctx.get(end), ctx.get(stride),
                                  output_limit);
      }
      return {
        .order = EventOrder::ordered,
        .filter_self = std::move(req.filter),
        .limit_upstream = limit,
        .projection_upstream = std::move(req.projection),
      };
    });
  }
};

class reverse_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "reverse";
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice, SliceNova>{SliceArgs{
      .begin = None{},
      .end = None{},
      .stride = int64_t{-1},
      .operator_location = {},
    }};
    d.operator_location(&SliceArgs::operator_location);
    return d.optimize(
      [](DescribeCtx&, ir::OptimizeRequest req) -> Optimization {
        // Reversing permutes events without changing them, so every
        // predicate commutes with it and the projection passes through.
        // Without an ordering requirement, `reverse` is a no-op that we drop
        // along with forwarding the limit. Otherwise, a limit on the reversed
        // output selects a suffix of the input and must stay behind.
        auto unordered = req.order == EventOrder::unordered;
        return {
          .order = req.order,
          .filter_upstream = std::move(req.filter),
          .drop = unordered,
          .limit_upstream = unordered ? req.limit : Option<uint64_t>{},
          .projection_upstream = std::move(req.projection),
        };
      });
  }
};

} // namespace

} // namespace tenzir::plugins::slice

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::reverse_plugin)
