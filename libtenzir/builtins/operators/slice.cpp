//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <arrow/type.h>

#include <ranges>

namespace tenzir::plugins::slice {

namespace {

class slice_operator final : public crtp_operator<slice_operator> {
public:
  slice_operator() = default;

  explicit slice_operator(std::optional<int64_t> begin,
                          std::optional<int64_t> end,
                          std::optional<int64_t> stride)
    : begin_{begin}, end_{end}, stride_{stride} {
  }

  static auto positive_begin_positive_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin >= 0);
    TENZIR_ASSERT(end >= 0);
    if (end <= begin) {
      co_return;
    }
    co_yield {};
    auto offset = int64_t{0};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      co_yield subslice(slice, clamped_begin, clamped_end);
      if (offset >= end) {
        break;
      }
    }
  }

  static auto positive_begin_negative_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin >= 0);
    TENZIR_ASSERT(end <= 0);
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    auto num_buffered = int64_t{0};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      offset += rows;
      auto result = subslice(slice, clamped_begin, rows);
      if (result.rows() == 0) {
        continue;
      }
      num_buffered += static_cast<int64_t>(result.rows());
      buffer.push_back(std::move(result));
      if (num_buffered > -end) {
        auto [lhs, rhs] = split(std::move(buffer), num_buffered + end);
        buffer = std::move(rhs);
        for (auto&& slice : std::move(lhs)) {
          num_buffered -= static_cast<int64_t>(slice.rows());
          co_yield std::move(slice);
        }
      }
    }
    TENZIR_ASSERT(num_buffered <= -end);
  }

  static auto negative_begin_positive_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin <= 0);
    TENZIR_ASSERT(end >= 0);
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      auto result = subslice(slice, int64_t{0}, clamped_end);
      buffer.push_back(std::move(result));
      if (result.rows() == 0) {
        break;
      }
    }
    begin = offset + begin;
    if (begin >= offset) {
      co_return;
    }
    offset = 0;
    for (auto&& slice : buffer) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      offset += rows;
      if (clamped_begin >= static_cast<int64_t>(slice.rows())) {
        continue;
      }
      auto result = subslice(slice, clamped_begin, rows);
      if (result.rows() > 0) {
        co_yield std::move(result);
      }
    }
  }

  static auto negative_begin_negative_end(generator<table_slice> input,
                                          int64_t begin, int64_t end)
    -> generator<table_slice> {
    TENZIR_ASSERT(begin <= 0);
    TENZIR_ASSERT(end <= 0);
    if (end <= begin) {
      co_return;
    }
    co_yield {};
    auto offset = int64_t{0};
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      const auto rows = static_cast<int64_t>(slice.rows());
      if (rows == 0) {
        co_yield {};
        continue;
      }
      offset += rows;
      buffer.push_back(std::move(slice));
    }
    begin = offset + begin;
    end = offset + end;
    offset = 0;
    for (auto&& slice : buffer) {
      const auto rows = static_cast<int64_t>(slice.rows());
      const auto clamped_begin = std::clamp(begin - offset, int64_t{0}, rows);
      const auto clamped_end = std::clamp(end - offset, int64_t{0}, rows);
      offset += rows;
      if (clamped_begin >= rows) {
        continue;
      }
      auto result = subslice(slice, clamped_begin, clamped_end);
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto slice(generator<table_slice> input) const -> generator<table_slice> {
    if (not begin_ and not end_) {
      return input;
    }
    if (not begin_ or *begin_ >= 0) {
      if (end_ and *end_ >= 0) {
        return positive_begin_positive_end(std::move(input), begin_.value_or(0),
                                           *end_);
      }
      return positive_begin_negative_end(std::move(input), begin_.value_or(0),
                                         end_.value_or(0));
    }
    if (end_ and *end_ >= 0) {
      return negative_begin_positive_end(std::move(input), *begin_, *end_);
    }
    return negative_begin_negative_end(std::move(input), *begin_,
                                       end_.value_or(0));
  }

  auto positive_stride(generator<table_slice> input, int64_t stride) const
    -> generator<table_slice> {
    TENZIR_ASSERT(stride > 0);
    constexpr auto make_stride_index
      = [](const int64_t offset, const int64_t rows, const int64_t stride) {
          auto b = int64_type::make_arrow_builder(arrow_memory_pool());
          check(b->Reserve((rows + 1) / stride));
          for (auto i = offset % stride; i < rows; i += stride) {
            check(b->Append(i));
          }
          return finish(*b);
        };
    auto offset = int64_t{0};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto batch = to_record_batch(slice);
      const auto rows = batch->num_rows();
      auto stride_index = make_stride_index(offset, rows, stride);
      offset += rows;
      auto take_result = arrow::compute::Take(batch, stride_index);
      if (not take_result.ok()) {
        diagnostic::error("{}", take_result.status().ToString())
          .note("failed to apply stride")
          .throw_();
      }
      const auto datum = take_result.MoveValueUnsafe();
      TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
      co_yield table_slice{
        datum.record_batch(),
        slice.schema(),
      };
    }
  }

  auto negative_stride(generator<table_slice> input, int64_t stride) const
    -> generator<table_slice> {
    auto buffer = std::vector<table_slice>{};
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      buffer.push_back(std::move(slice));
    }
    const auto make_stride_index = [&](const int64_t offset,
                                       const int64_t rows) {
      auto stride_builder = arrow::Int64Builder{tenzir::arrow_memory_pool()};
      const auto reserve_result = stride_builder.Reserve((rows + 1) / -stride);
      TENZIR_ASSERT(reserve_result.ok(), reserve_result.ToString().c_str());
      for (auto i = offset % -stride; i < rows; i += -stride) {
        const auto append_result = stride_builder.Append(rows - i - 1);
        TENZIR_ASSERT_EXPENSIVE(append_result.ok(),
                                append_result.ToString().c_str());
      }
      auto finish_result = stride_builder.Finish();
      TENZIR_ASSERT(finish_result.ok(),
                    finish_result.status().ToString().c_str());
      return finish_result.MoveValueUnsafe();
    };
    TENZIR_ASSERT(stride < 0);
    auto offset = int64_t{0};
    for (auto&& slice : buffer | std::ranges::views::reverse) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto batch = to_record_batch(slice);
      const auto rows = batch->num_rows();
      auto stride_index = make_stride_index(offset, rows);
      offset += rows;
      auto take_result = arrow::compute::Take(batch, stride_index);
      if (not take_result.ok()) {
        diagnostic::error("{}", take_result.status().ToString())
          .note("failed to apply stride")
          .throw_();
      }
      const auto datum = take_result.MoveValueUnsafe();
      TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
      co_yield table_slice{
        datum.record_batch(),
        slice.schema(),
      };
    }
  }

  auto stride(generator<table_slice> input) const -> generator<table_slice> {
    if (stride_.value_or(1) == 1) {
      return input;
    }
    if (*stride_ > 0) {
      return positive_stride(std::move(input), *stride_);
    }
    return negative_stride(std::move(input), *stride_);
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    return stride(slice(std::move(input)));
  }

  auto name() const -> std::string override {
    return "slice";
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    const auto nop_slice = begin_.value_or(0) == 0 and not end_;
    const auto reverse = stride_.value_or(1) == -1;
    const auto nop_stride = stride_.value_or(1) == 1
                            or (order == event_order::unordered and reverse);
    if (nop_slice and nop_stride) {
      // If there's neither a begin nor an end, then this operator is a no-op.
      // We optimize it away here.
      return optimize_result{
        filter,
        order,
        nullptr,
      };
    }
    return optimize_result{
      std::nullopt,
      event_order::ordered,
      copy(),
    };
  }

  friend auto inspect(auto& f, slice_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugin.slice.slice_operator")
      .fields(f.field("begin", x.begin_), f.field("end", x.end_),
              f.field("stride", x.stride_));
  }

private:
  std::optional<int64_t> begin_ = {};
  std::optional<int64_t> end_ = {};
  std::optional<int64_t> stride_ = {};
};

// New executor implementation
struct SliceArgs {
  Option<int64_t> begin;
  Option<int64_t> end;
  Option<int64_t> stride;
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

class plugin final : public virtual operator_plugin<slice_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto begin = std::optional<int64_t>{};
    auto end = std::optional<int64_t>{};
    auto stride = std::optional<int64_t>{};
    // TODO: Range selector syntax
    TRY(argument_parser2::operator_(name())
          .named("begin", begin)
          .named("end", end)
          .named("stride", stride)
          .parse(inv, ctx));
    return std::make_unique<slice_operator>(begin, end, stride);
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice>{};
    d.named("begin", &SliceArgs::begin);
    d.named("end", &SliceArgs::end);
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
    return d.without_optimize();
  }
};

class reverse_plugin final : public virtual operator_plugin2<slice_operator>,
                             public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.reverse";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("reverse").parse(inv, ctx));
    return std::make_unique<slice_operator>(std::nullopt, std::nullopt, -1);
  }

  auto describe() const -> Description override {
    auto d = Describer<SliceArgs, Slice>{SliceArgs{
      .begin = None{},
      .end = None{},
      .stride = int64_t{-1},
    }};
    return d.without_optimize();
  }
};

enum class mode { head, tail };

template <mode Mode>
class end_plugin final : public virtual operator_plugin2<slice_operator> {
public:
  auto name() const -> std::string override {
    return Mode == mode::head ? "tql2.head" : "tql2.tail";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto n = std::optional<int64_t>{10};
    TRY(argument_parser2::operator_(name()).positional("n", n).parse(inv, ctx));
    if (Mode == mode::head) {
      return std::make_unique<slice_operator>(std::nullopt, n, std::nullopt);
    }
    return std::make_unique<slice_operator>(-n.value(), std::nullopt,
                                            std::nullopt);
  }
};

using head_plugin = end_plugin<mode::head>;
using tail_plugin = end_plugin<mode::tail>;

} // namespace

} // namespace tenzir::plugins::slice

TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::reverse_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::head_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::slice::tail_plugin)
