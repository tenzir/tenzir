//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::sample_ {

TENZIR_ENUM(mode, ln, log2, log10, sqrt);

namespace {

constexpr auto default_period = std::chrono::seconds(30);
constexpr uint64_t default_min_events = 30;

struct operator_args {
  mode fn{};
  Option<located<duration>> period{
    located{std::chrono::seconds(30), location::unknown}};
  Option<uint64_t> min_events{30};
  Option<uint64_t> max_rate{};
  Option<uint64_t> max_samples{};

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("fn", x.fn), f.field("period", x.period),
              f.field("min_events", x.min_events),
              f.field("max_rate", x.max_rate),
              f.field("max_samples", x.max_samples));
  }
};

struct SampleArgs {
  Option<std::string> mode_str;
  duration period = default_period;
  Option<uint64_t> min_events;
  Option<uint64_t> max_rate;
  Option<uint64_t> max_samples;
};

class Sample final : public Operator<table_slice, table_slice> {
public:
  explicit Sample(SampleArgs args)
    : args_{std::move(args)}, last_{std::chrono::steady_clock::now()} {
    if (args_.mode_str) {
      auto parsed = from_string<mode>(*args_.mode_str);
      TENZIR_ASSERT(parsed);
      fn_ = *parsed;
    }
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    const auto min_events = args_.min_events.unwrap_or(default_min_events);
    if (auto now = std::chrono::steady_clock::now();
        now - last_ > args_.period) {
      if (count_ > 1 and count_ > min_events) {
        const auto rate
          = detail::narrow_cast<uint64_t>(std::ceil(compute_rate()));
        stride_ = std::max(args_.max_rate.unwrap_or(rate), rate);
      } else {
        stride_ = 1;
      }
      last_ = now - (now - last_) % args_.period;
      offset_ = 0;
      count_ = 0;
    }
    if (args_.max_samples and *args_.max_samples <= count_) {
      co_return;
    }
    count_ += input.rows();
    auto batch = to_record_batch(input);
    const auto rows = batch->num_rows();
    auto stride_index = make_stride_index(offset_, rows, stride_);
    offset_ += rows;
    const auto datum = check(arrow::compute::Take(batch, stride_index));
    TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
    co_await push(table_slice{datum.record_batch(), input.schema()});
  }

private:
  auto compute_rate() const -> double {
    switch (fn_) {
      case mode::ln:
        return std::log(count_);
      case mode::log2:
        return std::log2(count_);
      case mode::log10:
        return std::log10(count_);
      case mode::sqrt:
        return std::sqrt(count_);
    }
    TENZIR_UNREACHABLE();
  }

  static auto make_stride_index(int64_t offset, int64_t rows, int64_t stride)
    -> std::shared_ptr<arrow::Int64Array> {
    TENZIR_ASSERT(stride > 0);
    auto b = int64_type::make_arrow_builder(arrow_memory_pool());
    check(b->Reserve((rows + 1) / stride));
    for (auto i = offset % stride; i < rows; i += stride) {
      check(b->Append(i));
    }
    return finish(*b);
  }

  SampleArgs args_;
  mode fn_ = mode::ln;

  std::chrono::steady_clock::time_point last_;
  uint64_t count_ = 0;
  int64_t offset_ = 0;
  int64_t stride_ = 1;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "sample";
  }

  auto describe() const -> Description override {
    auto d = Describer<SampleArgs, Sample>{};
    auto period_arg = d.optional_positional("period", &SampleArgs::period);
    auto mode_arg = d.named("mode", &SampleArgs::mode_str);
    d.named("min_events", &SampleArgs::min_events);
    d.named("max_rate", &SampleArgs::max_rate);
    d.named("max_samples", &SampleArgs::max_samples);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      if (auto period = ctx.get(period_arg)) {
        if (*period <= duration::zero()) {
          diagnostic::error("`period` must be a positive duration")
            .primary(ctx.get_location(period_arg).value())
            .emit(ctx);
        }
      }
      if (auto mode_value = ctx.get(mode_arg)) {
        if (not from_string<mode>(*mode_value)) {
          diagnostic::error("unsupported `mode`: {}", *mode_value)
            .hint(
              R"(`mode` must be one of `"ln"`, `"log2"`, `"log10"` or `"sqrt"`)")
            .primary(ctx.get_location(mode_arg).value())
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::sample_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sample_::plugin)
