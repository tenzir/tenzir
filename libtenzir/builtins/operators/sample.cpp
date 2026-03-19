//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
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
  std::optional<located<duration>> period{
    located{std::chrono::seconds(30), location::unknown}};
  std::optional<uint64_t> min_events{30};
  std::optional<uint64_t> max_rate{};
  std::optional<uint64_t> max_samples{};

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
  std::optional<std::string> mode_str;
  duration period = default_period;
  std::optional<uint64_t> min_events;
  std::optional<uint64_t> max_rate;
  std::optional<uint64_t> max_samples;
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
    const auto min_events = args_.min_events.value_or(default_min_events);
    if (auto now = std::chrono::steady_clock::now();
        now - last_ > args_.period) {
      if (count_ > 1 && count_ > min_events) {
        const auto rate
          = detail::narrow_cast<uint64_t>(std::ceil(compute_rate()));
        stride_ = std::max(args_.max_rate.value_or(rate), rate);
      } else {
        stride_ = 1;
      }
      last_ = now - (now - last_) % args_.period;
      offset_ = 0;
      count_ = 0;
    }
    if (input.rows() == 0
        || (args_.max_samples && *args_.max_samples <= count_)) {
      co_return;
    }
    count_ += input.rows();
    auto batch = to_record_batch(input);
    const auto rows = batch->num_rows();
    auto stride_index = make_stride_index(offset_, rows, stride_);
    offset_ += rows;
    const auto datum = check(arrow::compute::Take(batch, stride_index));
    TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
    (co_await push(table_slice{datum.record_batch(), input.schema()})).ignore();
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

class sample_operator final : public crtp_operator<sample_operator> {
public:
  sample_operator() = default;

  sample_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<table_slice> input, operator_control_plane&) const
    -> generator<table_slice> {
    auto last = std::chrono::steady_clock::now();
    auto count = uint64_t{};
    // Logic copied from the slice_operator::positive_stride()
    // TODO: Consider using the slice operator directly or extracting this
    // funcitonality.
    auto offset = int64_t{};
    auto stride = int64_t{1};
    const auto compute_rate = [&]() {
      switch (args_.fn) {
        case mode::ln:
          return std::log(count);
        case mode::log2:
          return std::log2(count);
        case mode::log10:
          return std::log10(count);
        case mode::sqrt:
          return std::sqrt(count);
      }
      TENZIR_UNREACHABLE();
    };
    constexpr auto make_stride_index
      = [](const int64_t offset, const int64_t rows, const int64_t stride) {
          TENZIR_ASSERT(stride > 0);
          auto b = int64_type::make_arrow_builder(arrow_memory_pool());
          check(b->Reserve((rows + 1) / stride));
          for (auto i = offset % stride; i < rows; i += stride) {
            check(b->Append(i));
          }
          return finish(*b);
        };
    for (const auto& slice : input) {
      if (auto now = std::chrono::steady_clock::now();
          now - last > args_.period->inner) {
        if (count > 1) {
          if (count > args_.min_events.value()) {
            // `count` is a `uint64_t` > 1 so `logx(count)` or `sqrt(count)` can
            // never give a negative result.
            const auto rate
              = detail::narrow_cast<uint64_t>(std::ceil(compute_rate()));
            stride = std::max(args_.max_rate.value_or(rate), rate);
          } else {
            stride = 1;
          }
        }
        last = now - (now - last) % args_.period->inner;
        offset = 0;
        count = 0;
      }
      if (slice.rows() == 0
          or (args_.max_samples and args_.max_samples.value() <= count)) {
        co_yield {};
        continue;
      }
      count += slice.rows();
      auto batch = to_record_batch(slice);
      const auto rows = batch->num_rows();
      auto stride_index = make_stride_index(offset, rows, stride);
      offset += rows;
      const auto datum = check(arrow::compute::Take(batch, stride_index));
      TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
      co_yield table_slice{
        datum.record_batch(),
        slice.schema(),
      };
    }
  }

  auto name() const -> std::string override {
    return "tql2.sample";
  }

  auto location() const -> operator_location override {
    return operator_location::anywhere;
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    // TODO: Consider adding an option that just subslices instead of sampling
    // while respecting the input order. I.e., instead of taking every nth
    // element we could also take all the elements from the front of every batch
    // per batch.
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sample_operator& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.sample.sample_operator")
      .fields(f.field("args", x.args_));
  }

private:
  operator_args args_{};
};

class plugin final : public virtual operator_plugin<sample_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto operator_name() const -> std::string override {
    return "sample";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto str = std::optional<located<std::string>>{};
    auto args = operator_args{};
    TRY(argument_parser2::operator_("sample")
          .positional("period", args.period)
          .named("mode", str)
          .named("min_events", args.min_events)
          .named("max_rate", args.max_rate)
          .named("max_samples", args.max_samples)
          .parse(inv, ctx));
    if (args.period->inner <= duration::zero()) {
      diagnostic::error("`period` must be a positive duration")
        .primary(args.period.value())
        .emit(ctx);
      return failure::promise();
    }
    if (not str) {
      args.fn = mode::ln;
    } else {
      auto mode_ = from_string<mode>(str->inner);
      if (not mode_) {
        diagnostic::error("unsupported `mode`: {}", str->inner)
          .hint(
            R"(`mode` must be one of `"ln"`, `"log2"`, `"log10"` or `"sqrt"`)")
          .primary(str.value())
          .emit(ctx);
        return failure::promise();
      }
      args.fn = mode_.value();
    }
    return std::make_unique<sample_operator>(std::move(args));
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto str = std::optional<located<std::string>>{};
    auto args = operator_args{};
    auto parser
      = argument_parser{"sample", "https://docs.tenzir.com/operators/sample"};
    parser.add("--period", args.period, "<period>");
    parser.add("--mode", str, "<string>");
    parser.add("--min-events", args.min_events, "<uint>");
    parser.add("--max-rate", args.max_rate, "<uint>");
    parser.add("--max-samples", args.max_samples, "<uint>");
    parser.parse(p);
    if (args.period->inner <= duration::zero()) {
      diagnostic::error("`period` must be a positive duration")
        .primary(args.period.value())
        .throw_();
    }
    if (not str) {
      args.fn = mode::ln;
    } else {
      auto mode_ = from_string<mode>(str->inner);
      if (not mode_) {
        diagnostic::error("unsupported `mode`: {}", str->inner)
          .hint(
            R"(`mode` must be one of `"ln"`, `"log2"`, `"log10"` or `"sqrt"`)")
          .primary(str.value())
          .throw_();
      }
      args.fn = mode_.value();
    }
    return std::make_unique<sample_operator>(std::move(args));
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
