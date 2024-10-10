//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>

namespace tenzir::plugins::sample_ {

TENZIR_ENUM(mode, ln, log2, log10, sqrt);

namespace {

struct operator_args {
  mode fn{};
  std::optional<located<duration>> period{
    located{std::chrono::seconds(30), location::unknown}};
  std::optional<uint64_t> min_events{30};
  std::optional<uint64_t> max_rate{};

  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x)
      .pretty_name("operator_args")
      .fields(f.field("fn", x.fn), f.field("period", x.period),
              f.field("min_events", x.min_events),
              f.field("max_rate", x.max_rate));
  }
};

class sample_operator final : public crtp_operator<sample_operator> {
public:
  sample_operator() = default;

  sample_operator(operator_args args) : args_{std::move(args)} {
  }

  auto operator()(generator<table_slice> input,
                  operator_control_plane&) const -> generator<table_slice> {
    auto last = std::chrono::steady_clock::now();
    auto count = uint64_t{};
    // Logic copied from the slice_operator::positive_stride()
    // TODO: Consider using the slice operator directly or extracting this
    // funcitonality.
    auto offset = int64_t{};
    auto stride = int64_t{1};
    const auto calc = [&]() {
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
          auto b = int64_type::make_arrow_builder(arrow::default_memory_pool());
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
          // `count` is a `uint64_t` > 1 so `logx(count)` or `sqrt(count)` can
          // never give a negative result.
          stride = count > args_.min_events.value() ? std::ceil(calc()) : 1;
        }
        last = now - (now - last) % args_.period->inner;
        offset = 0;
        count = 0;
      }
      if (slice.rows() == 0
          or (args_.max_rate and args_.max_rate.value() <= count)) {
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

  auto
  optimize(expression const&, event_order) const -> optimize_result override {
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
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto operator_name() const -> std::string override {
    return "sample";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto str = std::optional<located<std::string>>{};
    auto args = operator_args{};
    TRY(argument_parser2::operator_("sample")
          .add(args.period, "<duration>")
          .add("mode", str)
          .add("min_events", args.min_events)
          .add("max_rate", args.max_rate)
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
};

} // namespace

} // namespace tenzir::plugins::sample_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sample_::plugin)
