//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::measure {

namespace {

class measure_operator final : public crtp_operator<measure_operator> {
public:
  measure_operator() = default;

  measure_operator(uint64_t batch_size, bool real_time, bool cumulative,
                   bool definition)
    : batch_size_{detail::narrow<int64_t>(batch_size)},
      real_time_{real_time},
      cumulative_{cumulative},
      definition_{definition} {
  }

  auto operator()(generator<table_slice> input) const
    -> generator<table_slice> {
    auto last_finish = std::chrono::steady_clock::now();
    auto builder = series_builder{};
    auto counters = std::unordered_map<type, uint64_t>{};
    for (auto&& slice : input) {
      const auto now = std::chrono::steady_clock::now();
      if (slice.rows() == 0) {
        if (builder.length() > 0
            and last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish_assert_one_slice("tenzir.measure.events");
          continue;
        }
        co_yield {};
        continue;
      }
      auto& events = counters[slice.schema()];
      const auto is_new = events == 0;
      events = cumulative_ ? events + slice.rows() : slice.rows();
      auto metric = builder.record();
      metric.field("timestamp", time::clock::now());
      metric.field("events", events);
      metric.field("schema_id", slice.schema().make_fingerprint());
      if (definition_) {
        metric.field("schema",
                     is_new ? data{slice.schema().to_definition()} : data{});
      } else {
        metric.field("schema", slice.schema().name());
      }
      if (real_time_ or builder.length() == batch_size_
          or last_finish + defaults::import::batch_timeout < now) {
        last_finish = now;
        co_yield builder.finish_assert_one_slice("tenzir.measure.events");
        continue;
      }
      co_yield {};
    }
    if (builder.length() > 0) {
      co_yield builder.finish_assert_one_slice("tenzir.measure.events");
    }
  }

  auto operator()(generator<chunk_ptr> input) const -> generator<table_slice> {
    auto last_finish = std::chrono::steady_clock::now();
    static const auto schema = type{
      "tenzir.measure.bytes",
      record_type{
        {"timestamp", time_type{}},
        {"bytes", uint64_type{}},
      },
    };
    auto builder = series_builder{schema};
    auto counter = uint64_t{};
    for (auto&& chunk : input) {
      const auto now = std::chrono::steady_clock::now();
      if (!chunk || chunk->size() == 0) {
        if (builder.length() > 0
            and last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish_assert_one_slice();
          continue;
        }
        co_yield {};
        continue;
      }
      counter = cumulative_ ? counter + chunk->size() : chunk->size();
      auto metric = builder.record();
      metric.field("timestamp", time::clock::now());
      metric.field("bytes", counter);
      if (real_time_ or builder.length() == batch_size_
          or last_finish + defaults::import::batch_timeout < now) {
        last_finish = now;
        co_yield builder.finish_assert_one_slice();
        continue;
      }
      co_yield {};
    }
    if (builder.length() > 0) {
      co_yield builder.finish_assert_one_slice();
    }
  }

  auto name() const -> std::string override {
    return "measure";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    // Note: This can change the output of `measure`.
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, measure_operator& x) -> bool {
    return f.object(x).fields(f.field("batch_size", x.batch_size_),
                              f.field("real_time", x.real_time_),
                              f.field("cumulative", x.cumulative_),
                              f.field("definition", x.definition_));
  }

private:
  int64_t batch_size_ = {};
  bool real_time_ = {};
  bool cumulative_ = {};
  bool definition_ = {};
};

class plugin final : public virtual operator_plugin<measure_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    bool real_time = false;
    bool cumulative = false;
    auto parser = argument_parser{"measure", "https://docs.tenzir.com/"
                                             "operators/measure"};
    parser.add("--real-time", real_time);
    parser.add("--cumulative", cumulative);
    parser.parse(p);
    return std::make_unique<measure_operator>(batch_size_, real_time,
                                              cumulative, false);
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    bool real_time = false;
    bool cumulative = false;
    bool definition = false;
    argument_parser2::operator_("measure")
      .named("real_time", real_time)
      .named("cumulative", cumulative)
      .named("_definition", definition)
      .parse(inv, ctx)
      .ignore();
    return std::make_unique<measure_operator>(batch_size_, real_time,
                                              cumulative, definition);
  }

private:
  uint64_t batch_size_ = {};
};

} // namespace

} // namespace tenzir::plugins::measure

TENZIR_REGISTER_PLUGIN(tenzir::plugins::measure::plugin)
