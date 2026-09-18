//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/type_system.hpp"

#include <tenzir/async.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>
#include <folly/coro/Sleep.h>

namespace tenzir::plugins::measure {

TENZIR_ENUM(schema, name_only, legacy, exact);

namespace {

struct MeasureArgs {
  bool cumulative = false;
  bool definition = false;
  bool exact_definition = false;
};

class MeasureTableSlice final : public Operator<table_slice, table_slice> {
public:
  explicit MeasureTableSlice(MeasureArgs args) : args_{args} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto& events = counters_[input.schema()];
    const auto is_new = events == 0;
    events = args_.cumulative ? events + input.rows() : input.rows();

    series_builder builder_;
    auto metric = builder_.record();
    metric.field("timestamp", time::clock::now());
    metric.field("events", events);
    metric.field("schema_id", input.schema().make_fingerprint());
    if (args_.exact_definition) {
      metric.field("schema",
                   is_new ? data{input.schema().to_definition()} : data{});
    } else if (args_.definition) {
      metric.field("schema", is_new
                               ? data{input.schema().to_legacy_definition()}
                               : data{});
    } else {
      metric.field("schema", input.schema().name());
    }
    co_await push(builder_.finish_assert_one_slice("tenzir.measure.events"));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("counters", counters_);
  }

private:
  MeasureArgs args_;
  std::unordered_map<type, uint64_t> counters_;
};

class MeasureChunk final : public Operator<chunk_ptr, table_slice> {
public:
  explicit MeasureChunk(MeasureArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    counter_ = args_.cumulative ? counter_ + input->size() : input->size();

    series_builder builder_;
    auto metric = builder_.record();
    metric.field("timestamp", time::clock::now());
    metric.field("bytes", counter_);

    co_await push(builder_.finish_assert_one_slice("tenzir.measure.bytes"));
  }

  auto snapshot(Serde& serde) -> void override {
    serde("counter", counter_);
  }

private:
  MeasureArgs args_;
  uint64_t counter_ = 0;
};

class MeasureEvents final : public Operator<nova::Events, nova::Events> {
public:
  explicit MeasureEvents(MeasureArgs args) : args_{args} {
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    const auto length = static_cast<uint64_t>(input.length());
    const auto active_count = static_cast<uint64_t>(input.active_count());
    events_ = args_.cumulative ? events_ + length : length;
    selected_ = args_.cumulative ? selected_ + active_count : active_count;

    auto builder = nova::ArrayBuilder<nova::Record>{};
    auto metric = builder.record();
    metric.field("timestamp").data(time::clock::now());
    metric.field("events").data(events_);
    metric.field("selected").data(selected_);
    auto result = builder.finish();
    auto const rows = result.length();
    auto mask = nova::storage::BitMap{rows, true};
    co_await push(nova::Events{std::move(result), std::move(mask),
                               nova::Events::Meta::make_empty(rows)});
  }

  auto snapshot(Serde& serde) -> void override {
    serde("events", events_);
    serde("selected", selected_);
  }

private:
  MeasureArgs args_;
  uint64_t events_ = 0;
  uint64_t selected_ = 0;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "measure";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<MeasureArgs, MeasureTableSlice, MeasureChunk, MeasureEvents>{
        MeasureArgs{}};
    d.named("cumulative", &MeasureArgs::cumulative);
    d.named("_definition", &MeasureArgs::definition);
    d.named("_exact_definition", &MeasureArgs::exact_definition);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::measure

TENZIR_REGISTER_PLUGIN(tenzir::plugins::measure::plugin)
