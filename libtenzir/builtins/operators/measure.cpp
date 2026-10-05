//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/array_builder.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/type_definition.hpp"
#include "tenzir/nova/type_id.hpp"
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

namespace {

struct MeasureArgs {
  bool cumulative = false;
  bool by_schema = true;
  bool definition = false;
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
    if (args_.definition) {
      metric.field("schema",
                   is_new ? data{input.schema().to_definition()} : data{});
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
    if (not args_.by_schema) {
      const auto events = input.mask.true_count();
      if (events == 0) {
        co_return;
      }
      auto& total = events_[""];
      total = args_.cumulative ? total + events : events;
      auto builder = nova::ArrayBuilder<nova::Record>{};
      auto metric = builder.record();
      metric.field("timestamp").data(time::clock::now());
      metric.field("events").data(total);
      auto result = builder.finish();
      auto const rows = result.length();
      auto mask = nova::storage::BitMap{rows, true};
      co_await push(nova::Events{
        std::move(result), std::move(mask),
        nova::Events::Meta::make_empty(rows, "tenzir.measure.events")});
      co_return;
    }
    // A batch can hold events of multiple schemas, so we group its active
    // rows by their schema identifier and report one metric per group. Schemas
    // are identified by their structure, so events that share a structure but
    // not their name form one group.
    const auto ids
      = nova::type_id(nova::Array<nova::Data>{input.data}, input.mask);
    struct Group {
      std::string id;
      nova::storage::Index first_row;
      uint64_t events;
    };
    auto groups = std::vector<Group>{};
    auto group_index = std::unordered_map<std::string, size_t>{};
    for (auto row : nova::storage::true_bits(input.mask)) {
      auto id = std::string{*ids.get(row)};
      auto it = group_index.find(id);
      if (it == group_index.end()) {
        groups.emplace_back(id, row, 0);
        it = group_index.emplace(std::move(id), groups.size() - 1).first;
      }
      groups[it->second].events += 1;
    }
    if (groups.empty()) {
      co_return;
    }
    auto builder = nova::ArrayBuilder<nova::Record>{};
    const auto now = time::clock::now();
    for (const auto& [id, first_row, events] : groups) {
      auto& total = events_[id];
      const auto is_new = total == 0;
      total = args_.cumulative ? total + events : events;
      auto metric = builder.record();
      metric.field("timestamp").data(now);
      metric.field("events").data(total);
      metric.field("schema_id").data(id);
      const auto name = *input.meta.name.get(first_row);
      if (not args_.definition) {
        metric.field("schema").data(name);
        continue;
      }
      // A schema never changes, so we only send its definition once.
      if (not is_new) {
        metric.field("schema").null();
        continue;
      }
      const auto row = input.data.get(first_row);
      const auto internal = *input.meta.internal.get(first_row);
      nova::append_data(metric.field("schema"),
                        nova::type_definition(row, name, internal));
    }
    auto result = builder.finish();
    auto const rows = result.length();
    auto mask = nova::storage::BitMap{rows, true};
    co_await push(nova::Events{
      std::move(result), std::move(mask),
      nova::Events::Meta::make_empty(rows, "tenzir.measure.events")});
  }

  auto snapshot(Serde& serde) -> void override {
    serde("events", events_);
  }

private:
  MeasureArgs args_;
  /// The number of events per schema identifier.
  std::unordered_map<std::string, uint64_t> events_;
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
    d.named("by_schema", &MeasureArgs::by_schema);
    d.named("_exact_definition", &MeasureArgs::definition);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::measure

TENZIR_REGISTER_PLUGIN(tenzir::plugins::measure::plugin)
