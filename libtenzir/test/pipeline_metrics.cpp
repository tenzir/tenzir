//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_metrics.hpp"

#include "tenzir/test/test.hpp"

namespace tenzir {

TEST("counter attribution separates connectors, not peers") {
  auto metrics = PipelineMetrics{};
  auto connector = std::string{"from_tcp"};
  auto first
    = metrics.make_counter(MetricsLabel{"peer_ip", "192.0.2.1"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::bytes, connector);
  connector = "changed";
  auto second
    = metrics.make_counter(MetricsLabel{"peer_ip", "192.0.2.2"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::bytes, "from_tcp");
  auto other
    = metrics.make_counter(MetricsLabel{"host", "example.org"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::bytes, "from_http");
  first.add(3);
  second.add(5);
  other.add(7);
  auto snapshot = metrics.take_snapshot();
  REQUIRE_EQUAL(snapshot.size(), 2u);
  CHECK_EQUAL(snapshot[0].connector, "from_tcp");
  CHECK_EQUAL(snapshot[0].value, 8u);
  CHECK_EQUAL(snapshot[1].connector, "from_http");
  CHECK_EQUAL(snapshot[1].value, 7u);
  // Node profiling sums the same traffic dimension across all connectors.
  CHECK_EQUAL(snapshot[0].value + snapshot[1].value, 15u);
  first.add(2);
  CHECK_EQUAL(snapshot[0].value, 8u);
  CHECK_EQUAL(metrics.take_snapshot()[0].value, 10u);
}

TEST("counter attribution preserves traffic dimensions") {
  auto metrics = PipelineMetrics{};
  auto expected = uint64_t{0};
  for (auto direction : {MetricsDirection::read, MetricsDirection::write}) {
    for (auto visibility :
         {MetricsVisibility::external_, MetricsVisibility::internal_}) {
      for (auto unit : {MetricsUnit::bytes, MetricsUnit::events}) {
        metrics
          .make_counter(MetricsLabel{"operator", "connector"}, direction,
                        visibility, unit, "connector")
          .add(++expected);
      }
    }
  }
  auto snapshot = metrics.take_snapshot();
  REQUIRE_EQUAL(snapshot.size(), 8u);
  auto index = size_t{0};
  for (auto direction : {MetricsDirection::read, MetricsDirection::write}) {
    for (auto visibility :
         {MetricsVisibility::external_, MetricsVisibility::internal_}) {
      for (auto unit : {MetricsUnit::bytes, MetricsUnit::events}) {
        auto const& entry = snapshot[index++];
        CHECK_EQUAL(entry.connector, "connector");
        CHECK(entry.direction == direction);
        CHECK(entry.visibility == visibility);
        CHECK(entry.type == unit);
        CHECK(entry.instrument == MetricsInstrument::counter);
        CHECK_EQUAL(entry.value, index);
      }
    }
  }
}

TEST("unattributed counters retain legacy aggregation") {
  auto metrics = PipelineMetrics{};
  auto first
    = metrics.make_counter(MetricsLabel{"operator", "first"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::events);
  auto second
    = metrics.make_counter(MetricsLabel{"operator", "second"},
                           MetricsDirection::read, MetricsVisibility::external_,
                           MetricsUnit::events);
  first.add(2);
  second.add(3);
  auto snapshot = metrics.take_snapshot();
  REQUIRE_EQUAL(snapshot.size(), 1u);
  CHECK(snapshot[0].connector.empty());
  CHECK_EQUAL(snapshot[0].label.value(), "first");
  CHECK_EQUAL(snapshot[0].value, 5u);
  metrics
    .make_counter(MetricsLabel{"operator", "first"}, MetricsDirection::read,
                  MetricsVisibility::external_, MetricsUnit::events, "first")
    .add(7);
  snapshot = metrics.take_snapshot();
  REQUIRE_EQUAL(snapshot.size(), 2u);
  CHECK_EQUAL(snapshot[0].value, 5u);
  CHECK_EQUAL(snapshot[1].value, 7u);
  auto null_counter = MetricsCounter{};
  CHECK(not null_counter);
  null_counter.add(9);
  CHECK_EQUAL(metrics.take_snapshot()[0].value, 5u);
}

} // namespace tenzir
