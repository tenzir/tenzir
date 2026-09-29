//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/type.hpp"

#include <vector>

namespace tenzir::detail {

/// Converts raw `tenzir.metrics.*` events into the canonical metric/value shape
/// consumed by Prometheus printers.
class prometheus_metric_shaper {
public:
  explicit prometheus_metric_shaper(type schema);

  auto shape(table_slice const& input) const -> std::vector<table_slice>;
  auto shape(nova::Events const& input) const -> std::vector<nova::Events>;

private:
  type schema_;
};

auto shape_metrics_for_prometheus(table_slice const& input)
  -> std::vector<table_slice>;

} // namespace tenzir::detail
