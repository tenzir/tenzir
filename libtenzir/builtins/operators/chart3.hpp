//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/operator_plugin.hpp"

#include <string>

namespace tenzir::plugins::chart {

auto to_double(data d) -> data;

auto jsonify_limit(const data& d) -> std::string;

TENZIR_ENUM(chart_type, area, bar, line, pie);

auto describe_chart_area() -> Description;
auto describe_chart_bar() -> Description;
auto describe_chart_line() -> Description;
auto describe_chart_pie() -> Description;

} // namespace tenzir::plugins::chart
