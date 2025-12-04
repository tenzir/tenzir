//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"
#include "tenzir/time.hpp"

namespace tenzir {

struct retention_policy {
  retention_policy() = default;

  static auto
  make(const record& cfg, session ctx) -> failure_or<retention_policy> {
    auto result = retention_policy{};
    auto failed = false;
    const auto try_parse = [&](auto& out, const auto key) -> void {
      if (const auto* opt_duration = get_if<duration>(&cfg, key)) {
        out.emplace(*opt_duration);
      } else if (const auto* opt_string = get_if<std::string>(&cfg, key)) {
        if (not parsers::duration(*opt_string, out.emplace())) {
          diagnostic::error("expected type `duration` for option `{}`", key)
            .hint("got `{}`", *opt_string)
            .emit(ctx);
          failed = true;
          return;
        }
      }
      if (out and *out < duration::zero()) {
        diagnostic::warning("expected positive value for option `{}`", key)
          .hint("got `{}`", *out)
          .emit(ctx);
        failed = true;
      }
    };
    try_parse(result.metrics_period, "tenzir.retention.metrics");
    try_parse(result.diagnostics_period, "tenzir.retention.diagnostics");
    try_parse(result.operator_metrics_period,
              "tenzir.retention.operator_metrics");
    if (failed) {
      return failure::promise();
    }
    return result;
  }

  static auto make(const record& cfg) -> caf::expected<retention_policy> {
    auto dh = collecting_diagnostic_handler{};
    auto sp = session_provider::make(dh);
    auto result = make(cfg, sp.as_session());
    if (not result) {
      return std::move(dh).to_error();
    }
    return std::move(*result);
  }

  auto should_be_persisted(const table_slice& slice) const -> bool {
    const auto& schema = slice.schema();
    if (not schema.attribute("internal")) {
      return true;
    }
    if (schema.name() == "tenzir.diagnostic") {
      return not diagnostics_period or *diagnostics_period > duration::zero();
    }
    if (schema.name() == "tenzir.metrics.operator") {
      return not operator_metrics_period
             or *operator_metrics_period > duration::zero();
    }
    if (schema.name().starts_with("tenzir.metrics.")) {
      return not metrics_period or *metrics_period > duration::zero();
    }
    return true;
  }

  friend auto inspect(auto& f, retention_policy& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.retention_policy")
      .fields(f.field("metrics_period", x.metrics_period),
              f.field("diagnostics_period", x.diagnostics_period),
              f.field("operator_metrics_period", x.operator_metrics_period));
  }

  std::optional<duration> metrics_period = days{16};
  std::optional<duration> diagnostics_period = days{30};
  std::optional<duration> operator_metrics_period = duration::zero();
};

} // namespace tenzir
