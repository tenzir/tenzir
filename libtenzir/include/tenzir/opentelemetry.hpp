//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/settings.hpp>

#include <utility>

namespace tenzir {

/// Owns the global OpenTelemetry tracer provider. On destruction it flushes
/// pending spans and shuts the provider down. Only meaningful when tracing was
/// actually enabled; a default-constructed guard does nothing.
class OpenTelemetryGuard {
public:
  OpenTelemetryGuard() noexcept = default;

  explicit OpenTelemetryGuard(bool active) noexcept : active_{active} {
  }

  OpenTelemetryGuard(const OpenTelemetryGuard&) = delete;
  auto operator=(const OpenTelemetryGuard&) -> OpenTelemetryGuard& = delete;

  OpenTelemetryGuard(OpenTelemetryGuard&& other) noexcept
    : active_{std::exchange(other.active_, false)} {
  }

  auto operator=(OpenTelemetryGuard&& other) noexcept -> OpenTelemetryGuard& {
    if (this != &other) {
      reset();
      active_ = std::exchange(other.active_, false);
    }
    return *this;
  }

  ~OpenTelemetryGuard();

private:
  void reset() noexcept;

  bool active_ = false;
};

/// Initializes the global OpenTelemetry tracer provider from the configuration
/// under the `tenzir.opentelemetry` key. Tracing remains a no-op unless an
/// OTLP/HTTP endpoint is configured. The returned guard flushes and shuts the
/// provider down when it goes out of scope.
[[nodiscard]] auto initialize_opentelemetry(const caf::settings& config)
  -> OpenTelemetryGuard;

} // namespace tenzir
