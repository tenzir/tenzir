//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/opentelemetry.hpp"

#include "tenzir/config.hpp"
#include "tenzir/logger.hpp"

#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/trace/provider.h>

#include <memory>
#include <string>

namespace tenzir {
namespace {

namespace otlp = opentelemetry::exporter::otlp;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace otel_resource = opentelemetry::sdk::resource;

// Keeps the active provider so that we can flush and shut it down later. The
// SDK type (rather than the API type) is retained because only it exposes
// `ForceFlush` and `Shutdown`.
std::shared_ptr<trace_sdk::TracerProvider> current_provider = {};

} // namespace

auto initialize_opentelemetry(const caf::settings& config)
  -> OpenTelemetryGuard {
  const auto endpoint
    = caf::get_or(config, "tenzir.opentelemetry.endpoint", std::string{});
  if (endpoint.empty()) {
    TENZIR_DEBUG("not initializing OpenTelemetry tracing: no endpoint "
                 "configured under tenzir.opentelemetry.endpoint");
    return OpenTelemetryGuard{false};
  }
  auto exporter_options = otlp::OtlpHttpExporterOptions{};
  exporter_options.url = endpoint;
  if (const auto* headers
      = caf::get_if<caf::settings>(&config, "tenzir.opentelemetry.headers")) {
    for (const auto& [name, value] : *headers) {
      if (auto str = caf::get_as<std::string>(value)) {
        exporter_options.http_headers.emplace(name, std::move(*str));
      } else {
        TENZIR_WARN("ignoring non-string OpenTelemetry header '{}'", name);
      }
    }
  }
  auto exporter = otlp::OtlpHttpExporterFactory::Create(exporter_options);
  auto processor = trace_sdk::BatchSpanProcessorFactory::Create(
    std::move(exporter), trace_sdk::BatchSpanProcessorOptions{});
  // Omitting the sampler selects the SDK default (parent-based, always-on),
  // which is what we want for the low-volume control plane.
  auto resource = otel_resource::Resource::Create({
    {"service.name", "tenzir"},
    {"service.version", version::version},
  });
  current_provider
    = trace_sdk::TracerProviderFactory::Create(std::move(processor), resource);
  trace_api::Provider::SetTracerProvider(
    std::shared_ptr<trace_api::TracerProvider>{current_provider});
  TENZIR_INFO("initialized OpenTelemetry tracing with OTLP/HTTP endpoint {}",
              endpoint);
  return OpenTelemetryGuard{true};
}

void OpenTelemetryGuard::reset() noexcept {
  if (not active_) {
    return;
  }
  active_ = false;
  trace_api::Provider::SetTracerProvider(
    std::shared_ptr<trace_api::TracerProvider>{});
  if (current_provider) {
    current_provider->ForceFlush();
    current_provider->Shutdown();
    current_provider.reset();
  }
}

OpenTelemetryGuard::~OpenTelemetryGuard() {
  reset();
}

} // namespace tenzir
