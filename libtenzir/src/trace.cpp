//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/trace.hpp"

#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>
#include <opentelemetry/trace/provider.h>

#include <string>

namespace tenzir {
namespace {

namespace otel_ctx = opentelemetry::context;
namespace otel_trace = opentelemetry::trace;

/// A text-map carrier that holds a single W3C `traceparent` value. We do not
/// need full header maps because Tenzir propagates the trace context as a lone
/// string field through its actor messages.
class TraceparentCarrier final : public otel_ctx::propagation::TextMapCarrier {
public:
  TraceparentCarrier() = default;

  explicit TraceparentCarrier(std::string traceparent)
    : traceparent_{std::move(traceparent)} {
  }

  auto Get(opentelemetry::nostd::string_view key) const noexcept
    -> opentelemetry::nostd::string_view override {
    if (key == "traceparent") {
      return traceparent_;
    }
    return "";
  }

  void Set(opentelemetry::nostd::string_view key,
           opentelemetry::nostd::string_view value) noexcept override {
    if (key == "traceparent") {
      traceparent_.assign(value.data(), value.size());
    }
  }

  auto traceparent() const -> const std::string& {
    return traceparent_;
  }

private:
  std::string traceparent_;
};

} // namespace

auto otel_tracer() -> opentelemetry::nostd::shared_ptr<otel_trace::Tracer> {
  return otel_trace::Provider::GetTracerProvider()->GetTracer("tenzir");
}

auto extract_trace_context(std::string_view traceparent) -> otel_ctx::Context {
  auto current = otel_ctx::RuntimeContext::GetCurrent();
  if (traceparent.empty()) {
    return current;
  }
  const auto carrier = TraceparentCarrier{std::string{traceparent}};
  auto propagator = otel_trace::propagation::HttpTraceContext{};
  return propagator.Extract(carrier, current);
}

auto inject_trace_context(const otel_ctx::Context& context) -> std::string {
  auto carrier = TraceparentCarrier{};
  auto propagator = otel_trace::propagation::HttpTraceContext{};
  propagator.Inject(carrier, context);
  return carrier.traceparent();
}

auto inject_trace_context(
  const opentelemetry::nostd::shared_ptr<otel_trace::Span>& span)
  -> std::string {
  auto context = otel_ctx::Context{};
  return inject_trace_context(otel_trace::SetSpan(context, span));
}

} // namespace tenzir
