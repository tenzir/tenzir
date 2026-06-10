//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <opentelemetry/context/context.h>
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>

#include <string>
#include <string_view>

namespace tenzir {

/// Returns the global "tenzir" tracer. Spans created from it are no-ops unless a
/// tracer provider was installed via `initialize_opentelemetry`.
auto otel_tracer()
  -> opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer>;

/// Parses a W3C `traceparent` header into a context that can be used as the
/// parent of a new span. Returns the current context unchanged when the string
/// is empty or malformed, so a span started from the result becomes a fresh
/// root in that case.
auto extract_trace_context(std::string_view traceparent)
  -> opentelemetry::context::Context;

/// Serializes the span contained in `context` into a W3C `traceparent` header.
auto inject_trace_context(const opentelemetry::context::Context& context)
  -> std::string;

/// Serializes `span` into a W3C `traceparent` header so it can be propagated to
/// another actor or node.
auto inject_trace_context(
  const opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span>& span)
  -> std::string;

} // namespace tenzir
