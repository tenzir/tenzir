//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/http_server.hpp"

#include <memory>

#include "types.hpp"

namespace tenzir::plugins::accept_otlp::detail {

auto make_error_response(uint16_t status) -> HttpResponse;
auto make_success_response(Encoding encoding) -> HttpResponse;

/// Owns an OTLP/HTTP server without blocking the owner during destruction.
class OtlpHttpServer {
public:
  explicit OtlpHttpServer(
    std::unique_ptr<http_server::ScopedServer> server) noexcept;
  ~OtlpHttpServer();

  OtlpHttpServer(OtlpHttpServer const&) = delete;
  OtlpHttpServer(OtlpHttpServer&&) = delete;
  auto operator=(OtlpHttpServer const&) -> OtlpHttpServer& = delete;
  auto operator=(OtlpHttpServer&&) -> OtlpHttpServer& = delete;

  auto drain() -> void;
  auto finish() -> void;
  auto force_stop() -> void;

private:
  Option<Box<http_server::ScopedServer>> server_;
};

auto start_http_server(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                       Arc<Semaphore> active_requests_limit, OpCtx& ctx)
  -> Task<Option<Box<OtlpHttpServer>>>;

} // namespace tenzir::plugins::accept_otlp::detail
