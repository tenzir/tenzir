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

auto start_http_server(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                       Arc<Semaphore> active_requests_limit, OpCtx& ctx)
  -> Task<Option<Box<http_server::Server>>>;

} // namespace tenzir::plugins::accept_otlp::detail
