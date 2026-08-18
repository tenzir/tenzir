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

#include <chrono>
#include <memory>

#include "types.hpp"

namespace tenzir::plugins::accept_otlp::detail {

class OtlpGrpcServer {
public:
  static auto start(AcceptOtlpArgs const& args, Arc<MessageQueue> queue,
                    Arc<Semaphore> active_requests_limit, OpCtx& ctx)
    -> Task<Option<Box<OtlpGrpcServer>>>;

  ~OtlpGrpcServer();

  OtlpGrpcServer(OtlpGrpcServer const&) = delete;
  OtlpGrpcServer(OtlpGrpcServer&&) = delete;
  auto operator=(OtlpGrpcServer const&) -> OtlpGrpcServer& = delete;
  auto operator=(OtlpGrpcServer&&) -> OtlpGrpcServer& = delete;

  auto shutdown(std::chrono::steady_clock::time_point deadline) -> void;

private:
  struct Impl;

  explicit OtlpGrpcServer(Box<Impl> impl);

  Box<Impl> impl_;
};

} // namespace tenzir::plugins::accept_otlp::detail
