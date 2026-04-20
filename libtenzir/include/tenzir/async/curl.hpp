//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"

#include <folly/Executor.h>
#include <folly/executors/IOExecutor.h>

#include <cstddef>
#include <string>

namespace tenzir {

enum class CurlTransferStatus {
  finished,
  local_abort,
};

using CurlTransferResult = Result<CurlTransferStatus, std::string>;

class CurlTransfer {
public:
  ~CurlTransfer();

  CurlTransfer(CurlTransfer const&) = delete;
  auto operator=(CurlTransfer const&) -> CurlTransfer& = delete;
  CurlTransfer(CurlTransfer&&) noexcept;
  auto operator=(CurlTransfer&&) noexcept -> CurlTransfer&;

  /// Queue a chunk for libcurl to read.
  /// Returns `false` if the stream has already been closed or aborted.
  auto push(chunk_ptr chunk) -> Task<bool>;

  /// Signal end-of-stream. Subsequent reads return EOF after buffered data.
  /// Call this after the last queued chunk was pushed successfully.
  auto close() -> void;

  /// Abort the local stream and wake blocked producers or consumers.
  auto abort() -> void;

  /// Receive the next queued chunk. Returns `None` after close or abort.
  /// Check the result of `wait()` to distinguish a clean end-of-stream, a local
  /// abort, and a transport failure.
  auto next() -> Task<Option<chunk_ptr>>;

  auto wait() -> Task<CurlTransferResult>;

private:
  struct Impl;

  explicit CurlTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

/// Reusable async curl session.
///
/// Configure `easy()` directly, then start one semantic transfer at a time. The
/// transfer runs on the Folly IO executor provided to `make()`. Cancelling the
/// awaiting task or calling `cancel()` aborts the in-flight libcurl transfer
/// and propagates `folly::OperationCancelled`.
class CurlSession {
public:
  static auto make(folly::Executor::KeepAlive<folly::IOExecutor> executor)
    -> CurlSession;

  ~CurlSession();

  CurlSession(CurlSession const&) = delete;
  auto operator=(CurlSession const&) -> CurlSession& = delete;
  CurlSession(CurlSession&&) noexcept;
  auto operator=(CurlSession&&) noexcept -> CurlSession&;

  auto easy() -> curl::easy&;

  auto start_send(size_t buffer_capacity = 16) -> CurlTransfer;
  auto start_receive(size_t buffer_capacity = 16) -> CurlTransfer;

  auto busy() const -> bool;

private:
  struct Impl;

  explicit CurlSession(Box<Impl> impl);

  Box<Impl> impl_;
};

} // namespace tenzir
