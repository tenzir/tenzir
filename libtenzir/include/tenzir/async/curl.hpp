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

struct CurlStreamOptions {
  size_t send_buffer_capacity = 16;
  size_t receive_buffer_capacity = 16;
};

enum class CurlCompletionKind {
  finished,
  local_abort,
};

struct CurlCompletion {
  CurlCompletionKind kind = CurlCompletionKind::finished;
};

struct CurlError {
  std::string message;
};

using CurlResult = Result<CurlCompletion, CurlError>;

class CurlPerformTransfer {
public:
  ~CurlPerformTransfer();

  CurlPerformTransfer(CurlPerformTransfer const&) = delete;
  auto operator=(CurlPerformTransfer const&) -> CurlPerformTransfer& = delete;
  CurlPerformTransfer(CurlPerformTransfer&&) noexcept;
  auto operator=(CurlPerformTransfer&&) noexcept -> CurlPerformTransfer&;

  auto wait() -> Task<CurlResult>;
  auto cancel() -> void;

private:
  struct Impl;

  explicit CurlPerformTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

class CurlSendTransfer {
public:
  ~CurlSendTransfer();

  CurlSendTransfer(CurlSendTransfer const&) = delete;
  auto operator=(CurlSendTransfer const&) -> CurlSendTransfer& = delete;
  CurlSendTransfer(CurlSendTransfer&&) noexcept;
  auto operator=(CurlSendTransfer&&) noexcept -> CurlSendTransfer&;

  /// Queue a chunk for libcurl to read.
  /// Returns `false` if the stream has already been closed or aborted.
  auto push(chunk_ptr chunk) -> Task<bool>;

  /// Signal end-of-stream. Subsequent reads return EOF after buffered data.
  /// Call this after the last queued chunk was pushed successfully.
  auto close() -> void;

  /// Abort the stream because the local producer failed or was cancelled.
  /// Subsequent reads fail with `CURLE_ABORTED_BY_CALLBACK`.
  auto abort() -> void;

  auto wait() -> Task<CurlResult>;
  auto cancel() -> void;

private:
  struct Impl;

  explicit CurlSendTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

class CurlReceiveTransfer {
public:
  ~CurlReceiveTransfer();

  CurlReceiveTransfer(CurlReceiveTransfer const&) = delete;
  auto operator=(CurlReceiveTransfer const&) -> CurlReceiveTransfer& = delete;
  CurlReceiveTransfer(CurlReceiveTransfer&&) noexcept;
  auto operator=(CurlReceiveTransfer&&) noexcept -> CurlReceiveTransfer&;

  /// Receive the next queued chunk. Returns `None` after close or abort.
  /// Check the result of `wait()` to distinguish a clean end-of-stream, a local
  /// abort, and a transport failure.
  auto next() -> Task<Option<chunk_ptr>>;

  /// Abort the stream and wake a blocked consumer.
  auto abort() -> void;

  auto wait() -> Task<CurlResult>;
  auto cancel() -> void;

private:
  struct Impl;

  explicit CurlReceiveTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

class CurlDuplexTransfer {
public:
  ~CurlDuplexTransfer();

  CurlDuplexTransfer(CurlDuplexTransfer const&) = delete;
  auto operator=(CurlDuplexTransfer const&) -> CurlDuplexTransfer& = delete;
  CurlDuplexTransfer(CurlDuplexTransfer&&) noexcept;
  auto operator=(CurlDuplexTransfer&&) noexcept -> CurlDuplexTransfer&;

  auto push(chunk_ptr chunk) -> Task<bool>;
  auto close_send() -> void;
  auto abort_send() -> void;

  auto next() -> Task<Option<chunk_ptr>>;
  auto abort_receive() -> void;

  auto wait() -> Task<CurlResult>;
  auto cancel() -> void;

private:
  struct Impl;

  explicit CurlDuplexTransfer(Box<Impl> impl);

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

  auto start_perform() -> CurlPerformTransfer;
  auto start_send(CurlStreamOptions options = {}) -> CurlSendTransfer;
  auto start_receive(CurlStreamOptions options = {}) -> CurlReceiveTransfer;
  auto start_duplex(CurlStreamOptions options = {}) -> CurlDuplexTransfer;

  auto busy() const -> bool;

private:
  struct Impl;

  explicit CurlSession(Box<Impl> impl);

  Box<Impl> impl_;
};

} // namespace tenzir
