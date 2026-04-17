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

#include <caf/error.hpp>
#include <folly/Executor.h>
#include <folly/executors/IOExecutor.h>

#include <cstddef>
#include <functional>
#include <span>

namespace tenzir {

struct CurlBodyHandlers;

/// Single-producer, single-consumer byte stream for libcurl read callbacks.
///
/// The producer side is asynchronous and can wait for bounded capacity. The
/// consumer side is synchronous so it can be called directly from
/// `CURLOPT_READFUNCTION`. When the callback runs out of queued data, it pauses
/// the transfer instead of blocking.
class CurlBodySource {
public:
  explicit CurlBodySource(size_t capacity);
  ~CurlBodySource();

  CurlBodySource(CurlBodySource const&) = delete;
  auto operator=(CurlBodySource const&) -> CurlBodySource& = delete;
  CurlBodySource(CurlBodySource&&) = delete;
  auto operator=(CurlBodySource&&) -> CurlBodySource& = delete;

  /// Queue a chunk for libcurl to read.
  /// Returns `false` if the stream has already been closed or aborted.
  auto push(chunk_ptr chunk) -> Task<bool>;

  /// Signal end-of-stream. Subsequent reads return EOF after buffered data.
  auto close() -> void;

  /// Wait until the stream has buffered data or reached a terminal state.
  auto wait_until_ready() -> Task<void>;

  /// Abort the stream. Subsequent reads fail with `CURLE_ABORTED_BY_CALLBACK`.
  auto abort() -> void;

  /// Internal hook for `perform_curl`.
  auto read(std::span<std::byte> buffer) -> size_t;

  /// Internal hook for `perform_curl`.
  auto set_resume_callback(std::function<void()> callback) -> void;

private:
  struct Impl;
  Box<Impl> impl_;

  friend struct CurlBodyHandlers;
  friend auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor>,
                           curl::easy&, CurlBodyHandlers) -> Task<caf::error>;
};

/// Single-producer, single-consumer byte stream for libcurl write callbacks.
///
/// The producer side is synchronous so it can be called directly from
/// `CURLOPT_WRITEFUNCTION`. The consumer side is asynchronous and can apply
/// backpressure by letting the callback pause the transfer when the bounded
/// queue is full.
class CurlBodySink {
public:
  explicit CurlBodySink(size_t capacity);
  ~CurlBodySink();

  CurlBodySink(CurlBodySink const&) = delete;
  auto operator=(CurlBodySink const&) -> CurlBodySink& = delete;
  CurlBodySink(CurlBodySink&&) = delete;
  auto operator=(CurlBodySink&&) -> CurlBodySink& = delete;

  /// Receive the next queued chunk. Returns `None` after close or abort.
  auto pop() -> Task<Option<chunk_ptr>>;

  /// Abort the stream and wake a blocked consumer.
  auto abort() -> void;

  /// Close the stream and wake a blocked consumer.
  auto close() -> void;

  /// Internal hook for `perform_curl`.
  auto write(std::span<const std::byte> buffer) -> size_t;

  /// Internal hook for `perform_curl`.
  auto set_resume_callback(std::function<void()> callback) -> void;

private:
  struct Impl;
  Box<Impl> impl_;

  friend struct CurlBodyHandlers;
  friend auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor>,
                           curl::easy&, CurlBodyHandlers) -> Task<caf::error>;
};

struct CurlBodyHandlers {
  CurlBodySource* source = nullptr;
  CurlBodySink* sink = nullptr;
};

/// Drive a prepared libcurl easy handle from a Folly IO executor.
///
/// The handle must outlive the returned task. Optional body handlers integrate
/// bounded, pause-aware upload and download streams with the transfer.
auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                  curl::easy& handle, CurlBodyHandlers handlers = {})
  -> Task<caf::error>;

} // namespace tenzir
