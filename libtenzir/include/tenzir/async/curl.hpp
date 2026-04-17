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
#include <utility>

namespace tenzir {

namespace detail {
struct CurlBodyAccess;
} // namespace detail

/// The outcome of `perform_curl*()`.

enum class CurlPerformResultKind {
  success,
  local_abort,
  failure,
};

class CurlPerformResult {
public:
  static auto success() -> CurlPerformResult {
    return CurlPerformResult{CurlPerformResultKind::success, {}};
  }

  static auto local_abort() -> CurlPerformResult {
    return CurlPerformResult{CurlPerformResultKind::local_abort, {}};
  }

  static auto failure(caf::error error) -> CurlPerformResult {
    return CurlPerformResult{CurlPerformResultKind::failure, std::move(error)};
  }

  auto kind() const -> CurlPerformResultKind {
    return kind_;
  }

  auto is_success() const -> bool {
    return kind_ == CurlPerformResultKind::success;
  }

  auto is_local_abort() const -> bool {
    return kind_ == CurlPerformResultKind::local_abort;
  }

  auto is_failure() const -> bool {
    return kind_ == CurlPerformResultKind::failure;
  }

  /// The transport error for failed transfers.
  ///
  /// Only inspect this if `is_failure()` is `true`.
  auto error() const -> caf::error const& {
    return error_;
  }

private:
  CurlPerformResult(CurlPerformResultKind kind, caf::error error)
    : kind_{kind}, error_{std::move(error)} {
  }

  CurlPerformResultKind kind_;
  caf::error error_;
};

/// Asynchronously drive a prepared `curl::easy` handle on a Folly IO executor.
///
/// This layer owns only the libcurl/Folly integration. Callers are expected to
/// configure the `curl::easy` handle themselves, either directly via
/// `tenzir::curl` or through higher-level helpers. Using `transfer` is
/// optional.
///
/// Typical usage:
/// - For uploads, create a `CurlUploadBody`, spawn a producer task that
///   repeatedly `push()`es chunks and eventually calls `close()` or `abort()`,
///   and then `co_await perform_curl_upload(...)`.
/// - For downloads, create a `CurlDownloadBody`, spawn a consumer task that
///   repeatedly `pop()`s chunks until it gets `None`, and then
///   `co_await perform_curl_download(...)`.
///
/// In other words, callers should mostly interact with `push()`/`pop()` and
/// `close()`/`abort()`. The libcurl callback plumbing stays internal to this
/// module.
///
/// Lifetime:
/// - The `curl::easy` handle and body objects must outlive the returned task.
/// - The transfer itself is driven on the provided IO executor's EventBase.
/// - Producer and consumer coroutines may run elsewhere; backpressure is
///   propagated through libcurl pause/resume callbacks.
///
/// Cancellation:
/// - Cancelling the awaiting task aborts the in-flight transfer on the IO
///   executor, wakes associated upload/download bodies, and propagates
///   `folly::OperationCancelled`.
/// - Upload cancellation before the transfer starts aborts the upload body so
///   blocked producers do not wait forever for a transfer that will never run.

/// Single-producer, single-consumer byte stream for libcurl upload callbacks.
///
/// The producer side is asynchronous and can wait for bounded capacity. The
/// consumer side is synchronous so libcurl can pull data directly from
/// `CURLOPT_READFUNCTION`. When the callback runs out of queued data, the
/// transfer pauses instead of blocking an EventBase thread.
class CurlUploadBody {
public:
  explicit CurlUploadBody(size_t capacity);
  ~CurlUploadBody();

  CurlUploadBody(CurlUploadBody const&) = delete;
  auto operator=(CurlUploadBody const&) -> CurlUploadBody& = delete;
  CurlUploadBody(CurlUploadBody&&) = delete;
  auto operator=(CurlUploadBody&&) -> CurlUploadBody& = delete;

  /// Queue a chunk for libcurl to read.
  /// Returns `false` if the stream has already been closed or aborted.
  auto push(chunk_ptr chunk) -> Task<bool>;

  /// Signal end-of-stream. Subsequent reads return EOF after buffered data.
  /// Call this after the last queued chunk was pushed successfully.
  auto close() -> void;

  /// Returns whether the stream was aborted locally.
  auto is_aborted() -> bool;

  /// Abort the stream because the local producer failed or was cancelled.
  /// Subsequent reads fail with `CURLE_ABORTED_BY_CALLBACK`.
  auto abort() -> void;

private:
  auto wait_until_ready() -> Task<bool>;
  auto read(std::span<std::byte> buffer) -> size_t;
  auto set_resume_callback(std::function<void()> callback) -> void;
  auto terminate() -> void;

  struct Impl;
  Box<Impl> impl_;

  friend struct detail::CurlBodyAccess;
};

/// Single-producer, single-consumer byte stream for libcurl download
/// callbacks.
///
/// The producer side is synchronous so libcurl can call it directly from
/// `CURLOPT_WRITEFUNCTION`. The consumer side is asynchronous and can apply
/// backpressure by letting the callback pause the transfer when the bounded
/// queue is full.
class CurlDownloadBody {
public:
  explicit CurlDownloadBody(size_t capacity);
  ~CurlDownloadBody();

  CurlDownloadBody(CurlDownloadBody const&) = delete;
  auto operator=(CurlDownloadBody const&) -> CurlDownloadBody& = delete;
  CurlDownloadBody(CurlDownloadBody&&) = delete;
  auto operator=(CurlDownloadBody&&) -> CurlDownloadBody& = delete;

  /// Receive the next queued chunk. Returns `None` after close or abort.
  /// Check the result of `perform_curl_download(...)` to distinguish a clean
  /// end-of-stream, a local abort, and a transport failure.
  auto pop() -> Task<Option<chunk_ptr>>;

  /// Returns whether the stream was aborted locally.
  auto is_aborted() -> bool;

  /// Abort the stream and wake a blocked consumer.
  auto abort() -> void;

  /// Close the stream and wake a blocked consumer.
  auto close() -> void;

private:
  auto write(std::span<const std::byte> buffer) -> size_t;
  auto set_resume_callback(std::function<void()> callback) -> void;

  struct Impl;
  Box<Impl> impl_;

  friend struct detail::CurlBodyAccess;
};

/// Drive a prepared curl handle with no streaming body callbacks.
///
/// Returns `success` on completion or `failure` with the curl error.
/// Cancellation aborts the transfer and propagates `folly::OperationCancelled`.
auto perform_curl(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                  curl::easy& handle) -> Task<CurlPerformResult>;

/// Drive a prepared curl handle with a streaming upload body.
///
/// The upload waits until the body has buffered data or reached a terminal
/// state before it starts the transfer, so callers do not need a separate
/// readiness barrier. If the body was already aborted at that point, the
/// function returns `local_abort` without touching the remote peer. If the
/// body closes before any bytes were queued, the function returns `success`
/// without starting the transfer.
///
/// Returns `local_abort` if the upload body was aborted locally, and `failure`
/// for transport errors.
/// Cancellation aborts the body, aborts the transfer, and propagates
/// `folly::OperationCancelled`.
auto perform_curl_upload(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                         curl::easy& handle, CurlUploadBody& body)
  -> Task<CurlPerformResult>;

/// Drive a prepared curl handle with a streaming download body.
///
/// Callers should consume `body.pop()` concurrently while this task is running.
/// The body is closed automatically when the transfer finishes or fails.
///
/// Returns `local_abort` if the download body was aborted locally, and
/// `failure` for transport errors.
/// Cancellation aborts the body, aborts the transfer, and propagates
/// `folly::OperationCancelled`.
auto perform_curl_download(
  folly::Executor::KeepAlive<folly::IOExecutor> executor, curl::easy& handle,
  CurlDownloadBody& body) -> Task<CurlPerformResult>;

} // namespace tenzir
