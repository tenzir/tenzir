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
#include "tenzir/variant.hpp"

#include <folly/Executor.h>
#include <folly/executors/IOExecutor.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace tenzir {

enum class CurlTransferStatus {
  finished,
  local_abort,
};

struct CurlTransferResult {
  CurlTransferStatus status;
  Option<long> response_code = None{};
};

struct CurlEasyError {
  curl::easy::code code;
};

struct CurlMultiError {
  curl::multi::code code;
};

using CurlError = variant<CurlEasyError, CurlMultiError>;

auto to_string(CurlError const& error) -> std::string_view;

using CurlUploadResult = Result<CurlTransferResult, CurlError>;

struct CurlDownloadChunk {
  chunk_ptr chunk;
};

struct CurlDownloadDone {
  CurlTransferStatus status;
  Option<long> response_code = None{};
};

struct CurlDownloadFailed {
  CurlError error;
};

using CurlDownloadEvent
  = variant<CurlDownloadChunk, CurlDownloadDone, CurlDownloadFailed>;

class CurlUploadTransfer {
public:
  ~CurlUploadTransfer();

  CurlUploadTransfer(CurlUploadTransfer const&) = delete;
  auto operator=(CurlUploadTransfer const&) -> CurlUploadTransfer& = delete;
  CurlUploadTransfer(CurlUploadTransfer&&) noexcept;
  auto operator=(CurlUploadTransfer&&) noexcept -> CurlUploadTransfer&;

  /// Queue a chunk for libcurl to read.
  /// Returns `false` if the stream has already been closed or aborted.
  auto push(chunk_ptr chunk) -> Task<bool>;

  /// Signal end-of-stream. Subsequent reads return EOF after buffered data.
  /// Call this after the last queued chunk was pushed successfully.
  auto close() -> void;

  /// Abort the local stream and wake blocked producers or consumers.
  auto abort() -> void;

  auto result() const -> Task<CurlUploadResult>;

private:
  struct Impl;

  explicit CurlUploadTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

class CurlDownloadTransfer {
public:
  ~CurlDownloadTransfer();

  CurlDownloadTransfer(CurlDownloadTransfer const&) = delete;
  auto operator=(CurlDownloadTransfer const&) -> CurlDownloadTransfer& = delete;
  CurlDownloadTransfer(CurlDownloadTransfer&&) noexcept;
  auto operator=(CurlDownloadTransfer&&) noexcept -> CurlDownloadTransfer&;

  /// Receive the next download event.
  ///
  /// The stream yields chunks while the transfer is active, then exactly one
  /// terminal event for clean completion, local abort, or transport failure.
  /// Later calls return `None`.
  auto next() const -> Task<Option<CurlDownloadEvent>>;

  /// Abort the local stream and wake blocked consumers.
  auto abort() -> void;

private:
  struct Impl;

  explicit CurlDownloadTransfer(Box<Impl> impl);

  Box<Impl> impl_;

  friend class CurlSession;
};

/// Reusable async curl session.
///
/// Configure `easy()` directly while the session is idle, then start one
/// semantic transfer at a time. The transfer runs on the Folly IO executor
/// provided to `make()`.
class CurlSession {
public:
  static auto make(folly::Executor::KeepAlive<folly::IOExecutor> executor)
    -> CurlSession;

  ~CurlSession();

  CurlSession(CurlSession const&) = delete;
  auto operator=(CurlSession const&) -> CurlSession& = delete;
  CurlSession(CurlSession&&) noexcept;
  auto operator=(CurlSession&&) noexcept -> CurlSession&;

  /// Access the underlying easy handle for pre-transfer configuration.
  /// Panics while a transfer is active.
  auto easy() -> curl::easy&;

  auto start_upload(size_t buffer_capacity = 16) -> CurlUploadTransfer;
  auto start_download(size_t buffer_capacity = 16) -> CurlDownloadTransfer;

  auto busy() const -> bool;

private:
  struct Impl;

  explicit CurlSession(Box<Impl> impl);

  Box<Impl> impl_;
};

} // namespace tenzir
