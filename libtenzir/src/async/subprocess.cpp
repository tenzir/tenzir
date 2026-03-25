//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/subprocess.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"

#include <folly/FileUtil.h>
#include <folly/io/IOBuf.h>
#include <folly/io/IOBufQueue.h>

#include <algorithm>
#include <cerrno>
#include <system_error>
#include <vector>

namespace tenzir {

namespace {

auto throw_last_system_error(char const* operation) -> void {
  throw std::system_error{errno, std::generic_category(), operation};
}

} // namespace

WritePipe::WritePipe(folly::File pipe, int child_fd)
  : pipe_{std::move(pipe)}, child_fd_{child_fd} {
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(child_fd_ >= 0);
}

WritePipe::~WritePipe() = default;

auto WritePipe::write(std::span<std::byte const> bytes) -> Task<void> {
  if (bytes.empty()) {
    co_return;
  }
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(not closed_);
  auto buffer = std::vector<std::byte>{bytes.begin(), bytes.end()};
  auto fd = pipe_.fd();
  co_await spawn_blocking([fd, buffer = std::move(buffer)]() {
    auto written = folly::writeFull(fd, buffer.data(), buffer.size());
    if (written == -1) {
      throw_last_system_error("writeFull");
    }
    if (detail::narrow<size_t>(written) != buffer.size()) {
      throw std::system_error{EIO, std::generic_category(),
                              "writeFull returned a short write"};
    }
  });
}

auto WritePipe::write(chunk_ptr chunk) -> Task<void> {
  if (not chunk or chunk->size() == 0) {
    co_return;
  }
  co_await write(as_bytes(chunk));
}

auto WritePipe::close() -> Task<void> {
  if (closed_) {
    co_return;
  }
  closed_ = true;
  auto pipe = std::move(pipe_);
  co_await spawn_blocking([pipe = std::move(pipe)]() mutable {
    if (pipe) {
      pipe.close();
    }
  });
}

auto WritePipe::is_closed() const noexcept -> bool {
  return closed_ or not pipe_;
}

auto WritePipe::child_fd() const noexcept -> int {
  return child_fd_;
}

ReadPipe::ReadPipe(folly::File pipe, int child_fd)
  : pipe_{std::move(pipe)}, child_fd_{child_fd} {
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(child_fd_ >= 0);
}

ReadPipe::~ReadPipe() = default;

auto ReadPipe::read_chunk(size_t max_bytes) -> Task<Option<chunk_ptr>> {
  if (closed_ or eof_) {
    co_return None{};
  }
  TENZIR_ASSERT(pipe_);
  TENZIR_ASSERT(not closed_);
  TENZIR_ASSERT(max_bytes > 0);
  auto fd = pipe_.fd();
  auto result = co_await spawn_blocking([fd, max_bytes]() -> Option<chunk_ptr> {
    auto buffer = std::vector<std::byte>{};
    buffer.resize(max_bytes);
    auto bytes_read = folly::readNoInt(fd, buffer.data(), buffer.size());
    if (bytes_read == -1) {
      throw_last_system_error("readNoInt");
    }
    if (bytes_read == 0) {
      return None{};
    }
    buffer.resize(detail::narrow<size_t>(bytes_read));
    return chunk::make(std::move(buffer));
  });
  if (result.is_none()) {
    eof_ = true;
  }
  co_return result;
}

auto ReadPipe::read_some(folly::IOBufQueue& out, size_t min_read,
                         size_t new_alloc) -> Task<size_t> {
  auto max_bytes = std::max(min_read, new_alloc);
  if (max_bytes == 0) {
    co_return 0;
  }
  auto next = co_await read_chunk(max_bytes);
  if (next.is_none()) {
    co_return 0;
  }
  auto bytes = as_bytes(*next);
  out.append(folly::IOBuf::copyBuffer(bytes.data(), bytes.size()));
  co_return bytes.size();
}

auto ReadPipe::close() -> Task<void> {
  if (closed_) {
    co_return;
  }
  closed_ = true;
  auto pipe = std::move(pipe_);
  co_await spawn_blocking([pipe = std::move(pipe)]() mutable {
    if (pipe) {
      pipe.close();
    }
  });
}

auto ReadPipe::is_closed() const noexcept -> bool {
  return closed_ or not pipe_;
}

auto ReadPipe::is_eof() const noexcept -> bool {
  return eof_;
}

auto ReadPipe::child_fd() const noexcept -> int {
  return child_fd_;
}

} // namespace tenzir
