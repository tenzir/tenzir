//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/option.hpp"

#include <folly/File.h>

#include <cstddef>
#include <span>

namespace folly {
class IOBufQueue;
} // namespace folly

namespace tenzir {

class WritePipe {
public:
  explicit WritePipe(folly::File pipe, int child_fd);
  WritePipe(WritePipe&&) noexcept = default;
  auto operator=(WritePipe&&) noexcept -> WritePipe& = default;
  WritePipe(WritePipe const&) = delete;
  auto operator=(WritePipe const&) -> WritePipe& = delete;
  ~WritePipe();

  auto write(std::span<std::byte const> bytes) -> Task<void>;
  auto write(chunk_ptr chunk) -> Task<void>;
  auto close() -> Task<void>;

  auto is_closed() const noexcept -> bool;
  auto child_fd() const noexcept -> int;

private:
  folly::File pipe_;
  int child_fd_ = -1;
  bool closed_ = false;
};

class ReadPipe {
public:
  explicit ReadPipe(folly::File pipe, int child_fd);
  ReadPipe(ReadPipe&&) noexcept = default;
  auto operator=(ReadPipe&&) noexcept -> ReadPipe& = default;
  ReadPipe(ReadPipe const&) = delete;
  auto operator=(ReadPipe const&) -> ReadPipe& = delete;
  ~ReadPipe();

  auto read_chunk(size_t max_bytes = 64 * 1024) -> Task<Option<chunk_ptr>>;
  auto read_some(folly::IOBufQueue& out, size_t min_read = 1,
                 size_t new_alloc = 64 * 1024) -> Task<size_t>;
  auto close() -> Task<void>;

  auto is_closed() const noexcept -> bool;
  auto is_eof() const noexcept -> bool;
  auto child_fd() const noexcept -> int;

private:
  folly::File pipe_;
  int child_fd_ = -1;
  bool eof_ = false;
  bool closed_ = false;
};

} // namespace tenzir
