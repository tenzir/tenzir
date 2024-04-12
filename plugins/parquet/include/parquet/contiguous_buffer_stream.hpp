//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "arrow/io/file.h"

#include <arrow/array.h>
#include <arrow/compute/cast.h>
#include <arrow/io/api.h>
#include <arrow/table.h>

namespace tenzir {

class contiguous_buffer_stream : public arrow::io::OutputStream {
public:
  explicit contiguous_buffer_stream(
    const std::shared_ptr<arrow::ResizableBuffer>& buffer);

  /// \brief Create in-memory output stream with indicated capacity using a
  /// memory pool
  /// \param[in] initial_capacity the initial allocated internal capacity of
  /// the OutputStream
  /// \param[in,out] pool a MemoryPool to use for allocations
  /// \return the created stream
  static auto Create(int64_t initial_capacity = 4096,
                     arrow::MemoryPool* pool = arrow::default_memory_pool())
    -> arrow::Result<std::shared_ptr<contiguous_buffer_stream>>;

  ~contiguous_buffer_stream() override;

  // Implement the OutputStream interface

  /// Close the stream, preserving the buffer (retrieve it with Finish()).
  auto Close() -> arrow::Status override;
  auto closed() const -> bool override;
  auto Tell() const -> arrow::Result<int64_t> override;
  auto Write(const void* data, int64_t nbytes) -> arrow::Status override;

  /// \cond FALSE
  using OutputStream::Write;
  /// \endcond

  /// Close the stream and return the buffer
  auto Finish() -> arrow::Result<std::shared_ptr<arrow::Buffer>>;

  /// \brief Initialize state of OutputStream with newly allocated memory and
  /// set position to 0
  /// \param[in] initial_capacity the starting allocated capacity
  /// \param[in,out] pool the memory pool to use for allocations
  /// \return Status
  auto Reset(int64_t initial_capacity = 1024, arrow::MemoryPool* pool
                                              = arrow::default_memory_pool())
    -> arrow::Status;

  auto PurgeReset() -> arrow::Status;

  auto capacity() const -> int64_t {
    return streaming_capacity_;
  }

  auto Purge() -> arrow::Result<std::shared_ptr<arrow::Buffer>>;

private:
  contiguous_buffer_stream();

  // Ensures there is sufficient space available to write nbytes
  auto Reserve(int64_t nbytes) -> arrow::Status;
  auto Streaming_Reserve(int64_t nbytes) -> arrow::Status;

  std::shared_ptr<arrow::ResizableBuffer> buffer_;
  bool is_open_;
  int64_t capacity_;
  int64_t streaming_position_;
  int64_t buffer_position_;
  uint8_t* mutable_data_;
  int64_t streaming_capacity_;
  int64_t data_size_;
};
} // namespace tenzir
