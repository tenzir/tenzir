//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/contiguous_buffer_stream.hpp"

#include <tenzir/fwd.hpp>

namespace tenzir {

auto contiguous_buffer_stream::Close() -> arrow::Status {
  if (is_open_) {
    is_open_ = false;
    buffer_.shrink_to_fit();
  }
  return arrow::Status::OK();
}

auto contiguous_buffer_stream::closed() const -> bool {
  return !is_open_;
}

auto contiguous_buffer_stream::Tell() const -> arrow::Result<int64_t> {
  return buffer_.size() + offset_;
}

auto contiguous_buffer_stream::Write(const void* data, int64_t nbytes)
  -> arrow::Status {
  if (closed()) [[unlikely]] {
    return arrow::Status::IOError("OutputStream is closed");
  }
  if (nbytes <= 0) [[unlikely]] {
    return arrow::Status::OK();
  }
  const auto old_size = buffer_.size();
  buffer_.resize(old_size + nbytes);
  std::memcpy(buffer_.data() + old_size, data, nbytes);
  return arrow::Status::OK();
}

auto contiguous_buffer_stream::purge() -> chunk_ptr {
  auto result = chunk::copy(buffer_);
  offset_ += buffer_.size();
  buffer_.clear();
  return result;
}

auto contiguous_buffer_stream::finish() -> chunk_ptr {
  const auto closed = Close();
  TENZIR_ASSERT(closed.ok());
  return chunk::make(std::move(buffer_));
}

} // namespace tenzir
