//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/chunked_buffer_output_stream.hpp"

#include <tenzir/fwd.hpp>

namespace tenzir::plugins::parquet {

auto chunked_buffer_output_stream::Close() -> arrow::Status {
  if (is_open_) {
    is_open_ = false;
    buffer_.shrink_to_fit();
  }
  return arrow::Status::OK();
}

auto chunked_buffer_output_stream::closed() const -> bool {
  return !is_open_;
}

auto chunked_buffer_output_stream::Tell() const -> arrow::Result<int64_t> {
  return buffer_.size() + offset_;
}

auto chunked_buffer_output_stream::Write(const void* data, int64_t nbytes)
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

auto chunked_buffer_output_stream::purge() -> chunk_ptr {
  auto result = chunk::copy(buffer_);
  offset_ += buffer_.size();
  buffer_.clear();
  return result;
}

auto chunked_buffer_output_stream::finish() -> chunk_ptr {
  const auto closed = Close();
  TENZIR_ASSERT(closed.ok());
  return chunk::make(std::move(buffer_));
}

} // namespace tenzir::plugins::parquet
