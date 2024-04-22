//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/chunk.hpp"

#include <arrow/io/api.h>

namespace tenzir::plugins::parquet {

// An output stream that returns contents of the buffer on request,
// but appears to be a contigous stream from the Tell() API
class chunked_buffer_output_stream final : public arrow::io::OutputStream {
public:
  chunked_buffer_output_stream() = default;
  ~chunked_buffer_output_stream() override = default;

  auto Close() -> arrow::Status override;
  auto closed() const -> bool override;

  // Return the position of the stream as though it is contiguous
  auto Tell() const -> arrow::Result<int64_t> override;

  // Write the given data to the stream
  auto Write(const void* data, int64_t nbytes) -> arrow::Status override;

  // Clear and return contents of buffer
  auto purge() -> chunk_ptr;

  // Close and return contents of the buffer
  auto finish() -> chunk_ptr;

private:
  bool is_open_ = true;
  std::vector<std::byte> buffer_ = {};
  size_t offset_ = {};
};
} // namespace tenzir::plugins::parquet
