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

namespace tenzir {

class contiguous_buffer_stream final : public arrow::io::OutputStream {
public:
  contiguous_buffer_stream() = default;
  ~contiguous_buffer_stream() override = default;

  auto Close() -> arrow::Status override;
  auto closed() const -> bool override;
  auto Tell() const -> arrow::Result<int64_t> override;
  auto Write(const void* data, int64_t nbytes) -> arrow::Status override;

  auto purge() -> chunk_ptr;
  auto finish() -> chunk_ptr;

private:
  bool is_open_ = true;
  std::vector<std::byte> buffer_ = {};
  size_t offset_ = {};
};
} // namespace tenzir
