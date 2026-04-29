//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/blob.hpp"
#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <simdjson.h>
#include <span>

namespace tenzir {

class SimdjsonPaddedBuffer {
public:
  static constexpr auto padding = simdjson::SIMDJSON_PADDING;

  SimdjsonPaddedBuffer() {
    resize(0);
  }

  explicit SimdjsonPaddedBuffer(size_t size) {
    resize(size);
  }

  explicit SimdjsonPaddedBuffer(std::span<std::byte const> input) {
    assign(input);
  }

  auto size() const -> size_t {
    return size_;
  }

  auto empty() const -> bool {
    return size_ == 0;
  }

  auto data() -> std::byte* {
    return bytes_.data();
  }

  auto data() const -> std::byte const* {
    return bytes_.data();
  }

  auto view() const -> std::span<std::byte const> {
    return {data(), size_};
  }

  auto clear() -> void {
    resize(0);
  }

  auto resize(size_t size) -> void {
    bytes_.resize(size + padding);
    size_ = size;
    std::fill_n(bytes_.data() + size_, padding, std::byte{0});
  }

  auto assign(std::span<std::byte const> input) -> void {
    resize(input.size());
    if (not input.empty()) {
      std::memcpy(data(), input.data(), input.size_bytes());
    }
  }

  auto append(std::span<std::byte const> input) -> void {
    auto offset = size_;
    resize(size_ + input.size());
    if (not input.empty()) {
      std::memcpy(data() + offset, input.data(), input.size_bytes());
    }
  }

  auto padded_view(size_t offset = 0, size_t length = std::dynamic_extent) const
    -> simdjson::padded_string_view {
    TENZIR_ASSERT(offset <= size_);
    if (length == std::dynamic_extent) {
      length = size_ - offset;
    }
    TENZIR_ASSERT(offset + length <= size_);
    return simdjson::padded_string_view{
      reinterpret_cast<char const*>(data() + offset),
      length,
      bytes_.size() - offset,
    };
  }

private:
  blob bytes_;
  size_t size_ = 0;
};

} // namespace tenzir
