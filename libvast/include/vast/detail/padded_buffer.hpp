//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <memory>
#include <string_view>

namespace vast::detail {

template <size_t PaddingSize, char PaddingValue>
class padded_buffer {
public:
  auto append(std::string_view input) -> void {
    if (const auto available_bytes = capacity_ - end_;
        available_bytes < input.size()) {
      const auto bytes_missing = input.size() - available_bytes;
      if (begin_ >= bytes_missing) {
        // copy the [begin_, end_] to the buffer start and later on append the
        // input after the end_.
        std::copy(buffer_.get() + begin_, buffer_.get() + end_, buffer_.get());
        end_ -= begin_;
        begin_ = 0u;
      } else {
        capacity_ = end_ + input.size();
        auto new_buffer = std::make_unique<char[]>(capacity_ + PaddingSize);
        std::copy(buffer_.get(), buffer_.get() + end_, new_buffer.get());
        std::fill_n(new_buffer.get() + capacity_, PaddingSize, PaddingValue);
        buffer_ = std::move(new_buffer);
      }
    }
    std::copy(input.begin(), input.end(), buffer_.get() + end_);
    end_ += input.size();
  }

  constexpr auto view() const -> std::string_view {
    return {buffer_.get() + begin_, end_ - begin_};
  }

  constexpr explicit operator bool() const {
    return begin_ != end_;
  }

  // Reuse the already allocated buffer.
  constexpr auto reset() -> void {
    end_ = 0u;
    begin_ = 0u;
  }
  // The input json can contain the whole event and a part of the other one. E.g
  // the input chunk is: {"a":5}{"a" This whole string will be in the buffer_
  // and the parser will tell us that it only consumed the first event. The
  // {"a":5} was consumed and we want to add the next chunk to the leftover
  // part. We set begin_ at the end of the consumed events because our allocated
  // buffer might still have enough capacity to write leftover + new chunk.
  constexpr auto truncate(std::size_t n) -> void {
    begin_ = end_ - n;
  }

private:
  std::unique_ptr<char[]> buffer_;
  std::size_t begin_{0u};
  std::size_t end_{0u};
  // The available bytes size. Doesn't include the padding.
  std::size_t capacity_{0u};
};

} // namespace vast::detail
