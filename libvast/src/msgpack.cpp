//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/msgpack.hpp"

#include <cstddef>
#include <span>

namespace vast::msgpack {

overlay array_view::data() const {
  return overlay{data_};
}

overlay::overlay(std::span<const std::byte> buffer)
  : buffer_{buffer}, position_{0} {
  // nop
}

object overlay::get() const {
  VAST_ASSERT(position_ < buffer_.size());
  return object{buffer_.subspan(position_)};
}

size_t overlay::next() {
  auto fmt = static_cast<format>(buffer_[position_]);
  auto advance = [&](size_t bytes) {
    position_ += bytes;
    return bytes;
  };
  if (is_positive_fixint(fmt) || is_negative_fixint(fmt)) {
    return advance(1);
  } else if (is_fixstr(fmt)) {
    return advance(1 + fixstr_size(fmt));
  } else if (is_fixarray(fmt)) {
    auto n = advance(1);
    for (size_t i = 0; i < fixarray_size(fmt); ++i)
      n += next();
    return n;
  } else if (is_fixmap(fmt)) {
    auto n = advance(1);
    for (size_t i = 0; i < fixmap_size(fmt) * 2; ++i)
      n += next();
    return n;
  }
  switch (fmt) {
    default:
      break;
    case nil:
    case false_:
    case true_:
      return advance(1);
    case uint8:
    case int8:
      return advance(2);
    case uint16:
    case int16:
      return advance(3);
    case uint32:
    case int32:
    case float32:
      return advance(5);
    case uint64:
    case int64:
    case float64:
      return advance(9);
    case str8:
    case bin8:
      return advance(1 + static_cast<uint8_t>(*at(1)));
    case str16:
    case bin16:
      return advance(1 + 2 + to_num<uint16_t>(at(1)));
    case str32:
    case bin32:
      return advance(1 + 4 + to_num<uint32_t>(at(1)));
    case array16: {
      auto size = to_num<uint16_t>(at(1));
      auto n = advance(3);
      for (size_t i = 0; i < size; ++i)
        n += next();
      return n;
    }
    case array32: {
      auto size = to_num<uint32_t>(at(1));
      auto n = advance(5);
      for (size_t i = 0; i < size; ++i)
        n += next();
      return n;
    }
    case map16: {
      auto size = to_num<uint16_t>(at(1));
      auto n = advance(3);
      for (size_t i = 0; i < size * 2; ++i)
        n += next();
      return n;
    }
    case map32: {
      auto size = to_num<uint32_t>(at(1));
      auto n = advance(5);
      for (size_t i = 0; i < size * 2; ++i)
        n += next();
      return n;
    }
    case fixext1:
      return advance(1 + 1 + 1);
    case fixext2:
      return advance(1 + 1 + 2);
    case fixext4:
      return advance(1 + 1 + 4);
    case fixext8:
      return advance(1 + 1 + 8);
    case fixext16:
      return advance(1 + 1 + 16);
    case ext8:
      return advance(1 + 1 + 1 + static_cast<uint8_t>(*at(1)));
    case ext16:
      return advance(1 + 2 + 1 + to_num<uint16_t>(at(1)));
    case ext32:
      return advance(1 + 4 + 1 + to_num<uint32_t>(at(1)));
  }
  return 0;
}

size_t overlay::next(size_t n) {
  auto result = 0;
  for (size_t i = 0; i < n; ++i) {
    auto n = next();
    VAST_ASSERT(n > 0);
    result += n;
  }
  return result;
}

} // namespace vast::msgpack
