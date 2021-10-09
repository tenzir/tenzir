//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketches/blocked_bloom_filter.hpp"

#include <filter/block.hpp>

#include <cstring>

namespace vast {

blocked_bloom_filter::blocked_bloom_filter(size_t size) {
  if (size < block_size)
    size = block_size;
  num_blocks_ = size / block_size;
  auto num_bytes = num_blocks_ * block_size;
  blocks_ = detail::allocate_aligned<block_type>(block_size, num_bytes);
  std::memset(blocks_.get(), 0, num_bytes);
}

// Delegate the computation of needed bytes to libfilter.
blocked_bloom_filter::blocked_bloom_filter(size_t n, double p)
  : blocked_bloom_filter{libfilter_block_bytes_needed(n, p)} {
}

bool operator==(const blocked_bloom_filter& x, const blocked_bloom_filter& y) {
  auto xs = as_bytes(x);
  auto ys = as_bytes(y);
  if (xs.size() != ys.size())
    return false;
  return std::memcmp(xs.data(), ys.data(), xs.size()) == 0;
}

std::span<const std::byte> as_bytes(const blocked_bloom_filter& x) {
  auto data = reinterpret_cast<const std::byte*>(x.blocks_.get());
  auto size = x.num_blocks_ * blocked_bloom_filter::block_size;
  return std::span<const std::byte>{data, size};
}

} // namespace vast
