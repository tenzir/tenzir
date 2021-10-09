//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository:       https://github.com/FastFilter/fastfilter_cpp
// - Commit:           95b7c98e805ee028a0934262d56e54f45f39ace7
// - Copyright Holder: Apache Software Foundation
// - Path:             src/bloom/simd-block-fixed-fpp.h
// - Created:          May 1, 2019
// - License:          Apache 2.0
//
// Adapted and modernized to fit in the VAST code base.

#pragma once

#include "vast/detail/allocate_aligned.hpp"
#include "vast/detail/operators.hpp"

#include <caf/meta/type_name.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <span>

#if defined(__AVX2__)
#  include <x86intrin.h>
#elif defined(__aarch64__)
#  include <arm_neon.h>
#else
#  error "Platform has no support for AVX2 or ARM"
#endif

namespace vast {

/// A cache-efficient Bloom filter implementation, also known as *split block
/// Bloom filter* because it splits the given bit arry into a sequence of small
/// blocks, each of which represents a standard Bloom filter that fits into one
/// cache line.
///
/// A blocked Bloom filter is substantially faster, but has a higher
/// false-positive rate than standard Bloom filters.
///
/// The implementation is a slightly tuned version by Jim Apple, per the
/// following papers:
///
/// - https://arxiv.org/pdf/2101.01719.pdf
/// - https://arxiv.org/pdf/2109.01947.pdf
///
/// @tparam HashFunction The hash function to use.
class blocked_bloom_filter : detail::equality_comparable<blocked_bloom_filter> {
public:
  using digest_type = uint64_t;

  /// The native type of a block representing a single Bloom filter.
#if defined(__AVX2__)
  using block_type = __m256i;
#elif defined(__aarch64__)
  using block_type = uint16x8_t;
#endif

  /// The size of a block.
  static constexpr size_t block_size = sizeof(block_type);

  /// Constructs a filter with a fixed size.
  /// @param size The number bytes the filter should use. The minimum size
  /// is *block_size*. If *size* is not a multiple of *block_size*, the filter
  /// rounds down *size* to the closest multiple of *block_size*.
  explicit blocked_bloom_filter(size_t size = 0);

  /// Constructs a filter with a capacity for a fixed number of items
  /// and a desired false-positive probability
  blocked_bloom_filter(size_t n, double p);

  /// Adds a hash digest.
  /// @param x The digest to add.
  [[gnu::always_inline]] inline void add(digest_type x) {
    const uint32_t idx = block_index(x, num_blocks_);
#if defined(__AVX2__)
    auto* block = &blocks_.get()[idx];
    // Perform a bitwise OR into the chosen block.
    _mm256_store_si256(block, _mm256_or_si256(*block, make_mask(x)));
#elif defined(__aarch64__)
    blocks_.get()[idx] = vorrq_u16(make_mask(x), blocks_.get()[idx]);
#endif
  }

  /// Test whether a hash digest exists in the filter.
  /// @param x The digest to test.
  /// @returns `false` if the *x* is not in the set and `true` if *x* may exist
  ///          according to the false-positive probability of the filter.
  [[gnu::always_inline]] inline bool lookup(digest_type x) const {
    const uint32_t idx = block_index(x, num_blocks_);
#if defined(__AVX2__)
    auto* block = &blocks_.get()[idx];
    // Check that all bits in the block are set.
    return _mm256_testc_si256(*block, make_mask(x));
#elif defined(__aarch64__)
    uint16x8_t bits = vbicq_u16(make_mask(x), blocks_.get()[idx]);
    uint64x2_t v64 = vreinterpretq_u64_u16(bits);
    uint32x2_t v32 = vqmovn_u64(v64);
    uint64x1_t result = vreinterpret_u64_u32(v32);
    return vget_lane_u64(result, 0) == 0;
#endif
  }

  // -- concepts --------------------------------------------------------------

  friend bool
  operator==(const blocked_bloom_filter& x, const blocked_bloom_filter& y);

  template <class Inspector>
  friend auto inspect(Inspector& f, blocked_bloom_filter& x) {
    return f(caf::meta::type_name("blocked_bloom_filter"), x.num_blocks_,
             x.blocks_);
  }

  friend std::span<const std::byte> as_bytes(const blocked_bloom_filter& x);

private:
  static inline uint64_t block_index(uint64_t digest, size_t num_blocks) {
    return ((digest >> 32) * (num_blocks)) >> 32;
  }

  // Computes a mask with 1-bits to be set/checked in each 32-bit lane.
#if defined(__AVX2__)
  [[gnu::always_inline]] static inline __m256i
  make_mask(const uint32_t digest) noexcept {
    const __m256i ones = _mm256_set1_epi32(1);
    // Set eight odd constants for multiply-shift hashing
    const __m256i rehash
      = _mm256_setr_epi32(0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                          0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U);
    __m256i digest_data = _mm256_set1_epi32(digest);
    digest_data = _mm256_mullo_epi32(rehash, digest_data);
    // Shift all data right, reducing the hash values from 32 bits to five bits.
    // Those five bits represent an index in [0, 31).
    digest_data = _mm256_srli_epi32(digest_data, 32 - 5);
    // Set a bit in each lane based on using the [0, 32) data as shift values.
    return _mm256_sllv_epi32(ones, digest_data);
  }
#elif defined(__aarch64__)
  [[gnu::always_inline]] static inline uint16x8_t
  make_mask(const uint16_t digest) noexcept {
    const uint16x8_t ones = {1, 1, 1, 1, 1, 1, 1, 1};
    const uint16x8_t rehash
      = {0x79d8, 0xe722, 0xf2fb, 0x21ec, 0x121b, 0x2302, 0x705a, 0x6e87};
    uint16x8_t digest_data
      = {digest, digest, digest, digest, digest, digest, digest, digest};
    uint16x8_t result = vmulq_u16(digest_data, rehash);
    result = vshrq_n_u16(result, 12);
    result = vshlq_u16(ones, vreinterpretq_s16_u16(result));
    return result;
  }
#endif

  size_t num_blocks_;
  detail::aligned_unique_ptr<block_type> blocks_;
};

} // namespace vast
