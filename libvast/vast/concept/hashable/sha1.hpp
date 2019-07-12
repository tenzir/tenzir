/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "vast/detail/endian.hpp"

namespace vast {

/// The [SHA-1](https://en.wikipedia.org/wiki/SHA-1) hash algorithm.
/// This implementation comes from https://github.com/kerukuro/digestpp.
class sha1 {
public:
  using result_type = std::array<uint32_t, 5>;

  static constexpr detail::endianness endian = detail::host_endian;

  sha1() noexcept;

  void operator()(const void* xs, size_t n) noexcept;

  operator result_type() noexcept;

  template <class Inspector>
  friend auto inspect(Inspector& f, sha1& x) {
    return f(x.H_, x.m_, x.pos_, x.total_);
  }

private:
  void finalize();

  void transform(const unsigned char* data, size_t num_blks);

  std::array<uint32_t, 5> H_;
  std::array<unsigned char, 64> m_;
  size_t pos_ = 0;
  uint64_t total_ = 0;
};

} // namespace vast
