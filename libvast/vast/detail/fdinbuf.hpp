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

#include <chrono>
#include <cstddef>
#include <optional>
#include <streambuf>
#include <vector>

namespace vast::detail {

/// A streambuffer that proxies reads to an underlying POSIX file descriptor.
/// Optionally, it supports setting a read timeout.
class fdinbuf : public std::streambuf {
  static constexpr size_t putback_area_size = 10;

public:
  /// Constructs an input streambuffer from a POSIX file descriptor.
  /// @param fd The file descriptor to construct the streambuffer for.
  /// @param buffer_size The size of the input buffer.
  /// @pre `buffer_size > putback_area_size`
  explicit fdinbuf(int fd, size_t buffer_size = 8192);

  std::optional<std::chrono::milliseconds>& read_timeout();
  bool timed_out() const;

protected:
  int_type underflow() override;

private:
  int fd_;
  std::vector<char> buffer_;
  std::optional<std::chrono::milliseconds> read_timeout_;
  bool timeout_fail_; // Was the last read failure caused by a timeout?
};

} // namespace vast::detail

