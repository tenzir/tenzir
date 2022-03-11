//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
  [[nodiscard]] bool timed_out() const;

protected:
  int_type underflow() override;

private:
  int fd_;
  std::vector<char> buffer_;
  std::optional<std::chrono::milliseconds> read_timeout_;
  bool timeout_fail_; // Was the last read failure caused by a timeout?
};

} // namespace vast::detail
