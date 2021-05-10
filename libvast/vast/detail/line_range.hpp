//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/range.hpp"

#include <chrono>
#include <cstdint>
#include <istream>
#include <string>

namespace vast::detail {

// A range of non-empty lines, extracted via `std::getline`.
class line_range : range_facade<line_range> {
public:
  explicit line_range(std::istream& input);

  [[nodiscard]] const std::string& get() const;

  void next_impl();
  void next();

  // This is only supported if input_ uses a detail::fdinbuf as its streambuf,
  // otherwise the timeout is ignored. The returned bool only indicates if a
  // timeout occurred, other errors still need to be checked by `done()`.
  [[nodiscard]] bool next_timeout(std::chrono::milliseconds timeout);

  template <class Rep, class Period = std::ratio<1>>
  [[nodiscard]] bool next_timeout(std::chrono::duration<Rep, Period> timeout) {
    return next_timeout(std::chrono::duration_cast<std::chrono::milliseconds>(
      std::move(timeout)));
  }

  [[nodiscard]] bool done() const;

  std::string& line();

  [[nodiscard]] size_t line_number() const;

private:
  std::istream& input_;
  std::string line_;
  size_t line_number_ = 0;
  bool timed_out_ = false;
};

} // namespace vast::detail
