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

  const std::string& get() const;

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

  bool done() const;

  std::string& line();

  size_t line_number() const;

private:
  std::istream& input_;
  std::string line_;
  size_t line_number_ = 0;
  bool timed_out_ = false;
};

} // namespace vast::detail

