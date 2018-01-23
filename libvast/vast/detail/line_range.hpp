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

#ifndef VAST_DETAIL_LINE_RANGE_HPP
#define VAST_DETAIL_LINE_RANGE_HPP

#include <cstdint>
#include <istream>
#include <string>

#include "vast/detail/range.hpp"

namespace vast::detail {

// A range of non-empty lines, extracted via `std::getline`.
class line_range : range_facade<line_range> {
public:
  line_range(std::istream& input);

  const std::string& get() const;

  void next();

  bool done() const;

  std::string& line();

  size_t line_number() const;

private:
  std::istream& input_;
  std::string line_;
  size_t line_number_ = 0;
};

} // namespace vast::detail

#endif
