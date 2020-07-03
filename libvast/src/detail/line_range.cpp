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

#include "vast/detail/line_range.hpp"

#include "vast/detail/fdinbuf.hpp"
#include "vast/detail/getline_generic.hpp"

namespace vast {
namespace detail {

line_range::line_range(std::istream& input) : input_{input} {
}

const std::string& line_range::get() const {
  return line_;
}

void line_range::next() {
  VAST_ASSERT(!done());
  line_.clear();
  if (!input_)
    return;
  // Get the next non-empty line.
  while (line_.empty())
    if (detail::getline_generic(input_, line_))
      ++line_number_;
    else
      break;
}

bool line_range::next_timeout(std::chrono::milliseconds timeout) {
  auto* p = dynamic_cast<fdinbuf*>(input_.rdbuf());
  if (p)
    p->read_timeout() = timeout;
  // Try to read next line.
  next();
  bool timed_out = false;
  if (p) {
    timed_out = p->timed_out();
    p->read_timeout() = std::nullopt;
    // Clear error state if the read timed out
    if (!input_ && timed_out)
      input_.clear();
  }
  return timed_out;
}

bool line_range::done() const {
  return line_.empty() && !input_;
}

std::string& line_range::line() {
  return line_;
}

size_t line_range::line_number() const {
  return line_number_;
}

} // namespace detail
} // namespace vast
