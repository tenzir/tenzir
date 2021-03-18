// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/line_range.hpp"

#include "vast/detail/absorb_line.hpp"
#include "vast/detail/fdinbuf.hpp"
#include "vast/logger.hpp"

namespace vast {
namespace detail {

line_range::line_range(std::istream& input) : input_{input} {
}

const std::string& line_range::get() const {
  return line_;
}

void line_range::next_impl() {
  if (!input_)
    return;
  // Get the next non-empty line.
  do {
    if (detail::absorb_line(input_, line_))
      ++line_number_;
    else
      break;
  } while (line_.empty());
}

void line_range::next() {
  VAST_ASSERT(!done());
  line_.clear();
  next_impl();
}

bool line_range::next_timeout(std::chrono::milliseconds timeout) {
  auto* p = dynamic_cast<fdinbuf*>(input_.rdbuf());
  if (p)
    p->read_timeout() = timeout;
  // Clear if the previous read did not time out.
  if (!timed_out_)
    line_.clear();
  // Try to read next line.
  next_impl();
  timed_out_ = false;
  if (p) {
    timed_out_ = p->timed_out();
    p->read_timeout() = std::nullopt;
    // Clear error state if the read timed out
    if (timed_out_) {
      input_.clear();
    }
  }
  return timed_out_;
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
