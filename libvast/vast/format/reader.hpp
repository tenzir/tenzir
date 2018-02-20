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

#ifndef VAST_FORMAT_READER_HPP
#define VAST_FORMAT_READER_HPP

#include <istream>
#include <memory>

#include "vast/detail/assert.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"

namespace vast::format {

/// A generic event reader.
template <class Parser>
class reader {
public:
  reader() = default;

  /// Constructs a generic reader.
  /// @param in The stream of logs to read.
  explicit reader(std::unique_ptr<std::istream> in) : in_{std::move(in)} {
    VAST_ASSERT(in_);
    lines_ = std::make_unique<detail::line_range>(*in_);
  }

  expected<event> read() {
    if (lines_->done())
      return make_error(ec::end_of_input, "input exhausted");
    event e;
    if (!parser_(lines_->get(), e))
      return make_error(ec::parse_error, "line", lines_->line_number());
    lines_->next();
    return e;
  }

protected:
  Parser parser_;

private:
  std::unique_ptr<std::istream> in_;
  std::unique_ptr<detail::line_range> lines_;
};

} // namespace vast::format

#endif
