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

#include <iterator>
#include <memory>
#include <ostream>

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"

#include "vast/format/writer.hpp"

namespace vast::format {

/// A writer that operates with a given printer.
template <class Printer>
class printer_writer : public writer {
public:
  printer_writer() = default;

  /// Constructs a generic writer.
  /// @param out The stream where to write to
  explicit printer_writer(std::unique_ptr<std::ostream> out)
    : out_{std::move(out)} {
  }

  expected<void> write(const event& e) override {
    auto i = std::ostreambuf_iterator<char>(*out_);
    if (!printer_.print(i, e))
      return make_error(ec::print_error, "failed to print event:", e);
    *out_ << '\n';
    return {};
  }

  expected<void> flush() override {
    out_->flush();
    if (!*out_)
      return make_error(ec::format_error, "failed to flush");
    return {};
  }

private:
  std::unique_ptr<std::ostream> out_;
  Printer printer_;
};

} // namespace vast::format
