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

#include <istream>
#include <memory>

#include "vast/detail/assert.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/expected.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

namespace vast::format {

/// A reader that operates with a given parser.
template <class Parser>
class parser_reader : public single_layout_reader {
public:
  using super = single_layout_reader;

  explicit parser_reader(caf::atom_value table_slice_type)
    : super(table_slice_type) {
    // nop
  }

  /// Constructs a generic reader.
  /// @param in The stream of logs to read.
  parser_reader(caf::atom_value id, std::unique_ptr<std::istream> in)
    : parser_reader(id) {
    reset(std::move(in));
  }

  void reset(std::unique_ptr<std::istream> in) {
    VAST_ASSERT(in != nullptr);
    in_ = std::move(in);
    lines_ = std::make_unique<detail::line_range>(*in_);
  }

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) final {
    VAST_ASSERT(in_ != nullptr && lines_ != nullptr);
    event e;
    for (size_t events = 0; events < max_events; ++events) {
      if (lines_->done())
        return finish(f, make_error(ec::end_of_input, "input exhausted"));
      if (!parser_(lines_->get(), e))
        return finish(f, make_error(ec::parse_error, "line",
                                    lines_->line_number()));
      if (builder_ == nullptr || builder_->layout() != e.type()) {
        VAST_ASSERT(caf::holds_alternative<record_type>(e.type()));
        if (builder_ != nullptr)
          if (auto err = finish(f))
            return err;
        if (!reset_builder(caf::get<record_type>(e.type())))
          return make_error(ec::parse_error,
                            "unable to create a builder for layout at line",
                            lines_->line_number());
      }
      VAST_ASSERT(builder_ != nullptr);
      if (!builder_->recursive_add(e.data(), e.type()))
        return finish(f,
                      make_error(ec::parse_error,
                                 "recursive_add failed to add content at line",
                                 lines_->line_number(),
                                 std::string{lines_->get()}));
      if (builder_->rows() == max_slice_size)
        if (auto err = finish(f))
          return err;
      lines_->next();
    }
    if (auto err = finish(f))
      return err;
    return caf::none;
  }

  Parser parser_;

private:
  std::unique_ptr<std::istream> in_;
  std::unique_ptr<detail::line_range> lines_;
};

} // namespace vast::format
