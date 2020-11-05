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

#include <iosfwd>
#include <memory>
#include <string_view>
#include <vector>

#include <caf/error.hpp>

#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/format/writer.hpp"
#include "vast/policy/include_field_names.hpp"
#include "vast/policy/omit_field_names.hpp"
#include "vast/table_slice.hpp"

namespace vast::format {

class ostream_writer : public writer {
public:
  // -- member types -----------------------------------------------------------

  using ostream_ptr = std::unique_ptr<std::ostream>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit ostream_writer(ostream_ptr out);

  ostream_writer() = default;

  ostream_writer(ostream_writer&&) = default;

  ostream_writer& operator=(ostream_writer&&) = default;

  ~ostream_writer() override;

  // -- overrides --------------------------------------------------------------

  caf::expected<void> flush() override;

  // -- properties -------------------------------------------------------------

  /// @returns the managed output stream.
  /// @pre `out_ != nullptr`
  std::ostream& out();

protected:
  /// Appends `x` to `buf_`.
  void append(std::string_view x) {
    buf_.insert(buf_.end(), x.begin(), x.end());
  }

  /// Appends `x` to `buf_`.
  void append(char x) {
    buf_.emplace_back(x);
  }

  /// Prints a table slice using the given VAST printer. This function assumes
  /// a human-readable output where each row in the slice gets printed to a
  /// single line.
  /// @tparam Policy either ::include_field_names to repeat the field name for
  ///         each value (e.g., JSON output) or ::omit_field_names to print the
  ///         values only after an initial header (e.g., Zeek output).
  /// @param printer The VAST printer for generating formatted output.
  /// @param xs The table slice for printing.
  /// @param begin_of_line Prefix for each printed line. For example, a JSON
  ///        writer would start each line with a '{'.
  /// @param separator Character sequence for separating columns. For example,
  ///        most human-readable formats could use ", ".
  /// @param end_of_line Suffix for each printed line. For example, a JSON
  ///        writer would end each line with a '}'.
  /// @returns `ec::print_error` if `printer` fails to generate output,
  ///          otherwise `caf::none`.
  template <class Policy, class Printer>
  caf::error print(Printer& printer, const table_slice_ptr& xs,
                   std::string_view begin_of_line, std::string_view separator,
                   std::string_view end_of_line) {
    auto layout = xs->layout();
    auto print_field = [&](auto& iter, size_t row, size_t column) {
      auto rep = [&](data_view x) {
        if constexpr (std::is_same_v<Policy, policy::include_field_names>)
          return std::pair{std::string_view{layout.fields[column].name}, x};
        else if constexpr (std::is_same_v<Policy, policy::omit_field_names>)
          return x;
        else
          static_assert(detail::always_false_v<Policy>,
                        "Unsupported policy: Expected either "
                        "include_field_names or omit_field_names");
      };
      auto x = to_canonical(layout.fields[column].type, xs->at(row, column));
      return printer.print(iter, rep(std::move(x)));
    };
    auto iter = std::back_inserter(buf_);
    for (size_t row = 0; row < xs->rows(); ++row) {
      append(begin_of_line);
      if (!print_field(iter, row, 0))
        return ec::print_error;
      for (size_t column = 1; column < xs->columns(); ++column) {
        append(separator);
        if (!print_field(iter, row, column))
          return ec::print_error;
      }
      append(end_of_line);
      append('\n');
      write_buf();
    }
    return caf::none;
  }

  /// Writes the content of `buf_` to `out_` and clears `buf_` afterwards.
  void write_buf();

  /// Buffer for building lines before writing to `out_`. Printing into this
  /// buffer with a `back_inserter` and then calling `out_->write(...)` gives a
  /// 4x speedup over printing directly to `out_`, even when setting
  /// `sync_with_stdio(false)`.
  std::vector<char> buf_;

  /// Output stream for writing to STDOUT or disk.
  ostream_ptr out_;
};

/// @relates ostream_writer
using ostream_writer_ptr = std::unique_ptr<ostream_writer>;

} // namespace vast::format
