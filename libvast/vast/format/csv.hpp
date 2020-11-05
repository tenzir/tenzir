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

#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/numeric.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/config.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/format/ostream_writer.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/schema.hpp"

#include <caf/fwd.hpp>
#include <caf/none.hpp>

namespace vast::format::csv {

struct options {
  char separator;
  std::string set_separator;
  std::string kvp_separator;
};

class writer : public format::ostream_writer {
public:
  using defaults = vast::defaults::export_::csv;

  using super = format::ostream_writer;

  using super::super;

  caf::error write(const table_slice_ptr& x) override;

  const char* name() const override;

private:
  std::string last_layout_;
};

/// A reader for CSV data. It operates with a *selector* to determine the
/// mapping of CSV object to the appropriate record type in the schema.
class reader final : public single_layout_reader {
public:
  using super = single_layout_reader;
  using iterator_type = std::string::const_iterator;
  using parser_type = type_erased_parser<iterator_type>;

  /// Constructs a CSV reader.
  /// @param table_slice_type The ID for table slice type to build.
  /// @param options Additional options.
  /// @param in The stream of CSV lines.
  reader(caf::atom_value table_slice_type, const caf::settings& options,
         std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  vast::system::report status() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  struct rec_table {
    record_type type;
    std::vector<std::string> sorted;
  };
  caf::optional<record_type> make_layout(const std::vector<std::string>& names);

  caf::expected<parser_type> read_header(std::string_view line);

  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  vast::schema schema_;
  std::vector<rec_table> records;
  caf::optional<parser_type> parser_;
  options opt_;
  mutable size_t num_lines_ = 0;
  mutable size_t num_invalid_lines_ = 0;
};

} // namespace vast::format::csv
