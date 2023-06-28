//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
#include "vast/format/single_schema_reader.hpp"
#include "vast/module.hpp"

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

  writer(ostream_ptr out, const caf::settings& options);

  caf::error write(const table_slice& x) override;

  [[nodiscard]] const char* name() const override;

private:
  std::string last_schema_;
};

/// A reader for CSV data. It operates with a *selector* to determine the
/// mapping of CSV object to the appropriate record type in the module.
class reader final : public single_schema_reader {
public:
  using super = single_schema_reader;
  using iterator_type = std::string::const_iterator;
  using parser_type = type_erased_parser<iterator_type>;

  constexpr static const defaults csv = {"tenzir.import.csv"};

  /// Constructs a CSV reader.
  /// @param options Additional options.
  /// @param in The stream of CSV lines.
  reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                       = nullptr);

  void reset(std::unique_ptr<std::istream> in) override;

  caf::error module(vast::module mod) override;

  vast::module module() const override;

  vast::report status() const override;

  const char* name() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

private:
  struct rec_table {
    record_type type;
    std::vector<std::string> sorted;
  };
  caf::optional<type>
  make_schema(const std::vector<std::string>& names, bool first_run = true);

  caf::expected<parser_type> read_header(std::string_view line);

  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  vast::module module_;
  std::vector<rec_table> records;
  caf::optional<parser_type> parser_;
  options opt_;
  mutable size_t num_lines_ = 0;
  mutable size_t num_invalid_lines_ = 0;
};

} // namespace vast::format::csv
