//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/line_range.hpp"
#include "vast/error.hpp"
#include "vast/format/json/selector.hpp"
#include "vast/format/multi_schema_reader.hpp"
#include "vast/format/ostream_writer.hpp"
#include "vast/module.hpp"
#include "vast/policy/omit_nulls.hpp"
#include "vast/view.hpp"

#include <caf/expected.hpp>
#include <caf/settings.hpp>

#include <chrono>
#include <optional>
#include <simdjson.h>

namespace vast::format::json {

/// Extracts data from a given JSON object for a given type.
/// @param object The simdjson DOM element of type object.
/// @param builder The builder to add data to.
caf::error
add(const ::simdjson::dom::object& object, table_slice_builder& builder);

class writer : public ostream_writer {
public:
  using super = ostream_writer;

  writer(ostream_ptr out, const caf::settings& options);

  caf::error write(const table_slice& x) override;

  [[nodiscard]] const char* name() const override;

private:
  bool flatten_ = false;
  bool numeric_durations_ = false;
  bool omit_nulls_ = false;
};

/// A reader for JSON data. It operates with a *selector* to determine the
/// mapping of JSON object to the appropriate record type in the module.
class reader final : public multi_schema_reader {
public:
  using super = multi_schema_reader;

  /// Constructs a JSON reader.
  /// @param options Additional options.
  /// @param in The stream of JSON objects.
  explicit reader(const caf::settings& options, std::unique_ptr<std::istream> in
                                                = nullptr);

  void reset(std::unique_ptr<std::istream> in) override;

  caf::error module(vast::module mod) override;

  vast::module module() const override;

  const char* name() const override;

  vast::system::report status() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

private:
  using iterator_type = std::string_view::const_iterator;

  std::unique_ptr<selector> selector_;
  std::string reader_name_ = "json-reader";

  std::unique_ptr<std::istream> input_;

  // https://simdjson.org/api/0.7.0/classsimdjson_1_1dom_1_1parser.html
  // Parser is designed to be reused.
  ::simdjson::dom::parser json_parser_;

  std::unique_ptr<detail::line_range> lines_;
  std::optional<size_t> proto_field_;
  std::vector<size_t> port_fields_;
  mutable size_t num_invalid_lines_ = 0;
  mutable size_t num_unknown_layouts_ = 0;
  mutable size_t num_lines_ = 0;
};

} // namespace vast::format::json
