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

#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/parseable/vast/json.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/flat_map.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/format/multi_layout_reader.hpp"
#include "vast/format/ostream_writer.hpp"
#include "vast/fwd.hpp"
#include "vast/json.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <chrono>

namespace vast::format::json {

class writer : public ostream_writer {
public:
  using defaults = vast::defaults::export_::json;

  using super = ostream_writer;

  using super::super;

  caf::error write(const table_slice& x) override;

  const char* name() const override;
};

/// Adds a JSON object to a table slice builder according to a given layout.
/// @param builder The builder to add the JSON object to.
/// @param xs The JSON object to add to *builder.
/// @param layout The record type describing *xs*.
/// @returns An error iff the operation failed.
caf::error add(table_slice_builder& builder, const vast::json::object& xs,
               const record_type& layout);

/// @relates reader
struct default_selector {
  caf::optional<record_type> operator()(const vast::json::object& obj) const {
    if (type_cache.empty())
      return caf::none;
    // Iff there is only one type in the type cache, allow the JSON reader to
    // use it despite not being an exact match.
    if (type_cache.size() == 1)
      return type_cache.begin()->second;
    std::vector<std::string> cache_entry;
    auto build_cache_entry = [&cache_entry](auto& prefix, const vast::json&) {
      cache_entry.emplace_back(detail::join(prefix.begin(), prefix.end(), "."));
    };
    each_field(vast::json{obj}, build_cache_entry);
    std::sort(cache_entry.begin(), cache_entry.end());
    if (auto search_result = type_cache.find(cache_entry);
        search_result != type_cache.end())
      return search_result->second;
    return caf::none;
  }

  caf::error schema(vast::schema sch) {
    if (sch.empty())
      return make_error(ec::invalid_configuration, "no schema provided or type "
                                                   "too restricted");
    for (auto& entry : sch) {
      if (!caf::holds_alternative<record_type>(entry))
        continue;
      auto layout = flatten(caf::get<record_type>(entry));
      std::vector<std::string> cache_entry;
      for (auto& [k, v] : layout.fields)
        cache_entry.emplace_back(k);
      std::sort(cache_entry.begin(), cache_entry.end());
      type_cache[cache_entry] = layout;
    }
    return caf::none;
  }

  vast::schema schema() const {
    vast::schema result;
    for (const auto& [k, v] : type_cache)
      result.add(v);
    return result;
  }

  static const char* name() {
    return "json-reader";
  }

  detail::flat_map<std::vector<std::string>, record_type> type_cache = {};
};

/// A reader for JSON data. It operates with a *selector* to determine the
/// mapping of JSON object to the appropriate record type in the schema.
template <class Selector = default_selector>
class reader final : public multi_layout_reader {
public:
  using super = multi_layout_reader;

  /// Constructs a JSON reader.
  /// @param table_slice_type The ID for table slice type to build.
  /// @param options Additional options.
  /// @param in The stream of JSON objects.
  reader(caf::atom_value table_slice_type, const caf::settings& options,
         std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

  vast::system::report status() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  using iterator_type = std::string_view::const_iterator;

  Selector selector_;
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  caf::optional<size_t> proto_field_;
  std::vector<size_t> port_fields_;
  mutable size_t num_invalid_lines_ = 0;
  mutable size_t num_unknown_layouts_ = 0;
  mutable size_t num_lines_ = 0;
};

// -- implementation ----------------------------------------------------------

template <class Selector>
reader<Selector>::reader(caf::atom_value table_slice_type,
                         const caf::settings& options,
                         std::unique_ptr<std::istream> in)
  : super(table_slice_type, options) {
  if (in != nullptr)
    reset(std::move(in));
}

template <class Selector>
void reader<Selector>::reset(std::unique_ptr<std::istream> in) {
  VAST_ASSERT(in != nullptr);
  input_ = std::move(in);
  lines_ = std::make_unique<detail::line_range>(*input_);
}

template <class Selector>
caf::error reader<Selector>::schema(vast::schema s) {
  return selector_.schema(std::move(s));
}

template <class Selector>
vast::schema reader<Selector>::schema() const {
  return selector_.schema();
}

template <class Selector>
const char* reader<Selector>::name() const {
  return Selector::name();
}

template <class Selector>
vast::system::report reader<Selector>::status() const {
  using namespace std::string_literals;
  uint64_t invalid_line = num_invalid_lines_;
  uint64_t unknown_layout = num_unknown_layouts_;
  if (num_invalid_lines_ > 0)
    VAST_WARNING(this, "failed to parse", num_invalid_lines_, "of", num_lines_,
                 "recent lines");
  if (num_unknown_layouts_ > 0)
    VAST_WARNING(this, "failed to find a matching type for",
                 num_unknown_layouts_, "of", num_lines_, "recent lines");
  num_invalid_lines_ = 0;
  num_unknown_layouts_ = 0;
  num_lines_ = 0;
  return {
    {name() + ".invalid-line"s, invalid_line},
    {name() + ".unknown-layout"s, unknown_layout},
  };
}

template <class Selector>
caf::error reader<Selector>::read_impl(size_t max_events, size_t max_slice_size,
                                       consumer& cons) {
  VAST_TRACE("json-reader", VAST_ARG(max_events), VAST_ARG(max_slice_size));
  VAST_ASSERT(max_events > 0);
  VAST_ASSERT(max_slice_size > 0);
  size_t produced = 0;
  table_slice_builder_ptr bptr = nullptr;
  while (produced < max_events) {
    if (lines_->done())
      return finish(cons, make_error(ec::end_of_input, "input exhausted"));
    if (produced > 0 && batch_timeout_ > reader_clock::duration::zero()
        && last_batch_sent_ + batch_timeout_ < reader_clock::now()) {
      VAST_DEBUG(this, "reached input timeout");
      break;
    }
    bool timed_out = lines_->next_timeout(read_timeout_);
    if (timed_out) {
      VAST_DEBUG(this, "stalled at line", lines_->line_number());
      return ec::stalled;
    }
    auto& line = lines_->get();
    ++num_lines_;
    if (line.empty()) {
      // Ignore empty lines.
      VAST_DEBUG(this, "ignores empty line at", lines_->line_number());
      continue;
    }
    vast::json j;
    if (!parsers::json(line, j)) {
      if (num_invalid_lines_ == 0)
        VAST_WARNING(this, "failed to parse line", lines_->line_number(), ":",
                     line);
      ++num_invalid_lines_;
      continue;
    }
    auto xs = caf::get_if<vast::json::object>(&j);
    if (!xs)
      return make_error(ec::type_clash, "not a json object");
    auto layout = selector_(*xs);
    if (!layout) {
      if (num_unknown_layouts_ == 0)
        VAST_WARNING(this, "failed to find a matching type at line",
                     lines_->line_number(), ":", line);
      ++num_unknown_layouts_;
      continue;
    }
    bptr = builder(*layout);
    if (bptr == nullptr)
      return make_error(ec::parse_error, "unable to get a builder");
    if (auto err = add(*bptr, *xs, *layout)) {
      err.context() += caf::make_message("line", lines_->line_number());
      return finish(cons, err);
    }
    produced++;
    if (bptr->rows() == max_slice_size)
      if (auto err = finish(cons, bptr))
        return err;
  }
  return finish(cons);
}

} // namespace vast::format::json
