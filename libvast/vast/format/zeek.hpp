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

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/vast/address.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/data.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/detail/string.hpp"
#include "vast/format/ostream_writer.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/format/writer.hpp"
#include "vast/fwd.hpp"
#include "vast/path.hpp"
#include "vast/schema.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace vast::format::zeek {

/// Parses non-container types.
template <class Iterator, class Attribute>
struct zeek_parser {
  zeek_parser(Iterator& f, const Iterator& l, Attribute& attr)
    : f_{f},
      l_{l},
      attr_{attr} {
  }

  template <class Parser>
  bool parse(const Parser& p) const {
    return p(f_, l_, attr_);
  }

  template <class T>
  bool operator()(const T&) const {
    return false;
  }

  bool operator()(const bool_type&) const {
    return parse(parsers::tf);
  }

  bool operator()(const integer_type&) const {
    static auto p = parsers::i64 ->* [](integer x) { return x; };
    return parse(p);
  }

  bool operator()(const count_type&) const {
    static auto p = parsers::u64 ->* [](count x) { return x; };
    return parse(p);
  }

  bool operator()(const time_type&) const {
    static auto p = parsers::real->*[](real x) {
      auto i = std::chrono::duration_cast<duration>(double_seconds(x));
      return time{i};
    };
    return parse(p);
  }

  bool operator()(const duration_type&) const {
    static auto p = parsers::real->*[](real x) {
      return std::chrono::duration_cast<duration>(double_seconds(x));
    };
    return parse(p);
  }

  bool operator()(const string_type&) const {
    static auto p = +parsers::any
      ->* [](std::string x) { return detail::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(const pattern_type&) const {
    static auto p = +parsers::any
      ->* [](std::string x) { return detail::byte_unescape(x); };
    return parse(p);
  }

  bool operator()(const address_type&) const {
    static auto p = parsers::addr ->* [](address x) { return x; };
    return parse(p);
  }

  bool operator()(const subnet_type&) const {
    static auto p = parsers::net ->* [](subnet x) { return x; };
    return parse(p);
  }

  Iterator& f_;
  const Iterator& l_;
  Attribute& attr_;
};

/// Constructs a polymorphic Zeek data parser.
template <class Iterator, class Attribute>
struct zeek_parser_factory {
  using result_type = rule<Iterator, Attribute>;

  zeek_parser_factory(const std::string& set_separator)
    : set_separator_{set_separator} {
  }

  template <class T>
  result_type operator()(const T&) const {
    return {};
  }

  result_type operator()(const bool_type&) const {
    return parsers::tf;
  }

  result_type operator()(const real_type&) const {
    return parsers::real->*[](real x) { return x; };
  }

  result_type operator()(const integer_type&) const {
    return parsers::i64 ->* [](integer x) { return x; };
  }

  result_type operator()(const count_type&) const {
    return parsers::u64 ->* [](count x) { return x; };
  }

  result_type operator()(const time_type&) const {
    return parsers::real->*[](real x) {
      auto i = std::chrono::duration_cast<duration>(double_seconds(x));
      return time{i};
    };
  }

  result_type operator()(const duration_type&) const {
    return parsers::real->*[](real x) {
      return std::chrono::duration_cast<duration>(double_seconds(x));
    };
  }

  result_type operator()(const string_type&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return detail::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
               ->*[](std::string x) { return detail::byte_unescape(x); };
  }

  result_type operator()(const pattern_type&) const {
    if (set_separator_.empty())
      return +parsers::any
        ->* [](std::string x) { return detail::byte_unescape(x); };
    else
      return +(parsers::any - set_separator_)
        ->* [](std::string x) { return detail::byte_unescape(x); };
  }

  result_type operator()(const address_type&) const {
    return parsers::addr ->* [](address x) { return x; };
  }

  result_type operator()(const subnet_type&) const {
    return parsers::net ->* [](subnet x) { return x; };
  }

  result_type operator()(const list_type& t) const {
    return (caf::visit(*this, t.value_type) % set_separator_)
             ->*[](std::vector<Attribute> x) { return list(std::move(x)); };
  }

  const std::string& set_separator_;
};

/// Constructs a Zeek data parser from a type and set separator.
template <class Iterator, class Attribute = data>
rule<Iterator, Attribute>
make_zeek_parser(const type& t, const std::string& set_separator = ",") {
  rule<Iterator, Attribute> r;
  auto sep = is_container(t) ? set_separator : "";
  return caf::visit(zeek_parser_factory<Iterator, Attribute>{sep}, t);
}

/// Parses non-container Zeek data.
template <class Iterator, class Attribute = data>
bool zeek_basic_parse(const type& t, Iterator& f, const Iterator& l,
                     Attribute& attr) {
  return caf::visit(zeek_parser<Iterator, Attribute>{f, l, attr}, t);
}

/// A Zeek reader.
class reader final : public single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a Zeek reader.
  /// @param table_slice_type The ID for table slice type to build.
  /// @param options Additional options.
  /// @param input The stream of logs to read.
  reader(caf::atom_value table_slice_type, const caf::settings& options,
         std::unique_ptr<std::istream> in = nullptr);

  void reset(std::unique_ptr<std::istream> in);

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

protected:
  caf::error read_impl(size_t max_events, size_t max_slice_size,
                       consumer& f) override;

private:
  using iterator_type = std::string_view::const_iterator;

  caf::error parse_header();

  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  std::string separator_;
  std::string set_separator_;
  std::string empty_field_;
  std::string unset_field_;
  vast::schema schema_;
  type type_;
  record_type layout_;
  caf::optional<size_t> proto_field_;
  std::vector<rule<iterator_type, data>> parsers_;
};

/// A Zeek writer.
class writer : public format::writer {
public:
  using defaults = vast::defaults::export_::zeek;

  writer() = default;

  writer(writer&&) = default;

  writer& operator=(writer&&) = default;

  ~writer() override;

  /// Constructs a Zeek writer.
  /// @param dir The path where to write the log file(s) to.
  /// @param show_timestamp_tags Flag to control whether to include comments
  /// with `#open` and `#close` tags in the output. Setting this flag to
  /// `false` produces a deterministic output.
  writer(path dir, bool show_timestamp_tags);

  error write(const table_slice& e) override;

  caf::expected<void> flush() override;

  const char* name() const override;

private:
  path dir_;
  type previous_layout_;
  bool show_timestamp_tags_;

  /// One writer for each layout.
  std::unordered_map<std::string, ostream_writer_ptr> writers_;
};

} // namespace vast::format::zeek
