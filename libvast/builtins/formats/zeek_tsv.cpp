//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/vast/pipeline.hpp"
#include "vast/concept/printable/string/escape.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/zeekify.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/module.hpp"
#include "vast/table_slice_builder.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/plugin.hpp>
#include <vast/view.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>

namespace vast::plugins::zeek {

namespace {
// The type name prefix to preprend to zeek log names when transleting them
// into VAST types.
constexpr std::string_view type_name_prefix = "zeek.";

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

  result_type operator()(const double_type&) const {
    return parsers::real->*[](double x) {
      return x;
    };
  }

  result_type operator()(const int64_type&) const {
    return parsers::i64->*[](int64_t x) {
      return int64_t{x};
    };
  }

  result_type operator()(const uint64_type&) const {
    return parsers::u64->*[](uint64_t x) {
      return x;
    };
  }

  result_type operator()(const time_type&) const {
    return parsers::real->*[](double x) {
      auto i = std::chrono::duration_cast<duration>(double_seconds(x));
      return time{i};
    };
  }

  result_type operator()(const duration_type&) const {
    return parsers::real->*[](double x) {
      return std::chrono::duration_cast<duration>(double_seconds(x));
    };
  }

  result_type operator()(const string_type&) const {
    if (set_separator_.empty())
      return +parsers::any->*[](std::string x) {
        return detail::byte_unescape(x);
      };
    else
      return +(parsers::any - set_separator_)->*[](std::string x) {
        return detail::byte_unescape(x);
      };
  }

  result_type operator()(const ip_type&) const {
    return parsers::ip->*[](ip x) {
      return x;
    };
  }

  result_type operator()(const subnet_type&) const {
    return parsers::net->*[](subnet x) {
      return x;
    };
  }

  result_type operator()(const list_type& t) const {
    return (caf::visit(*this, t.value_type()) % set_separator_)
             ->*[](std::vector<Attribute> x) {
                   return list(std::move(x));
                 };
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

// Creates a VAST type from an ASCII Zeek type in a log header.
auto parse_type(std::string_view zeek_type) -> caf::expected<type> {
  type t;
  if (zeek_type == "enum" || zeek_type == "string" || zeek_type == "file"
      || zeek_type == "pattern")
    t = type{string_type{}};
  else if (zeek_type == "bool")
    t = type{bool_type{}};
  else if (zeek_type == "int")
    t = type{int64_type{}};
  else if (zeek_type == "count")
    t = type{uint64_type{}};
  else if (zeek_type == "double")
    t = type{double_type{}};
  else if (zeek_type == "time")
    t = type{time_type{}};
  else if (zeek_type == "interval")
    t = type{duration_type{}};
  else if (zeek_type == "addr")
    t = type{ip_type{}};
  else if (zeek_type == "subnet")
    t = type{subnet_type{}};
  else if (zeek_type == "port")
    // FIXME: once we ship with builtin type aliases, we should reference the
    // port alias type here. Until then, we create the alias manually.
    // See also:
    // - src/format/pcap.cpp
    t = type{"port", uint64_type{}};
  if (!t
      && (zeek_type.starts_with("vector") || zeek_type.starts_with("set")
          || zeek_type.starts_with("table"))) {
    // Zeek's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = zeek_type.find('[');
    auto close = zeek_type.rfind(']');
    if (open == std::string::npos || close == std::string::npos)
      return caf::make_error(ec::format_error, "missing container brackets:",
                             std::string{zeek_type});
    auto elem = parse_type(zeek_type.substr(open + 1, close - open - 1));
    if (!elem)
      return elem.error();
    // Zeek sometimes logs sets as tables, e.g., represents set[string] as
    // table[string]. In VAST, they are all lists.
    t = type{list_type{*elem}};
  }
  if (!t)
    return caf::make_error(ec::format_error,
                           "failed to parse type: ", std::string{zeek_type});
  return t;
}

auto to_lines(generator<chunk_ptr> input) -> generator<std::string_view> {
  auto buffer = std::string{};
  bool ended_on_linefeed = false;
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield {};
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    if (ended_on_linefeed && *begin == '\n') {
      ++begin;
    };
    ended_on_linefeed = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' && *current != '\r') {
        continue;
      }
      if (buffer.empty()) {
        co_yield {begin, current};
      } else {
        buffer.append(begin, current);
        co_yield buffer;
        buffer.clear();
      }
      if (*current == '\r') {
        auto next = current + 1;
        if (next == end) {
          ended_on_linefeed = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer.append(begin, end);
    co_yield {};
  }
  if (!buffer.empty()) {
    co_yield std::move(buffer);
  }
}
} // namespace

class plugin : public virtual parser_plugin, public virtual printer_plugin {
public:
  using iterator_type = std::string_view::const_iterator;
  auto
  make_parser(std::span<std::string const> args, generator<chunk_ptr> loader,
              operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    return std::invoke(
      [](generator<std::string_view> lines,
         operator_control_plane& ctrl) -> generator<table_slice> {
        // Parse header.
        auto it = lines.begin();
        auto header = std::string_view{};
        while (it != lines.end()) {
          header = *it;
          if (!header.empty())
            break;
          co_yield {};
        }
        if (header.empty())
          co_return;

        auto sep_parser
          = "#separator" >> ignore(+(parsers::space)) >> +(parsers::any);
        auto separator_option = std::string{};

        if (not sep_parser(*it, separator_option)
            or not separator_option.starts_with("\\x")) {
          die("WTF NO SEP!!!!!!");
          // ctrl.abort(caf::make_error(ec::syntax_error, fmt::format("")))
          co_return;
        }
        auto sep = std::string{};
        sep.push_back(std::stoi(separator_option.substr(2, 2), nullptr, 16));
        // VAST_ASSERT(sep >= 0 && sep <= 255);
        auto prefix_options = std::array<std::string_view, 7>{
          "#set_separator", "#empty_field", "#unset_field", "#path",
          "#open",          "#fields",      "#types",
        };
        auto parsed_options = std::vector<std::string>{};

        for (const auto& prefix : prefix_options) {
          ++it;
          if (it == lines.end()) {
            die("END TOO EARLY WTF");
          }
          header = *it;
          auto pos = header.find(prefix);
          if (pos != 0)
            die("invalid header line, expected");
          pos = header.find(sep);
          if (pos == std::string::npos)
            die("invalid separator");
          if (pos + 1 >= header.size())
            die("missing header content");
          parsed_options.emplace_back(header.substr(pos + 1));
        }
        VAST_ERROR(parsed_options);
        auto set_separator = std::string{parsed_options[0][0]};
        auto empty_field = parsed_options[1];
        auto unset_field = parsed_options[2];
        auto path = parsed_options[3];
        auto open = parsed_options[4];
        auto fields = detail::split(parsed_options[5], sep);
        auto types = detail::split(parsed_options[6], sep);
        VAST_ERROR("fields: {}", fields);
        VAST_ERROR("types: {}", types);
        if (fields.size() != types.size())
          die("SIZE MISMATCH :(");
        std::vector<struct record_type::field> record_fields;
        for (auto i = 0u; i < fields.size(); ++i) {
          auto t = parse_type(types[i]);
          if (!t)
            die("t.error()");
          record_fields.push_back({
            std::string{fields[i]},
            *t,
          });
        }
        auto name = std::string{type_name_prefix} + path;
        auto schema = type{};
        // If a congruent type exists in the module, we give the type in the
        // module precedence.
        auto record_schema = record_type{record_fields};
        record_schema = detail::zeekify(record_schema);
        for (const auto& ctrl_schema : ctrl.schemas()) {
          if (ctrl_schema.name() == name) {
            schema = ctrl_schema;
          }
        }
        if (not schema) {
          schema = type{name, record_schema};
        }
        // Create Zeek parsers.
        auto make_parser = [](const auto& type, const auto& set_sep) {
          return make_zeek_parser<iterator_type>(type, set_sep);
        };
        std::vector<rule<iterator_type, data>> parsers_(
          record_schema.num_fields());
        parsers_.resize(record_schema.num_fields());
        for (size_t i = 0; i < record_schema.num_fields(); i++)
          parsers_[i] = make_parser(record_schema.field(i).type, set_separator);

        VAST_ERROR("name: {}", name);
        VAST_ERROR("schema: {}", schema);
        auto split_parser = ((+(parsers::any - sep)) % sep);
        auto b = table_slice_builder{schema};
        auto is_unset = [&](auto i) {
          return std::equal(unset_field.begin(), unset_field.end(),
                            fields[i].begin(), fields[i].end());
        };
        auto is_empty = [&](auto i) {
          return std::equal(empty_field.begin(), empty_field.end(),
                            fields[i].begin(), fields[i].end());
        };
        ++it;
        auto closed = false;
        for (; it != lines.end(); ++it) {
          auto line = *it;
          VAST_ERROR(line);
          if (line.empty()) {
            co_yield {};
            continue;
          }
          if (line.starts_with("#close")) {
            if (closed)
              die("redundant closing");
            closed = true;
            co_yield {};
            continue;
          }
          if (line.starts_with("#separator")) {
            if (not closed)
              die("previous log is not closed");
            closed = false;
            // reset header options.
          }
          VAST_ERROR("parsing");
          auto values = std::vector<std::string>{};
          if (!split_parser(*it, values)) {
            ctrl.warn(caf::make_error(
              ec::parse_error, fmt::format("zeek tsv parser skipped line: "
                                           "parsing line failed")));
            continue;
          }
          if (values.size() != fields.size()) {
            ctrl.warn(caf::make_error(
              ec::parse_error, fmt::format("zeek tsv parser skipped line: "
                                           "expected {} fields but got "
                                           "{}",
                                           fields.size(), values.size())));
            continue;
          }
          VAST_ERROR("adding row");
          for (auto i = size_t{0}; i << values.size(); ++i) {
            auto value = values[i];
            auto added = false;
            if (is_unset(i))
              added = b.add(caf::none);
            else if (is_empty(i))
              added = b.add(
                caf::get<record_type>(schema).field(i).type.construct());
            else if (!parsers_[i](fields[i], values[i]))
              die("failed to parse");
            added = b.add(value);
            VAST_ERROR("{} added", value);
            if (!added)
              die("die?");
          }
          VAST_ERROR("finish");
          co_yield b.finish();
        }
      },
      to_lines(std::move(loader)), ctrl);
  }

  auto default_loader([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdin", {}};
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane&) const
    -> caf::expected<printer> override {
    auto input_type = caf::get<record_type>(input_schema);
    return [input_type = std::move(input_type)](
             table_slice slice) -> generator<chunk_ptr> {

    };
  }

  auto default_saver(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"directory", {"."}};
  }

  auto printer_allows_joining() const -> bool override {
    return false;
  };

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "zeek_tsv";
  }
};

} // namespace vast::plugins::zeek

VAST_REGISTER_PLUGIN(vast::plugins::zeek::plugin)
