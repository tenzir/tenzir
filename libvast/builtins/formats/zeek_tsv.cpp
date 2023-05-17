//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/arrow_table_slice.hpp"
#include "vast/cast.hpp"
#include "vast/concept/parseable/string/any.hpp"
#include "vast/concept/parseable/vast/option_set.hpp"
#include "vast/concept/parseable/vast/pipeline.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/data.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/string_literal.hpp"
#include "vast/detail/to_xsv_sep.hpp"
#include "vast/detail/zeekify.hpp"
#include "vast/generator.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/to_lines.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/none.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>
#include <string_view>

namespace vast::plugins::zeek_tsv {

namespace {
// The type name prefix to prepend to Zeek log names when translating them
// into VAST types.
constexpr std::string_view type_name_prefix = "zeek.";
constexpr auto default_sep = '\x09';
constexpr auto default_set_sep = ',';
constexpr std::string_view default_empty_val = "(empty)";
constexpr std::string_view default_unset_val = "-";
constexpr auto set_separator_option = "set-separator";
constexpr auto empty_field_option = "empty-field";
constexpr auto unset_field_option = "unset-field";

/// Constructs a polymorphic Zeek data parser.
template <class Iterator, class Attribute>
struct zeek_parser_factory {
  using result_type = rule<Iterator, Attribute>;

  explicit zeek_parser_factory(const std::string& set_sep) : set_sep_{set_sep} {
  }

  template <class T>
  auto operator()(const T&) const -> result_type {
    return {};
  }

  auto operator()(const bool_type&) const -> result_type {
    return parsers::tf;
  }

  auto operator()(const double_type&) const -> result_type {
    return parsers::real->*[](double x) {
      return x;
    };
  }

  auto operator()(const int64_type&) const -> result_type {
    return parsers::i64->*[](int64_t x) {
      return int64_t{x};
    };
  }

  auto operator()(const uint64_type&) const -> result_type {
    return parsers::u64->*[](uint64_t x) {
      return x;
    };
  }

  auto operator()(const time_type&) const -> result_type {
    return parsers::real->*[](double x) {
      auto i = std::chrono::duration_cast<duration>(double_seconds(x));
      return time{i};
    };
  }

  auto operator()(const duration_type&) const -> result_type {
    return parsers::real->*[](double x) {
      return std::chrono::duration_cast<duration>(double_seconds(x));
    };
  }

  auto operator()(const string_type&) const -> result_type {
    if (set_sep_.empty())
      return +parsers::any->*[](std::string x) {
        return detail::byte_unescape(x);
      };
    return +(parsers::any - set_sep_)->*[](std::string x) {
      return detail::byte_unescape(x);
    };
  }

  auto operator()(const ip_type&) const -> result_type {
    return parsers::ip->*[](ip x) {
      return x;
    };
  }

  auto operator()(const subnet_type&) const -> result_type {
    return parsers::net->*[](subnet x) {
      return x;
    };
  }

  auto operator()(const list_type& t) const -> result_type {
    return (caf::visit(*this, t.value_type()) % set_sep_)
             ->*[](std::vector<Attribute> x) {
                   return list(std::move(x));
                 };
  }

  const std::string& set_sep_;
};

/// Constructs a Zeek data parser from a type and set separator.
template <class Iterator, class Attribute = data>
auto make_zeek_parser(const type& t, const std::string& set_sep = ",")
  -> rule<Iterator, Attribute> {
  rule<Iterator, Attribute> r;
  auto sep = is_container(t) ? set_sep : "";
  return caf::visit(zeek_parser_factory<Iterator, Attribute>{sep}, t);
}

// Creates a VAST type from an ASCII Zeek type in a log header.
auto parse_type(std::string_view zeek_type) -> caf::expected<type> {
  type t;
  if (zeek_type == "enum" or zeek_type == "string" or zeek_type == "file"
      or zeek_type == "pattern")
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
      && (zeek_type.starts_with("vector") or zeek_type.starts_with("set")
          or zeek_type.starts_with("table"))) {
    // Zeek's logging framwork cannot log nested vectors/sets/tables, so we can
    // safely assume that we're dealing with a basic type inside the brackets.
    // If this will ever change, we'll have to enhance this simple parser.
    auto open = zeek_type.find('[');
    auto close = zeek_type.rfind(']');
    if (open == std::string::npos or close == std::string::npos)
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

struct zeek_metadata {
  using iterator_type = std::string_view::const_iterator;

  auto is_unset(std::string_view field) -> bool {
    return std::equal(unset_field.begin(), unset_field.end(), field.begin(),
                      field.end());
  }

  auto is_empty(std::string_view field) -> bool {
    return std::equal(empty_field.begin(), empty_field.end(), field.begin(),
                      field.end());
  }

  auto parse_header(generator<std::optional<std::string_view>>::iterator it,
                    const generator<std::optional<std::string_view>>& lines,
                    operator_control_plane& ctrl) -> caf::expected<void> {
    auto sep_parser
      = "#separator" >> ignore(+(parsers::space)) >> +(parsers::any);
    auto sep_option = std::string{};
    if (not sep_parser((*it).value(), sep_option)
        or not sep_option.starts_with("\\x")) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("zeek-tsv parser failed: invalid "
                                         "#separator option encountered"));
    }
    auto sep_char = std::stoi(sep_option.substr(2, 2), nullptr, 16);
    VAST_ASSERT(sep_char >= 0 && sep_char <= 255);
    if (not sep.empty())
      sep.clear();
    sep.push_back(sep_char);
    auto prefix_options = std::array<std::string_view, 7>{
      "#set_separator", "#empty_field", "#unset_field", "#path",
      "#open",          "#fields",      "#types",
    };
    auto parsed_options = std::vector<std::string>{};
    auto header = std::string{};
    for (const auto& prefix : prefix_options) {
      ++it;
      if (not *it or it == lines.end()) {
        return caf::make_error(ec::syntax_error,
                               fmt::format(
                                 "zeek-tsv parser failed: header ended too "
                                 "early"));
      }
      header = (*it).value();
      auto pos = header.find(prefix);
      if (pos != 0)
        return caf::make_error(ec::syntax_error,
                               fmt::format("zeek-tsv parser encountered "
                                           "invalid header line: prefix '{}' "
                                           "not found at beginning of line",
                                           prefix));
      pos = header.find(sep);
      if (pos == std::string::npos)
        return caf::make_error(ec::syntax_error,
                               fmt::format("zeek-tsv parser encountered "
                                           "invalid header line: separator "
                                           "'{}' not found",
                                           sep_option));
      if (pos + 1 >= header.size())
        return caf::make_error(
          ec::syntax_error, fmt::format("zeek-tsv detected missing header line "
                                        "content: {}",
                                        header));
      parsed_options.emplace_back(header.substr(pos + 1));
    }
    set_sep = std::string{parsed_options[0][0]};
    empty_field = parsed_options[1];
    unset_field = parsed_options[2];
    path = parsed_options[3];
    fields_str = parsed_options[5];
    types_str = parsed_options[6];
    fields = detail::split(fields_str, sep);
    types = detail::split(types_str, sep);
    if (fields.size() != types.size())
      return caf::make_error(ec::syntax_error,
                             fmt::format("zeek-tsv parser detected header "
                                         "types mismatch: "
                                         "expected {} fields but got {}",
                                         fields.size(), types.size()));

    std::vector<struct record_type::field> record_fields;
    for (auto i = size_t{0}; i < fields.size(); ++i) {
      auto t = parse_type(types[i]);
      if (!t)
        return std::move(t.error());
      record_fields.push_back({
        std::string{fields[i]},
        *t,
      });
    }
    name = std::string{type_name_prefix} + path;
    // If a congruent type exists in the module, we give the type in the
    // module precedence.
    auto record_schema = record_type{record_fields};
    record_schema = detail::zeekify(record_schema);
    output_slice_schema = {};
    for (const auto& ctrl_schema : ctrl.schemas()) {
      if (ctrl_schema.name() == name) {
        auto is_castable = can_cast(record_schema, ctrl_schema);
        if (!is_castable) {
          VAST_WARN("zeek-tsv parser ignores incompatible schema '{}' from "
                    "schema files: {}",
                    ctrl_schema, is_castable.error());
        } else {
          output_slice_schema = ctrl_schema;
          break;
        }
      }
    }
    temp_slice_schema = type{name, record_schema};
    // Create Zeek parsers.
    auto make_parser = [](const auto& type, const auto& set_sep) {
      return make_zeek_parser<iterator_type>(type, set_sep);
    };
    parsers.resize(record_schema.num_fields());
    for (size_t i = 0; i < record_schema.num_fields(); i++)
      parsers[i] = make_parser(record_schema.field(i).type, set_sep);
    return {};
  }

  std::string sep{};
  std::string set_sep{};
  std::string empty_field{};
  std::string unset_field{};
  std::string path{};
  std::string fields_str{};
  std::string types_str{};
  std::vector<std::string_view> fields{};
  std::vector<std::string_view> types{};
  std::string name{};
  type output_slice_schema{};
  type temp_slice_schema{};
  std::vector<rule<iterator_type, data>> parsers{};
};

struct zeek_printer {
  zeek_printer(char set_sep, std::string_view empty = "",
               std::string_view unset = "", bool disable_timestamp_tags = false)
    : set_sep{set_sep},
      empty_field{empty},
      unset_field{unset},
      disable_timestamp_tags{disable_timestamp_tags} {
  }

  auto to_zeek_string(const type& t) const -> std::string {
    auto f = detail::overload{
      [](const bool_type&) -> std::string {
        return "bool";
      },
      [](const int64_type&) -> std::string {
        return "int";
      },
      [&](const uint64_type&) -> std::string {
        return t.name() == "port" ? "port" : "count";
      },
      [](const double_type&) -> std::string {
        return "double";
      },
      [](const duration_type&) -> std::string {
        return "interval";
      },
      [](const time_type&) -> std::string {
        return "time";
      },
      [](const string_type&) -> std::string {
        return "string";
      },
      [](const ip_type&) -> std::string {
        return "addr";
      },
      [](const subnet_type&) -> std::string {
        return "subnet";
      },
      [](const enumeration_type&) -> std::string {
        return "enumeration";
      },
      [this](const list_type& lt) -> std::string {
        return fmt::format("vector[{}]", to_zeek_string(lt.value_type()));
      },
      [](const map_type&) -> std::string {
        return "map";
      },
      [](const record_type&) -> std::string {
        return "record";
      },
    };
    return t ? caf::visit(f, t) : "none";
  }

  auto generate_timestamp() const -> std::string {
    auto now = std::chrono::system_clock::now();
    return fmt::format(timestamp_format, now);
  }

  template <typename It>
  auto print_header(It& out, const type& t) const noexcept -> bool {
    auto header = fmt::format("#separator \\x{0:02x}\n"
                              "#set_separator{0}{1}\n"
                              "#empty_field{0}{2}\n"
                              "#unset_field{0}{3}\n"
                              "#path{0}{4}",
                              sep, set_sep, empty_field, unset_field, t.name());
    if (not disable_timestamp_tags)
      header.append(fmt::format("\n#open{}{}", sep, generate_timestamp()));
    header.append("\n#fields");
    auto r = caf::get<record_type>(t);
    for (const auto& [_, offset] : r.leaves())
      header.append(fmt::format("{}{}", sep, to_string(r.key(offset))));
    header.append("\n#types");
    for (const auto& [field, _] : r.leaves())
      header.append(fmt::format("{}{}", sep, to_zeek_string(field.type)));
    out = std::copy(header.begin(), header.end(), out);
    return true;
  }

  template <typename It>
  auto print_values(It& out, const view<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (not first) {
        ++out = sep;
      } else {
        first = false;
      }
      caf::visit(visitor{out, *this}, v);
    }
    return true;
  }

  template <typename It>
  auto print_closing_line(It& out) const noexcept -> void {
    if (not disable_timestamp_tags) {
      out = fmt::format_to(out, "#close{}{}\n", sep, generate_timestamp());
    }
  }

  template <class Iterator>
  struct visitor {
    visitor(Iterator& out, const zeek_printer& printer)
      : out{out}, printer{printer} {
    }

    auto operator()(caf::none_t) noexcept -> bool {
      out = std::copy(printer.unset_field.begin(), printer.unset_field.end(),
                      out);
      return true;
    }

    auto operator()(auto x) noexcept -> bool {
      make_printer<decltype(x)> p;
      return p.print(out, x);
    }

    auto operator()(view<bool> x) noexcept -> bool {
      return printers::any.print(out, x ? 'T' : 'F');
    }

    auto operator()(view<pattern>) noexcept -> bool {
      die("unreachable");
    }

    auto operator()(view<map>) noexcept -> bool {
      die("unreachable");
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      if (x.empty()) {
        out = std::copy(printer.unset_field.begin(), printer.unset_field.end(),
                        out);
        return true;
      }
      for (auto c : x)
        if (std::iscntrl(c) or c == printer.sep or c == printer.set_sep) {
          auto hex = detail::byte_to_hex(c);
          *out++ = '\\';
          *out++ = 'x';
          *out++ = hex.first;
          *out++ = hex.second;
        } else {
          *out++ = c;
        }
      return true;
    }

    auto operator()(const view<list>& x) noexcept -> bool {
      if (x.empty()) {
        out = std::copy(printer.empty_field.begin(), printer.empty_field.end(),
                        out);
        return true;
      }
      auto first = true;
      for (const auto& v : x) {
        if (not first) {
          ++out = printer.set_sep;
        } else {
          first = false;
        }
        caf::visit(*this, v);
      }
      return true;
    }

    auto operator()(const view<record>& x) noexcept -> bool {
      // TODO: This won't be needed when flatten() for table_slices is in the
      // codebase.
      VAST_WARN("printing records as zeek-tsv data is currently a work in "
                "progress; printing null instead");
      auto first = true;
      for (const auto& v : x) {
        if (not first) {
          ++out = printer.sep;
        } else {
          first = false;
        }
        (*this)(caf::none);
      }
      return true;
    }

    Iterator& out;
    const zeek_printer& printer;
  };

  static constexpr auto timestamp_format{"{:%Y-%m-%d-%H-%M-%S}"};
  char sep{'\t'};
  char set_sep{','};
  std::string empty_field{};
  std::string unset_field{};
  bool disable_timestamp_tags{false};
};
} // namespace

class plugin : public virtual parser_plugin, public virtual printer_plugin {
public:
  auto
  make_parser([[maybe_unused]] std::vector<std::string> args,
              generator<chunk_ptr> loader, operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         operator_control_plane& ctrl) -> generator<table_slice> {
        // Parse header.
        auto it = lines.begin();
        while (it != lines.end()) {
          auto header_line = *it;
          if (header_line and not header_line->empty())
            // Encountered the first non-empty header line.
            break;
          co_yield {};
          if (header_line and header_line->empty())
            ++it;
        }
        if (it == lines.end()) {
          // Empty input.
          co_return;
        }
        auto metadata = zeek_metadata{};
        auto parsed = metadata.parse_header(it, lines, ctrl);
        if (not parsed) {
          ctrl.abort(parsed.error());
          VAST_ERROR("aborting Zeek metadata parsing");
          co_return;
        }
        ++it;
        auto closed = false;
        auto b = table_slice_builder{metadata.temp_slice_schema};
        std::vector<data> xs(metadata.fields.size());
        for (; it != lines.end(); ++it) {
          auto line = *it;
          if (not line) {
            co_yield {};
            continue;
          }
          if (line->empty()) {
            VAST_DEBUG("zeek-tsv parser ignored empty line");
            continue;
          }
          if (line->starts_with("#close")) {
            if (closed) {
              ctrl.abort(caf::make_error(ec::syntax_error,
                                         fmt::format("zeek-tsv parser failed: "
                                                     "#close without previous "
                                                     "#open header found")));
              co_return;
            }
            closed = true;
            co_yield {};
            continue;
          }
          if (line->starts_with("#separator")) {
            if (not closed) {
              ctrl.abort(caf::make_error(
                ec::syntax_error, fmt::format("zeek-tsv parser failed: "
                                              "previous logs are still open")));
              co_return;
            }
            closed = false;
            auto finished = b.finish();
            if (metadata.output_slice_schema)
              finished
                = cast(std::move(finished), metadata.output_slice_schema);
            co_yield std::move(finished);
            auto parsed = metadata.parse_header(it, lines, ctrl);
            if (not parsed) {
              ctrl.abort(parsed.error());
              co_return;
            }
            xs.resize(metadata.fields.size());
            b = table_slice_builder{metadata.temp_slice_schema};
            ++it;
          }
          if (closed) {
            ctrl.abort(caf::make_error(ec::syntax_error,
                                       fmt::format("zeek-tsv parser failed: "
                                                   "additional data after "
                                                   "#close should be "
                                                   "preceded by Zeek header")));
            co_return;
          }
          auto values = detail::split((*it).value(), metadata.sep);
          if (values.size() != metadata.fields.size()) {
            ctrl.warn(caf::make_error(
              ec::parse_error,
              fmt::format("zeek-tsv parser skipped line: "
                          "expected {} fields but got "
                          "{}",
                          metadata.fields.size(), values.size())));
            continue;
          }
          for (auto i = size_t{0}; i < values.size(); ++i) {
            auto value = values[i];
            if (metadata.is_unset(value)) {
              xs[i] = caf::none;
            } else if (metadata.is_empty(value)) {
              xs[i] = caf::get<record_type>(metadata.temp_slice_schema)
                        .field(i)
                        .type.construct();
            } else if (!metadata.parsers[i](values[i], xs[i])) {
              ctrl.warn(caf::make_error(ec::parse_error,
                                        fmt::format("zeek-tsv parser skipped "
                                                    "line: failed"
                                                    "to parse value '{}'",
                                                    value)));
              continue;
            }
          }
          for (auto i = size_t{0}; i < xs.size(); ++i) {
            auto&& x = xs[i];
            if (not b.add(make_data_view(x))) {
              ctrl.abort(caf::make_error(ec::parse_error,
                                         fmt::format("zeek-tsv parser failed "
                                                     "to finalize value '{}'",
                                                     x)));
              co_return;
            }
          }
        }
        auto finished = b.finish();
        if (metadata.output_slice_schema)
          finished = cast(std::move(finished), metadata.output_slice_schema);
        co_yield std::move(finished);
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
    auto set_sep = default_set_sep;
    auto empty_field = default_empty_val;
    auto unset_field = default_unset_val;
    auto options = option_set_parser{{{set_separator_option, 's'},
                                      {empty_field_option, 'e'},
                                      {unset_field_option, 'u'}}};
    auto parsed_options = std::unordered_map<std::string, data>{};
    for (std::string_view arg : args) {
      const auto* f = arg.begin();
      const auto* const l = arg.end();
      options(f, l, parsed_options);
    }
    if (parsed_options.contains(set_separator_option)) {
      auto* set_sep_option_val
        = caf::get_if<std::string>(&parsed_options[set_separator_option]);
      if (not set_sep_option_val)
        return caf::make_error(ec::invalid_argument,
                               "invalid set separator argument");
      auto parsed_set_sep = to_xsv_sep(*set_sep_option_val);
      if (not parsed_set_sep) {
        return std::move(parsed_set_sep.error());
      }
      if (*parsed_set_sep == '\t') {
        return caf::make_error(ec::invalid_argument,
                               "separator and set separator must be "
                               "different");
      }
      set_sep = *parsed_set_sep;
    }
    if (parsed_options.contains(empty_field_option)) {
      auto* empty_field_option_val
        = caf::get_if<std::string>(&parsed_options[empty_field_option]);
      if (not empty_field_option_val)
        return caf::make_error(ec::invalid_argument,
                               "invalid set separator argument");
      empty_field = *empty_field_option_val;
      auto conflict
        = std::any_of(empty_field.begin(), empty_field.end(), [&](auto ch) {
            return ch == default_sep or ch == set_sep;
          });
      if (conflict) {
        return caf::make_error(ec::invalid_argument, "empty field must not "
                                                     "contain separator or set "
                                                     "separator");
      }
    }
    if (parsed_options.contains(unset_field_option)) {
      auto* unset_field_option_val
        = caf::get_if<std::string>(&parsed_options[unset_field_option]);
      if (not unset_field_option_val)
        return caf::make_error(ec::invalid_argument,
                               "invalid set separator argument");
      unset_field = *unset_field_option_val;
      auto conflict
        = std::any_of(unset_field.begin(), unset_field.end(), [&](auto ch) {
            return ch == default_sep or ch == set_sep;
          });
      if (conflict) {
        return caf::make_error(ec::invalid_argument, "unset field must not "
                                                     "contain separator or set "
                                                     "separator");
      }
      if (empty_field == unset_field) {
        return caf::make_error(ec::invalid_argument,
                               "unset field must not be equal to empty "
                               "field");
      }
    }
    auto printer = zeek_printer{set_sep, empty_field, unset_field,
                                disable_timestamp_tags_};
    return to_printer(
      [printer = std::move(printer), input_schema = std::move(input_schema)](
        table_slice slice) -> generator<chunk_ptr> {
        auto input_type = caf::get<record_type>(input_schema);
        auto buffer = std::vector<char>{};
        auto out_iter = std::back_inserter(buffer);
        auto resolved_slice = resolve_enumerations(slice);
        auto array
          = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
        auto first = true;
        for (const auto& row : values(input_type, *array)) {
          VAST_ASSERT_CHEAP(row);
          if (first) {
            printer.print_header(out_iter, input_schema);
            first = false;
            out_iter = fmt::format_to(out_iter, "\n");
          }
          const auto ok = printer.print_values(out_iter, *row);
          VAST_ASSERT_CHEAP(ok);
          out_iter = fmt::format_to(out_iter, "\n");
        }
        printer.print_closing_line(out_iter);
        auto chunk = chunk::make(std::move(buffer));
        co_yield std::move(chunk);
      });
  }

  auto default_saver(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"directory", {"."}};
  }

  auto printer_allows_joining() const -> bool override {
    return false;
  };

  auto initialize(const record&, const record& global_config)
    -> caf::error override {
    disable_timestamp_tags_
      = get_or(global_config, "vast.export.zeek.disable-timestamp-tags",
               disable_timestamp_tags_);

    return caf::none;
  }

  auto name() const -> std::string override {
    return "zeek-tsv";
  }

private:
  bool disable_timestamp_tags_{false};
};

} // namespace vast::plugins::zeek_tsv

VAST_REGISTER_PLUGIN(vast::plugins::zeek_tsv::plugin)
