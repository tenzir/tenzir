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
#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/to_xsv_sep.hpp"
#include "vast/detail/zeekify.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/generator.hpp"
#include "vast/module.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/to_lines.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/plugin.hpp>
#include <vast/view.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>
#include <cctype>
#include <iterator>
#include <string>
#include <string_view>

namespace vast::plugins::zeek {

namespace {
// The type name prefix to preprend to zeek log names when transleting them
// into VAST types.
constexpr std::string_view type_name_prefix = "zeek.";
constexpr auto separator = '\x09';
constexpr auto default_set_sep = ',';
constexpr std::string_view default_empty_val = "(empty)";
constexpr std::string_view default_unset_val = "-";

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

struct zeek_metadata {
  using iterator_type = std::string_view::const_iterator;

  auto is_unset(auto i) -> bool {
    return std::equal(unset_field.begin(), unset_field.end(), fields[i].begin(),
                      fields[i].end());
  }

  auto is_empty(auto i) -> bool {
    return std::equal(empty_field.begin(), empty_field.end(), fields[i].begin(),
                      fields[i].end());
  }

  auto parse_header(generator<std::optional<std::string_view>>::iterator it,
                    const generator<std::optional<std::string_view>>& lines,
                    operator_control_plane& ctrl) -> caf::expected<void> {
    auto sep_parser
      = "#separator" >> ignore(+(parsers::space)) >> +(parsers::any);
    auto separator_option = std::string{};

    if (not sep_parser((*it).value(), separator_option)
        or not separator_option.starts_with("\\x")) {
      return caf::make_error(
        ec::syntax_error, fmt::format("Invalid #separator option while parsing "
                                      "Zeek TSV file - aborting"));
    }
    auto sep_char = std::stoi(separator_option.substr(2, 2), nullptr, 16);
    VAST_ASSERT(sep_char >= 0 && sep_char <= 255);
    sep.push_back(sep_char);
    auto prefix_options = std::array<std::string_view, 7>{
      "#set_separator", "#empty_field", "#unset_field", "#path",
      "#open",          "#fields",      "#types",
    };
    auto parsed_options = std::vector<std::string>{};
    auto header = std::string{};
    for (const auto& prefix : prefix_options) {
      ++it;
      if (it == lines.end()) {
        return caf::make_error(ec::syntax_error,
                               fmt::format("Zeek TSV file header ended too "
                                           "early - aborting"));
      }
      header = (*it).value();
      auto pos = header.find(prefix);
      if (pos != 0)
        return caf::make_error(ec::syntax_error,
                               fmt::format("Invalid header line: prefix '{}' "
                                           "not beginning at line "
                                           "beginning or not found",
                                           prefix));
      pos = header.find(sep);
      if (pos == std::string::npos)
        return caf::make_error(ec::syntax_error,
                               fmt::format("Invalid header line: separator "
                                           "{} not found",
                                           sep));
      if (pos + 1 >= header.size())
        return caf::make_error(ec::syntax_error,
                               fmt::format("Missing Zeek TSV header line "
                                           "content: {}",
                                           header));
      parsed_options.emplace_back(header.substr(pos + 1));
    }

    set_sep = std::string{parsed_options[0][0]};
    empty_field = parsed_options[1];
    unset_field = parsed_options[2];
    path = parsed_options[3];
    open = parsed_options[4];
    fields_str = parsed_options[5];
    types_str = parsed_options[6];
    fields = detail::split(fields_str, sep);
    types = detail::split(types_str, sep);
    if (fields.size() != types.size())
      return caf::make_error(ec::syntax_error,
                             fmt::format("Zeek TSV header types mismatch: "
                                         "expected {} fields but got {}",
                                         fields.size(), types.size()));

    std::vector<struct record_type::field> record_fields;
    for (auto i = 0u; i < fields.size(); ++i) {
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
    for (const auto& ctrl_schema : ctrl.schemas()) {
      if (ctrl_schema.name() == name) {
        schema = ctrl_schema;
        break;
      }
    }
    if (not schema) {
      schema = type{name, record_schema};
    }
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
  std::string open{};
  std::string fields_str{};
  std::string types_str{};
  std::vector<std::string_view> fields{};
  std::vector<std::string_view> types{};
  std::string name{};
  type schema{};
  std::vector<rule<iterator_type, data>> parsers{};
};

struct zeek_printer {
  zeek_printer(char set_sep, std::string_view empty = "",
               std::string_view unset = "", bool show_timestamp_tags = false)
    : set_separator{set_sep},
      empty_field{empty},
      unset_field{unset},
      show_timestamp_tags{show_timestamp_tags} {
  }

  struct time_factory {
    const char* fmt = "%Y-%m-%d-%H-%M-%S";
  };

  std::string to_zeek_string(const type& t) const {
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

  template <typename It>
  auto print_header(It& out, const type& t) const noexcept -> bool {
    auto header
      = fmt::format("#separator \\x{0:02x}\n"
                    "#set_separator{0}{1}\n"
                    "#empty_field{0}{2}\n"
                    "#unset_field{0}{3}\n"
                    "#path{0}{4}\n",
                    sep, set_separator, empty_field, unset_field, t.name());
    if (show_timestamp_tags)
      header.append(fmt::format("#open{} TIME", sep));
    header.append("#fields");
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
  template <class Iterator>
  struct visitor {
    visitor(Iterator& out, const zeek_printer& printer)
      : out{out}, printer{printer} {
    }

    auto operator()(caf::none_t) noexcept -> bool {
      if (not printer.unset_field.empty()) {
        out = std::copy(printer.unset_field.begin(), printer.unset_field.end(),
                        out);
      }
      return true;
    }

    auto operator()(auto x) noexcept -> bool {
      // TODO: Avoid the data_view cast.
      return data_view_printer{}.print(out, make_data_view(x));
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
      for (auto c : x)
        if (std::iscntrl(c) || c == printer.sep || c == printer.set_separator) {
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
        for (auto c : std::string_view(printer.empty_field))
          *out++ = c;
        return true;
      }
      auto i = x.begin();
      if (!print(out, *i))
        return false;
      for (++i; i != x.end(); ++i) {
        *out++ = printer.set_separator;
        if (!print(out, *i))
          return false;
      }
      return true;
    }

    auto operator()(const view<record>& x) noexcept -> bool {
      auto flattened = flatten(materialize(x));
      return data_view_printer{}.print(out, make_data_view(flattened));
    }

    Iterator& out;
    const zeek_printer& printer;
  };

  char sep{'\t'};
  char set_separator{','};
  std::string empty_field{};
  std::string unset_field{};
  bool show_timestamp_tags{false};
};
} // namespace

class plugin : public virtual parser_plugin, public virtual printer_plugin {
public:
  auto
  make_parser(std::span<std::string const> args, generator<chunk_ptr> loader,
              operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         operator_control_plane& ctrl) -> generator<table_slice> {
        // Parse header.
        auto it = lines.begin();
        auto header = std::string_view{};
        while (it != lines.end()) {
          header = (*it).value();
          if (!header.empty())
            break;
          co_yield {};
        }
        if (header.empty())
          co_return;

        auto metadata = zeek_metadata{};
        auto parsed = metadata.parse_header(it, lines, ctrl);
        if (not parsed) {
          ctrl.abort(parsed.error());
          co_return;
        }
        auto split_parser = ((+(parsers::any - metadata.sep)) % metadata.sep);
        ++it;
        auto closed = false;
        for (; it != lines.end(); ++it) {
          auto b = table_slice_builder{metadata.schema};
          auto line = *it;
          if (!line) {
            co_yield {};
            continue;
          }
          if (line->empty()) {
            VAST_DEBUG("Zeek TSV parser ignored empty line");
            continue;
          }
          if (line->starts_with("#close")) {
            if (closed) {
              ctrl.abort(caf::make_error(
                ec::syntax_error, fmt::format("Parsing Zeek TSV failed: "
                                              "duplicate #close found")));
              co_return;
            }
            closed = true;
            co_yield {};
            continue;
          }
          if (line->starts_with("#separator")) {
            if (not closed) {
              ctrl.abort(caf::make_error(
                ec::syntax_error, fmt::format("Parsing Zeek TSV failed: "
                                              "previous logs are still open")));
              co_return;
            }
            closed = false;
            metadata = zeek_metadata{};
            auto parsed = metadata.parse_header(it, lines, ctrl);
            if (not parsed) {
              ctrl.abort(parsed.error());
              co_return;
            }
            split_parser = ((+(parsers::any - metadata.sep)) % metadata.sep);
            ++it;
          }
          auto values = std::vector<std::string>{};
          if (!split_parser((*it).value(), values)) {
            ctrl.warn(caf::make_error(
              ec::parse_error, fmt::format("zeek tsv parser skipped line: "
                                           "parsing line failed")));
            continue;
          }
          if (values.size() != metadata.fields.size()) {
            ctrl.warn(caf::make_error(
              ec::parse_error,
              fmt::format("zeek tsv parser skipped line: "
                          "expected {} fields but got "
                          "{}",
                          metadata.fields.size(), values.size())));
            continue;
          }
          for (auto i = size_t{0}; i < values.size(); ++i) {
            auto value = values[i];
            auto added = false;
            if (metadata.is_unset(i))
              added = b.add(caf::none);
            else if (metadata.is_empty(i))
              added = b.add(caf::get<record_type>(metadata.schema)
                              .field(i)
                              .type.construct());
              VAST_ERROR("{} is_empty", value);
            } else if (!metadata.parsers[i](metadata.fields[i], values[i])) {
              ctrl.abort(caf::make_error(ec::parse_error,
                                         fmt::format("Zeek TSV parser failed "
                                                     "to parse value '{}'",
                                                     value)));
              co_return;
            }
            added = b.add(value);
            VAST_ERROR("added: {}", added);
            if (!added) {
              ctrl.abort(caf::make_error(ec::parse_error,
                                         fmt::format("Zeek TSV parser failed "
                                                     "to finalize value '{}'",
                                                     value)));
              co_return;
            }
          }
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
    if (not args.empty() and args.size() != 3) {
      return caf::make_error(
        ec::syntax_error,
        fmt::format("{} printer requires 0 or 3 arguments but "
                    "got {}: [{}]",
                    name(), args.size(), fmt::join(args, ", ")));
    }
    auto set_sep = default_set_sep;
    auto empty_field = default_empty_val;
    auto unset_field = default_unset_val;
    if (args.size() == 3) {
      auto parsed_set_sep = to_xsv_sep(args[0]);
      if (!parsed_set_sep) {
        return std::move(parsed_set_sep.error());
      }
      if (*parsed_set_sep == '\t') {
        return caf::make_error(ec::invalid_argument,
                               "separator and set separator must be different");
      }
      set_sep = *parsed_set_sep;
      empty_field = args[1];
      auto conflict
        = std::any_of(empty_field.begin(), empty_field.end(), [&](auto ch) {
            return ch == separator || ch == parsed_set_sep;
          });
      if (conflict) {
        return caf::make_error(ec::invalid_argument, "empty value must not "
                                                     "contain separator or set "
                                                     "separator");
      }
      unset_field = args[2];
      conflict
        = std::any_of(unset_field.begin(), unset_field.end(), [&](auto ch) {
            return ch == separator || ch == parsed_set_sep;
          });
      if (conflict) {
        return caf::make_error(ec::invalid_argument, "unset value must not "
                                                     "contain separator or set "
                                                     "separator");
      }
    }
    auto printer
      = zeek_printer{set_sep, empty_field, unset_field, show_timestamp_tags};
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
        if (printer.show_timestamp_tags)
          out_iter = fmt::format_to(out_iter, "#close{}TIME", separator);
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
    show_timestamp_tags
      = get_or(global_config, "vast.export.zeek.disable-timestamp-tags",
               show_timestamp_tags);

    return caf::none;
  }

  auto name() const -> std::string override {
    return "zeek";
  }

  bool show_timestamp_tags{false};
};

} // namespace vast::plugins::zeek

VAST_REGISTER_PLUGIN(vast::plugins::zeek::plugin)
