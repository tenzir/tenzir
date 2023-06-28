//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"
#include "vast/argument_parser.hpp"
#include "vast/arrow_table_slice.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/string_literal.hpp"
#include "vast/detail/to_xsv_sep.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/parser_interface.hpp"
#include "vast/plugin.hpp"
#include "vast/to_lines.hpp"
#include "vast/tql/basic.hpp"
#include "vast/view.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <iterator>

namespace vast::plugins::xsv {
namespace {

struct xsv_printer_impl {
  xsv_printer_impl(char sep, char list_sep, std::string null)
    : sep{sep}, list_sep{list_sep}, null{std::move(null)} {
  }

  template <typename It>
  auto print_header(It& out, const view<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [k, _] : x) {
      if (!first) {
        ++out = sep;
      } else {
        first = false;
      }
      visitor{out, *this}(k);
    }
    return true;
  }

  template <typename It>
  auto print_values(It& out, const view<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (!first) {
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
    visitor(Iterator& out, const xsv_printer_impl& printer)
      : out{out}, printer{printer} {
    }

    auto operator()(caf::none_t) noexcept -> bool {
      if (!printer.null.empty()) {
        sequence_empty = false;
        out = std::copy(printer.null.begin(), printer.null.end(), out);
      }
      return true;
    }

    auto operator()(auto x) noexcept -> bool {
      sequence_empty = false;
      make_printer<decltype(x)> p;
      return p.print(out, x);
    }

    auto operator()(view<pattern>) noexcept -> bool {
      die("unreachable");
    }

    auto operator()(view<map>) noexcept -> bool {
      die("unreachable");
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      sequence_empty = false;
      auto needs_escaping = std::any_of(x.begin(), x.end(), [this](auto c) {
        return c == printer.sep || c == '"';
      });
      if (needs_escaping) {
        static auto escaper = [](auto& f, auto out) {
          switch (*f) {
            default:
              *out++ = *f++;
              return;
            case '\\':
              *out++ = '\\';
              *out++ = '\\';
              break;
            case '"':
              *out++ = '\\';
              *out++ = '"';
              break;
          }
          ++f;
          return;
        };
        static auto p = '"' << printers::escape(escaper) << '"';
        return p.print(out, x);
      }
      out = std::copy(x.begin(), x.end(), out);
      return true;
    }

    auto operator()(const view<list>& x) noexcept -> bool {
      sequence_empty = true;
      for (const auto& v : x) {
        if (!sequence_empty) {
          ++out = printer.list_sep;
        }
        if (!caf::visit(*this, v)) {
          return false;
        }
      }
      return true;
    }

    auto operator()(const view<record>& x) noexcept -> bool {
      sequence_empty = true;
      for (const auto& [_, v] : x) {
        if (!sequence_empty) {
          ++out = printer.list_sep;
        }
        if (!caf::visit(*this, v)) {
          return false;
        }
      }
      return true;
    }

    Iterator& out;
    const xsv_printer_impl& printer;
    bool sequence_empty{true};
  };

  char sep{','};
  char list_sep{';'};
  std::string null{};
};

} // namespace

auto parse_impl(generator<std::optional<std::string_view>> lines,
                operator_control_plane& ctrl, char sep, std::string name)
  -> generator<table_slice> {
  // Parse header.
  auto it = lines.begin();
  auto header = std::optional<std::string_view>{};
  while (it != lines.end()) {
    header = *it;
    if (header && !header->empty()) {
      break;
    }
    co_yield {};
    if (header and header->empty()) {
      ++it;
    }
  }
  if (!header || header->empty())
    co_return;
  auto split_parser
    = (((parsers::qqstr
           .then([](std::string in) {
             static auto unescaper = [](auto& f, auto l, auto out) {
               if (*f != '\\') { // Skip every non-escape character.
                 *out++ = *f++;
                 return true;
               }
               if (l - f < 2)
                 return false;
               switch (auto c = *++f) {
                 case '\\':
                   *out++ = '\\';
                   break;
                 case '"':
                   *out++ = '"';
                   break;
               }
               ++f;
               return true;
             };
             return detail::unescape(in, unescaper);
           })
           .with([](const std::string& in) {
             return !in.empty();
           })
         >> &(sep | parsers::eoi))
        | +(parsers::any - sep))
       % sep);
  auto fields = std::vector<std::string>{};
  if (!split_parser(*header, fields)) {
    ctrl.abort(
      caf::make_error(ec::parse_error, fmt::format("{0} parser failed to parse "
                                                   "header of {0} input",
                                                   name)));
    co_return;
  }
  ++it;
  auto b = adaptive_table_slice_builder{};
  for (; it != lines.end(); ++it) {
    auto line = *it;
    if (!line || line->empty()) {
      co_yield b.finish();
      continue;
    }
    auto row = b.push_row();
    auto values = std::vector<std::string>{};
    if (!split_parser(*line, values)) {
      ctrl.warn(
        caf::make_error(ec::parse_error, fmt::format("{} parser skipped line: "
                                                     "parsing line failed",
                                                     name)));
      continue;
    }
    if (values.size() != fields.size()) {
      ctrl.warn(caf::make_error(
        ec::parse_error,
        fmt::format("{} parser skipped line: expected {} fields but got "
                    "{}",
                    name, fields.size(), values.size())));
      continue;
    }
    for (const auto& [field, value] : detail::zip(fields, values)) {
      auto field_guard = row.push_field(field);
      if (auto err = field_guard.add(value))
        ctrl.warn(std::move(err));
      // TODO: Check what add() does with strings.
    }
  }
}

class xsv_parser final : public plugin_parser {
public:
  xsv_parser() = default;

  explicit xsv_parser(char sep) : sep_{sep} {
  }

  auto name() const -> std::string override {
    return "xsv";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_impl(to_lines(std::move(input)), ctrl, sep_, "xsv");
  }

  friend auto inspect(auto& f, xsv_parser& x) -> bool {
    return f.apply(x.sep_);
  }

private:
  char sep_{};
};

class xsv_printer final : public plugin_printer {
public:
  struct args {
    char field_sep{};
    char list_sep{};
    std::string null_value;

    friend auto inspect(auto& f, args& x) -> bool {
      return f.object(x).fields(f.field("field_sep", x.field_sep),
                                f.field("list_sep", x.list_sep),
                                f.field("null_value", x.null_value));
    }
  };

  xsv_printer() = default;

  explicit xsv_printer(args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "xsv";
  }

  auto
  instantiate([[maybe_unused]] type input_schema, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    auto input_type = caf::get<record_type>(input_schema);
    auto printer
      = xsv_printer_impl{args_.field_sep, args_.list_sep, args_.null_value};
    return printer_instance::make([printer = std::move(printer)](
                                    table_slice slice) -> generator<chunk_ptr> {
      auto buffer = std::vector<char>{};
      auto out_iter = std::back_inserter(buffer);
      auto resolved_slice = flatten(resolve_enumerations(slice)).slice;
      auto input_schema = resolved_slice.schema();
      auto input_type = caf::get<record_type>(input_schema);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto first = true;
      for (const auto& row : values(input_type, *array)) {
        VAST_ASSERT_CHEAP(row);
        if (first) {
          printer.print_header(out_iter, *row);
          first = false;
          out_iter = fmt::format_to(out_iter, "\n");
        }
        const auto ok = printer.print_values(out_iter, *row);
        VAST_ASSERT_CHEAP(ok);
        out_iter = fmt::format_to(out_iter, "\n");
      }
      auto chunk = chunk::make(std::move(buffer));
      co_yield std::move(chunk);
    });
  }

  auto allows_joining() const -> bool override {
    return false;
  };

  friend auto inspect(auto& f, xsv_printer& x) -> bool {
    return f.apply(x.args_);
  }

private:
  args args_;
};

class xsv_plugin : public virtual parser_plugin<xsv_parser>,
                   public virtual printer_plugin<xsv_printer> {
public:
  auto name() const -> std::string override {
    return "xsv";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto sep_str = located<std::string>{};
    auto parser = argument_parser{"xsv", "https://docs.tenzir.com/next/"
                                         "formats/xsv"};
    parser.add(sep_str, "<sep>");
    parser.parse(p);
    auto sep = to_xsv_sep(sep_str.inner);
    if (!sep) {
      // TODO: Improve error message.
      diagnostic::error("{}", sep.error()).primary(sep_str.source).throw_();
    }
    return std::make_unique<xsv_parser>(*sep);
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto field_sep_str = located<std::string>{};
    auto list_sep_str = located<std::string>{};
    auto null_value = located<std::string>{};
    auto parser
      = argument_parser{"xsv", "https://docs.tenzir.com/next/formats/xsv"};
    parser.add(field_sep_str, "<field-sep>");
    parser.add(list_sep_str, "<list-sep>");
    parser.add(null_value, "<null-value>");
    parser.parse(p);
    auto field_sep = to_xsv_sep(field_sep_str.inner);
    if (!field_sep) {
      diagnostic::error("invalid separator: {}", field_sep.error())
        .primary(field_sep_str.source)
        .throw_();
    }
    auto list_sep = to_xsv_sep(list_sep_str.inner);
    if (!list_sep) {
      diagnostic::error("invalid separator: {}", list_sep.error())
        .primary(list_sep_str.source)
        .throw_();
    }
    if (*field_sep == *list_sep) {
      diagnostic::error("field separator and list separator must be "
                        "different")
        .primary(field_sep_str.source)
        .primary(list_sep_str.source)
        .throw_();
    }
    for (auto ch : null_value.inner) {
      if (ch == *field_sep) {
        diagnostic::error("null value conflicts with field separator")
          .primary(field_sep_str.source)
          .primary(null_value.source)
          .throw_();
      }
      if (ch == *list_sep) {
        diagnostic::error("null value conflicts with list separator")
          .primary(list_sep_str.source)
          .primary(null_value.source)
          .throw_();
      }
    }
    return std::make_unique<xsv_printer>(
      xsv_printer::args{.field_sep = *field_sep,
                        .list_sep = *list_sep,
                        .null_value = std::move(null_value.inner)});
  }
};

template <detail::string_literal Name, char Sep, char ListSep,
          detail::string_literal Null>
class configured_xsv_plugin final : public virtual parser_parser_plugin,
                                    public virtual printer_parser_plugin {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    argument_parser{name()}.parse(p);
    return std::make_unique<xsv_parser>(Sep);
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    argument_parser{name()}.parse(p);
    return std::make_unique<xsv_printer>(
      xsv_printer::args{.field_sep = Sep,
                        .list_sep = ListSep,
                        .null_value = std::string{Null.str()}});
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  }
};

using csv_plugin = configured_xsv_plugin<"csv", ',', ';', "">;
using tsv_plugin = configured_xsv_plugin<"tsv", '\t', ',', "-">;
using ssv_plugin = configured_xsv_plugin<"ssv", ' ', ',', "-">;

} // namespace vast::plugins::xsv

VAST_REGISTER_PLUGIN(vast::plugins::xsv::xsv_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::xsv::csv_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::xsv::tsv_plugin)
VAST_REGISTER_PLUGIN(vast::plugins::xsv::ssv_plugin)
