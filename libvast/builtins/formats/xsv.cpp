//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"
#include "vast/arrow_table_slice.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/detail/string_literal.hpp"
#include "vast/detail/to_xsv_sep.hpp"
#include "vast/detail/zip_iterator.hpp"
#include "vast/plugin.hpp"
#include "vast/to_lines.hpp"
#include "vast/view.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <iterator>

namespace vast::plugins::xsv {
namespace {

struct xsv_printer {
  xsv_printer(char sep, char list_sep, std::string null)
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
    visitor(Iterator& out, const xsv_printer& printer)
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
    const xsv_printer& printer;
    bool sequence_empty{true};
  };

  char sep{','};
  char list_sep{';'};
  std::string null{};
};

} // namespace

class xsv_plugin : public virtual parser_plugin, public virtual printer_plugin {
public:
  auto make_parser(std::vector<std::string> args, generator<chunk_ptr> loader,
                   operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (args.size() != 1) {
      return caf::make_error(
        ec::syntax_error,
        fmt::format("{} parser requires exactly 1 argument but "
                    "got {}: [{}]",
                    name(), args.size(), fmt::join(args, ", ")));
    }
    auto sep = to_xsv_sep(args[0]);
    if (!sep) {
      return std::move(sep.error());
    }
    return std::invoke(
      [](generator<std::optional<std::string_view>> lines,
         operator_control_plane& ctrl, char sep,
         std::string name) -> generator<table_slice> {
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
          ctrl.abort(caf::make_error(ec::parse_error,
                                     fmt::format("{0} parser failed to parse "
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
            ctrl.warn(caf::make_error(ec::parse_error,
                                      fmt::format("{} parser skipped line: "
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
            field_guard.add(value);
            // TODO: Check what add() does with strings.
          }
        }
      },
      to_lines(std::move(loader)), ctrl, *sep, name());
  }

  auto default_loader([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdin", {}};
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane&) const
    -> caf::expected<printer> override {
    if (args.size() != 3) {
      return caf::make_error(
        ec::syntax_error,
        fmt::format("{} printer requires exactly 3 arguments but "
                    "got {}: [{}]",
                    name(), args.size(), fmt::join(args, ", ")));
    }
    auto sep = to_xsv_sep(args[0]);
    if (!sep) {
      return std::move(sep.error());
    }
    auto list_sep = to_xsv_sep(args[1]);
    if (!list_sep) {
      return std::move(list_sep.error());
    }
    if (*sep == *list_sep) {
      return caf::make_error(ec::invalid_argument,
                             "separator and list separator must be different");
    }
    auto null = args[2];
    auto conflict = std::any_of(null.begin(), null.end(), [&](auto ch) {
      return ch == sep || ch == list_sep;
    });
    if (conflict) {
      return caf::make_error(ec::invalid_argument,
                             "null value must not contain separator or list "
                             "separator");
    }
    auto input_type = caf::get<record_type>(input_schema);
    auto printer = xsv_printer{*sep, *list_sep, null};
    return to_printer(
      [printer = std::move(printer), input_type = std::move(input_type)](
        table_slice slice) -> generator<chunk_ptr> {
        auto buffer = std::vector<char>{};
        auto out_iter = std::back_inserter(buffer);
        auto resolved_slice = resolve_enumerations(slice);
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
    return "xsv";
  }
};

template <detail::string_literal Name, char Sep, char ListSep,
          detail::string_literal Null>
class configured_xsv_plugin final : public virtual xsv_plugin {
public:
  auto make_parser(std::vector<std::string> args, generator<chunk_ptr> loader,
                   operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} parser does not expect any "
                                         "arguments, "
                                         "but got [{}]",
                                         name(), fmt::join(args, ", ")));
    }

    return xsv_plugin::make_parser(std::vector<std::string>{std::string{Sep}},
                                   std::move(loader), ctrl);
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane& ctrl) const
    -> caf::expected<printer> override {
    if (!args.empty()) {
      return caf::make_error(ec::invalid_argument,
                             fmt::format("{} printer does not expect any "
                                         "arguments, "
                                         "but got [{}]",
                                         name(), fmt::join(args, ", ")));
    }

    return xsv_plugin::make_printer(
      std::vector<std::string>{std::string{Sep}, std::string{ListSep},
                               std::string{Null.str()}},
      std::move(input_schema), ctrl);
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
