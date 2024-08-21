//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/concept/parseable/string/quoted_string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/detail/to_xsv_sep.hpp"
#include "tenzir/parser_interface.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/to_lines.hpp"
#include "tenzir/tql/basic.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/view.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <iterator>

namespace tenzir::plugins::xsv {
namespace {

struct xsv_options {
  char field_sep = {};
  char list_sep = {};
  bool no_header = {};
  bool allow_comments = {};
  bool auto_expand = {};
  std::string null_value = {};
  std::string name = {};
  std::optional<std::string> header = {};

  static auto try_parse(parser_interface& p, std::string name, bool is_parser)
    -> xsv_options {
    auto parser = argument_parser{"xsv", "https://docs.tenzir.com/formats/xsv"};
    auto allow_comments = bool{};
    auto auto_expand = bool{};
    auto field_sep_str = located<std::string>{};
    auto list_sep_str = located<std::string>{};
    auto null_value = located<std::string>{};
    auto header = std::optional<std::string>{};
    auto no_header = bool{};
    if (is_parser) {
      parser.add("--allow-comments", allow_comments);
      parser.add("--auto-expand", auto_expand);
      parser.add("--header", header, "<header>");
    } else {
      parser.add("--no-header", no_header);
    }
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
    return xsv_options{
      .field_sep = *field_sep,
      .list_sep = *list_sep,
      .no_header = no_header,
      .allow_comments = allow_comments,
      .auto_expand = auto_expand,
      .null_value = std::move(null_value.inner),
      .name = std::move(name),
      .header = std::move(header),
    };
  }

  friend auto inspect(auto& f, xsv_options& x) -> bool {
    return f.object(x).fields(
      f.field("name", x.name), f.field("field_sep", x.field_sep),
      f.field("list_sep", x.list_sep), f.field("null_value", x.null_value),
      f.field("allow_comments", x.allow_comments),
      f.field("auto_expand", x.auto_expand), f.field("header", x.header),
      f.field("no_header", x.no_header));
  }
};

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
      TENZIR_UNREACHABLE();
    }

    auto operator()(view<map>) noexcept -> bool {
      TENZIR_UNREACHABLE();
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

    auto operator()(view<blob> x) noexcept -> bool {
      return (*this)(detail::base64::encode(x));
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
                operator_control_plane& ctrl, xsv_options args)
  -> generator<table_slice> {
  auto last_finish = std::chrono::steady_clock::now();
  // Parse header.
  auto it = lines.begin();
  auto header = std::optional<std::string_view>{};
  if (args.header) {
    header = *args.header;
  } else {
    while (it != lines.end()) {
      auto line = *it;
      ++it;
      if (not line) {
        co_yield {};
        continue;
      }
      if (line->empty()) {
        continue;
      }
      if (args.allow_comments && line->front() == '#') {
        continue;
      }
      header = line;
      break;
      if (not header) {
        co_return;
      }
    }
  }
  const auto qqstring_value_parser = parsers::qqstr.then([](std::string in) {
    static auto unescaper = [](auto& f, auto l, auto out) {
      if (*f != '\\') { // Skip every non-escape character.
        *out++ = *f++;
        return true;
      }
      if (l - f < 2) {
        return false;
      }
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
  });
  const auto string_value_parser
    = ((qqstring_value_parser >> &(args.field_sep | parsers::eoi))
       | *(parsers::any - args.field_sep));
  auto header_parser = (string_value_parser % args.field_sep);
  auto fields = std::vector<std::string>{};
  if (!header_parser(*header, fields)) {
    diagnostic::error("failed to parse header")
      .note("from `{}`", args.name)
      .emit(ctrl.diagnostics());
    co_return;
  }
  auto b = series_builder{};
  for (; it != lines.end(); ++it) {
    auto line = *it;
    const auto now = std::chrono::steady_clock::now();
    if (b.length()
          >= detail::narrow_cast<int64_t>(defaults::import::table_slice_size)
        or last_finish + defaults::import::batch_timeout < now) {
      last_finish = now;
      for (auto&& slice :
           b.finish_as_table_slice(fmt::format("tenzir.{}", args.name))) {
        co_yield std::move(slice);
      }
    }
    if (not line) {
      if (last_finish != now) {
        co_yield {};
      }
      continue;
    }
    if (line->empty()) {
      continue;
    }
    if (args.allow_comments && line->front() == '#') {
      continue;
    }
    const auto single_value_delimiter
      = (parsers::eoi | args.list_sep | args.field_sep);
    const auto single_value_parser
      = (parsers::lit{args.null_value} >> &single_value_delimiter)
          .then([](std::string) {
            return data{};
          })
        | (parsers::data >> &single_value_delimiter).with([](const data& d) {
            return caf::visit(
              []<class T>(const T&) {
                return not detail::is_any_v<T, pattern, std::string, list,
                                            record>;
              },
              d);
          })
        | (qqstring_value_parser >> &single_value_delimiter
           | *(parsers::any - single_value_delimiter))
            .then([](std::string str) {
              return data{std::move(str)};
            });
    const auto value_parser = (single_value_parser % args.list_sep)
                                .then([](std::vector<data> values) -> data {
                                  TENZIR_ASSERT(not values.empty());
                                  if (values.size() == 1) {
                                    return std::move(values[0]);
                                  }
                                  return values;
                                });
    auto values_parser = (value_parser % args.field_sep);
    auto values = std::vector<data>{};
    if (not values_parser(*line, values)) {
      diagnostic::warning("skips unparseable line")
        .note("from `{}` parser", args.name)
        .emit(ctrl.diagnostics());
      continue;
    }
    auto generated_field_id = 0;
    if (args.auto_expand) {
      while (fields.size() < values.size()) {
        auto name = fmt::format("unnamed{}", ++generated_field_id);
        if (std::find(fields.begin(), fields.end(), name) == fields.end()) {
          fields.push_back(name);
        }
      }
    } else if (fields.size() < values.size()) {
      diagnostic::warning("skips {} excess values in line",
                          values.size() - fields.size())
        .hint("use `--auto-expand` to add fields for excess values")
        .note("from `{}` parser", args.name)
        .emit(ctrl.diagnostics());
    }
    auto row = b.record();
    for (size_t i = 0; i < fields.size(); ++i) {
      if (i >= values.size()) {
        row.field(fields[i]).null();
        continue;
      }
      auto result = row.field(fields[i]).try_data(values[i]);
      if (not result) {
        diagnostic::warning(result.error())
          .note("from `{}` parser", args.name)
          .emit(ctrl.diagnostics());
      }
    }
  }
  if (b.length() > 0) {
    for (auto&& slice :
         b.finish_as_table_slice(fmt::format("tenzir.{}", args.name))) {
      co_yield std::move(slice);
    }
  }
}

class xsv_parser final : public plugin_parser {
public:
  xsv_parser() = default;

  explicit xsv_parser(xsv_options args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "xsv";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_impl(to_lines(std::move(input)), ctrl, args_);
  }

  friend auto inspect(auto& f, xsv_parser& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.plugins.xsv.xsv_parser")
      .fields(f.field("args", x.args_));
  }

private:
  xsv_options args_{};
};

class xsv_printer final : public plugin_printer {
public:
  xsv_printer() = default;

  explicit xsv_printer(xsv_options args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "xsv";
  }

  auto
  instantiate([[maybe_unused]] type input_schema, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    auto metadata = chunk_metadata{.content_type = content_type()};
    return printer_instance::make(
      [meta = std::move(metadata), args = args_,
       first = true](table_slice slice) mutable -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer
          = xsv_printer_impl{args.field_sep, args.list_sep, args.null_value};
        auto buffer = std::vector<char>{};
        auto out_iter = std::back_inserter(buffer);
        auto resolved_slice = flatten(resolve_enumerations(slice)).slice;
        auto input_schema = resolved_slice.schema();
        auto input_type = caf::get<record_type>(input_schema);
        auto array
          = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
        for (const auto& row : values(input_type, *array)) {
          TENZIR_ASSERT(row);
          if (first && not args.no_header) {
            printer.print_header(out_iter, *row);
            first = false;
            out_iter = fmt::format_to(out_iter, "\n");
          }
          const auto ok = printer.print_values(out_iter, *row);
          TENZIR_ASSERT(ok);
          out_iter = fmt::format_to(out_iter, "\n");
        }
        auto chunk = chunk::make(std::move(buffer), meta);
        co_yield std::move(chunk);
      });
  }

  auto allows_joining() const -> bool override {
    return args_.no_header;
  };

  auto prints_utf8() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, xsv_printer& x) -> bool {
    return f.apply(x.args_);
  }

private:
  auto content_type() const -> std::string {
    switch (args_.field_sep) {
      default:
        return "text/plain";
      case ',':
        return "text/csv";
      case '\t':
        return "text/tab-separated-values";
    }
  }

  xsv_options args_;
};

class xsv_plugin : public virtual parser_plugin<xsv_parser>,
                   public virtual printer_plugin<xsv_printer> {
public:
  auto name() const -> std::string override {
    return "xsv";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    const auto is_parser = true;
    auto options = xsv_options::try_parse(p, "xsv", is_parser);
    return std::make_unique<xsv_parser>(std::move(options));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    const auto is_parser = false;
    auto options = xsv_options::try_parse(p, "xsv", is_parser);
    return std::make_unique<xsv_printer>(std::move(options));
  }
};

template <detail::string_literal Name, char Sep, char ListSep,
          detail::string_literal Null>
class configured_xsv_plugin final : public virtual parser_parser_plugin,
                                    public virtual printer_parser_plugin {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{name()};
    bool allow_comments = {};
    bool auto_expand = {};
    std::optional<std::string> header = {};
    parser.add("--allow-comments", allow_comments);
    parser.add("--auto-expand", auto_expand);
    parser.add("--header", header, "<header>");
    parser.parse(p);
    return std::make_unique<xsv_parser>(xsv_options{
      .field_sep = Sep,
      .list_sep = ListSep,
      .no_header = false,
      .allow_comments = allow_comments,
      .auto_expand = auto_expand,
      .null_value = std::string{Null.str()},
      .name = std::string{Name.str()},
      .header = std::move(header),
    });
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{name()};
    bool no_header = {};
    parser.add("--no-header", no_header);
    parser.parse(p);
    return std::make_unique<xsv_printer>(xsv_options{
      .field_sep = Sep,
      .list_sep = ListSep,
      .no_header = no_header,
      .allow_comments = false,
      .auto_expand = false,
      .null_value = std::string{Null.str()},
      .name = std::string{Name.str()},
      .header = {},
    });
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  }
};

using csv_plugin = configured_xsv_plugin<"csv", ',', ';', "">;
using tsv_plugin = configured_xsv_plugin<"tsv", '\t', ',', "-">;
using ssv_plugin = configured_xsv_plugin<"ssv", ' ', ',', "-">;

class read_csv final : public operator_plugin2<parser_adapter<xsv_parser>> {
public:
  auto name() const -> std::string override {
    return "read_csv";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto comments = false;
    argument_parser2::operator_(name())
      .add("comments", comments)
      .parse(inv, ctx)
      .ignore();
    auto options = xsv_options{};
    options.name = "csv";
    options.field_sep = ',';
    options.list_sep = ';';
    options.null_value = "";
    options.allow_comments = comments;
    return std::make_unique<parser_adapter<xsv_parser>>(
      xsv_parser{std::move(options)});
  }
};

} // namespace tenzir::plugins::xsv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::xsv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::csv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::tsv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::ssv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::read_csv)
