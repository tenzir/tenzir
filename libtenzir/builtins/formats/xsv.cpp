//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/concept/parseable/string/quoted_string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/detail/to_xsv_sep.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/multi_series_builder_argument_parser.hpp"
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
  std::string name = {};
  char field_sep = {};
  char list_sep = {};
  std::string null_value = {};
  std::string quotes = "\"\'";
  bool doubled_quotes_escape = {};
  bool no_header = {};
  bool auto_expand = {};
  bool allow_comments = {};
  std::optional<std::string> header = {};
  multi_series_builder::options builder_options = {};

  static auto try_parse_printer_options(parser_interface& p,
                                        std::string name) -> xsv_options {
    auto parser = argument_parser{"xsv", "https://docs.tenzir.com/formats/xsv"};
    auto field_sep_str = located<std::string>{};
    auto list_sep_str = located<std::string>{};
    auto null_value = located<std::string>{};
    auto no_header = bool{};
    parser.add("--no-header", no_header);
    parser.add(field_sep_str, "<field-sep>");
    parser.add(list_sep_str, "<list-sep>");
    parser.add(null_value, "<null-value>");
    parser.parse(p);
    auto field_sep = to_xsv_sep(field_sep_str.inner);
    if (!field_sep) {
      diagnostic::error(field_sep.error())
        .primary(field_sep_str.source)
        .throw_();
    }
    auto list_sep = to_xsv_sep(list_sep_str.inner);
    if (!list_sep) {
      diagnostic::error(list_sep.error()).primary(list_sep_str.source).throw_();
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
      .name = std::move(name),
      .field_sep = *field_sep,
      .list_sep = *list_sep,
      .null_value = std::move(null_value.inner),
      .no_header = no_header,
    };
  }

  friend auto inspect(auto& f, xsv_options& x) -> bool {
    return f.object(x).fields(
      f.field("name", x.name), f.field("field_sep", x.field_sep),
      f.field("list_sep", x.list_sep), f.field("null_value", x.null_value),
      f.field("allow_comments", x.allow_comments), f.field("header", x.header),
      f.field("no_header", x.no_header), f.field("auto_expand", x.auto_expand),
      f.field("builder_options", x.builder_options));
  }
};

struct xsv_common_parser_options_parser : multi_series_builder_argument_parser {
  xsv_common_parser_options_parser(std::string name, char field_sep_default,
                                   char list_sep_default,
                                   std::string null_value_default)
    : name_{std::move(name)},
      field_sep_str_{
        located{std::string{field_sep_default}, location::unknown}},
      list_sep_str_{located{std::string{list_sep_default}, location::unknown}},
      null_value_{located{std::move(null_value_default), location::unknown}},
      mode_{mode::special_optional} {
    settings_.merge = true;
  }

  xsv_common_parser_options_parser(std::string name) : name_{std::move(name)} {
    settings_.merge = true;
  }

  auto add_to_parser(argument_parser& parser) -> void {
    if (mode_ == mode::special_optional) {
      parser.add("--list-sep", list_sep_str_, "<list-sep>");
      parser.add("--null-value", null_value_, "<null-value>");
    } else {
      field_sep_str_ = located{"REQUIRED", location::unknown};
      list_sep_str_ = located{"REQUIRED", location::unknown};
      null_value_ = located{"REQUIRED", location::unknown};
      parser.add(*field_sep_str_, "<field-sep>");
      parser.add(*list_sep_str_, "<list-sep>");
      parser.add(*null_value_, "<null-value>");
    }
    parser.add("--allow-comments", allow_comments_);
    parser.add("--header", header_, "<header>");
    parser.add("--auto-expand", auto_expand_);
    multi_series_builder_argument_parser::add_policy_to_parser(parser);
    multi_series_builder_argument_parser::add_settings_to_parser(parser, true,
                                                                 false);
  }

  auto add_to_parser(argument_parser2& parser) -> void {
    if (mode_ == mode::special_optional) {
      parser.named("list_sep", list_sep_str_);
      parser.named("null_value", null_value_);
    } else {
      field_sep_str_ = located{"REQUIRED", location::unknown};
      list_sep_str_ = located{"REQUIRED", location::unknown};
      null_value_ = located{"REQUIRED", location::unknown};
      parser.positional("field_sep", *field_sep_str_);
      parser.positional("list_sep", *list_sep_str_);
      parser.positional("null_value", *null_value_);
    }
    parser.named("quotes", quotes_);
    parser.named("doubled_quotes_escape", doubled_quotes_escape_);
    parser.named("comments", allow_comments_);
    parser.named("header", header_);
    parser.named("auto_expand", auto_expand_);
    multi_series_builder_argument_parser::add_policy_to_parser(parser);
    multi_series_builder_argument_parser::add_settings_to_parser(parser, true,
                                                                 false);
  }

  auto get_options(diagnostic_handler& dh) -> failure_or<xsv_options> {
    auto field_sep = to_xsv_sep(field_sep_str_->inner);
    if (!field_sep) {
      diagnostic::error(field_sep.error())
        .primary(field_sep_str_->source)
        .emit(dh);
      return failure::promise();
    }
    auto list_sep = to_xsv_sep(list_sep_str_->inner);
    if (!list_sep) {
      diagnostic::error(list_sep.error())
        .primary(list_sep_str_->source)
        .emit(dh);
      return failure::promise();
    }
    if (*field_sep == *list_sep) {
      diagnostic::error("field separator and list separator must be "
                        "different")
        .primary(field_sep_str_->source)
        .primary(list_sep_str_->source)
        .emit(dh);
      return failure::promise();
    }
    for (auto ch : null_value_->inner) {
      if (ch == *field_sep) {
        diagnostic::error("null value conflicts with field separator")
          .primary(field_sep_str_->source)
          .primary(null_value_->source)
          .emit(dh);
        return failure::promise();
      }
      if (ch == *list_sep) {
        diagnostic::error("null value conflicts with list separator")
          .primary(field_sep_str_->source)
          .primary(null_value_->source)
          .emit(dh);
        return failure::promise();
      }
    }
    for (auto ch : quotes_->inner) {
      if (ch == *field_sep) {
        diagnostic::error("quote character conflicts with field separator")
          .primary(field_sep_str_->source)
          .primary(null_value_->source)
          .emit(dh);
        return failure::promise();
      }
      if (ch == *list_sep) {
        diagnostic::error("quote character conflicts with list separator")
          .primary(field_sep_str_->source)
          .primary(null_value_->source)
          .emit(dh);
        return failure::promise();
      }
      for (auto ch2 : null_value_->inner) {
        if (ch == ch2) {
          diagnostic::error("quote character conflicts with null value")
            .primary(field_sep_str_->source)
            .primary(null_value_->source)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    TRY(auto opts, multi_series_builder_argument_parser::get_options(dh));
    return xsv_options{
      .name = "xsv",
      .field_sep = *field_sep,
      .list_sep = *list_sep,
      .null_value = null_value_->inner,
      .quotes = quotes_->inner,
      .doubled_quotes_escape = doubled_quotes_escape_,
      .no_header = false,
      .auto_expand = auto_expand_,
      .allow_comments = allow_comments_,
      .header = header_ ? std::optional{header_->inner} : std::nullopt,
      .builder_options = std::move(opts),
    };
  }
  enum class mode {
    all_required,
    special_optional,
  };

protected:
  std::string name_;
  bool allow_comments_{};
  std::optional<located<std::string>> header_{};
  std::optional<located<std::string>> field_sep_str_{};
  std::optional<located<std::string>> list_sep_str_{};
  std::optional<located<std::string>> null_value_{};
  std::optional<located<std::string>> quotes_
    = located{xsv_options{}.quotes, location::unknown};
  bool doubled_quotes_escape_{};
  bool auto_expand_{};
  mode mode_ = mode::all_required;
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
      match(v, visitor{out, *this});
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

    auto operator()(view<record>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      sequence_empty = false;
      auto needs_escaping = std::any_of(x.begin(), x.end(), [this](auto c) {
        return c == printer.sep || c == '"' || c == '\n' || c == '\r'
               || c == '\v' || c == '\f';
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
        if (!match(v, *this)) {
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

auto parse_loop(generator<std::optional<std::string_view>> lines,
                operator_control_plane& ctrl,
                xsv_options args) -> generator<table_slice> {
  // Parse header.
  auto it = lines.begin();
  auto line = std::optional<std::string_view>{};
  auto field_text = std::string_view{};
  size_t line_counter = 0;
  auto header = args.header;
  const auto quoting_options = detail::quoting_escaping_policy{
    .quotes = args.quotes,
    .backslashes_escape = true,
    .doubled_quotes_escape = args.doubled_quotes_escape,
  };
  if (not header) {
    for (; it != lines.end(); ++it) {
      line = *it;
      if (not line) {
        co_yield {};
        continue;
      }
      ++line_counter;
      if (line->empty()) {
        continue;
      }
      if (args.allow_comments && line->front() == '#') {
        continue;
      }
      header = std::string{*line};
      ++it;
      break;
    }
    if (not header) {
      co_return;
    }
  }
  TENZIR_ASSERT(header);
  auto fields = std::vector<std::string>{};
  line = header;
  while (not line->empty()) {
    std::tie(field_text, *line)
      = quoting_options.split_at_unquoted(*line, args.field_sep);
    fields.emplace_back(quoting_options.unquote_unescape(field_text));
  }
  if (fields.empty()) {
    diagnostic::error("failed to parse header")
      .note("from `{}`", args.name)
      .emit(ctrl.diagnostics());
    co_return;
  }
  // parse the body
  const auto original_field_count = fields.size();
  args.builder_options.settings.default_schema_name
    = fmt::format("tenzir.{}", args.name);
  auto dh = transforming_diagnostic_handler{
    ctrl.diagnostics(),
    [&](diagnostic d) {
      d.message = fmt::format("{} parser: {}", args.name, d.message);
      d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                      fmt::format("line {}", line_counter));
      return d;
    },
  };
  auto msb = multi_series_builder{
    args.builder_options,
    dh,
  };
  for (; it != lines.end(); ++it) {
    for (auto& v : msb.yield_ready_as_table_slice()) {
      co_yield std::move(v);
    }
    line = *it;
    if (not line) {
      co_yield {};
      continue;
    }
    ++line_counter;
    if (line->empty()) {
      continue;
    }
    if (args.allow_comments && line->front() == '#') {
      continue;
    }
    auto r = msb.record();
    auto field_idx = size_t{0};
    for (field_idx = 0; true; ++field_idx) {
      if (line->empty()) {
        if (field_idx < original_field_count) {
          diagnostic::warning("{} parser found too few values in a line",
                              args.name)
            .note("line {} has {} values, but should have {} values",
                  line_counter, field_idx, original_field_count)
            .emit(ctrl.diagnostics());
          r.unflattened_field(fields[field_idx]).null();
          continue;
        } else {
          break;
        }
      } else if (field_idx >= fields.size()) {
        if (args.auto_expand) {
          size_t unnamed_idx = 1;
          while (true) {
            auto name = fmt::format("unnamed{}", unnamed_idx);
            if (std::ranges::find(fields, name) == fields.end()) {
              fields.push_back(name);
              break;
            } else {
              ++unnamed_idx;
            }
          }
        } else {
          const auto excess_values
            = 1 + std::ranges::count(*line, args.field_sep);
          diagnostic::warning("{} parser skipped excess values in a line",
                              args.name)
            .note("line {}: {} extra values were skipped", line_counter,
                  excess_values)
            .hint("use `--auto-expand` to add fields for excess values")
            .emit(ctrl.diagnostics());
          break;
        }
      }
      auto field = r.unflattened_field(fields[field_idx]);
      std::tie(field_text, line)
        = quoting_options.split_at_unquoted(*line, args.field_sep);
      auto list_element_text = std::string_view{};
      std::tie(list_element_text, field_text)
        = quoting_options.split_at_unquoted(field_text, args.list_sep);
      // If it is a list, then there remains text in `field_text` (because it is
      // after the list separator)
      if (not field_text.empty()) {
        auto l = field.list();
        while (true) {
          if (list_element_text.empty() and field_text.empty()) {
            break;
          } else if (list_element_text.empty()) {
            l.null();
          } else {
            l.data_unparsed(
              quoting_options.unquote_unescape(list_element_text));
          }
          std::tie(list_element_text, field_text)
            = quoting_options.split_at_unquoted(field_text, args.list_sep);
        }
      }
      // If it is NOT a list, then all text was moved into `list_element_text`
      else {
        if (list_element_text.empty()) {
          field.null();
        } else {
          field.data_unparsed(
            quoting_options.unquote_unescape(list_element_text));
        }
      }
    }
    for (; field_idx < fields.size(); ++field_idx) {
      r.unflattened_field(fields[field_idx]).null();
    }
  }
  for (auto& v : msb.finalize_as_table_slice()) {
    co_yield std::move(v);
  }
}
} // namespace

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
    return parse_loop(to_lines(std::move(input)), ctrl, args_);
  }

  auto optimize(event_order order) -> std::unique_ptr<plugin_parser> override {
    auto args = args_;
    args.builder_options.settings.ordered = order == event_order::ordered;
    return std::make_unique<xsv_parser>(std::move(args));
  }

  friend auto inspect(auto& f, xsv_parser& x) -> bool {
    return f.apply(x.args_);
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
        const auto& input_type = as<record_type>(input_schema);
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
    return f.object(x).fields(f.field("args", x.args_));
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
    // const auto is_parser = true;
    // auto options = xsv_options::try_parse(p, "xsv", is_parser);
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};

    auto opt_parser = xsv_common_parser_options_parser{name()};
    opt_parser.add_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto opts = opt_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    return std::make_unique<xsv_parser>(std::move(*opts));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto options = xsv_options::try_parse_printer_options(p, "xsv");
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
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto opt_parser = xsv_common_parser_options_parser{name(), Sep, ListSep,
                                                       std::string{Null.str()}};
    opt_parser.add_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto opts = opt_parser.get_options(dh);
    for (auto& d : std::move(dh).collect()) {
      if (d.severity == severity::error) {
        throw std::move(d);
      }
    }
    TENZIR_ASSERT(opts);
    opts->name = Name.str();
    return std::make_unique<xsv_parser>(std::move(*opts));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{name()};
    bool no_header = {};
    parser.add("--no-header", no_header);
    parser.parse(p);
    return std::make_unique<xsv_printer>(xsv_options{
      .name = std::string{Name.str()},
      .field_sep = Sep,
      .list_sep = ListSep,
      .null_value = std::string{Null.str()},
      .no_header = no_header,
      .auto_expand = false,
      .allow_comments = false,
    });
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  }
};

using csv_plugin = configured_xsv_plugin<"csv", ',', ';', "">;
using tsv_plugin = configured_xsv_plugin<"tsv", '\t', ',', "-">;
using ssv_plugin = configured_xsv_plugin<"ssv", ' ', ',', "-">;

class read_xsv : public operator_plugin2<parser_adapter<xsv_parser>> {
public:
  auto name() const -> std::string override {
    return "read_xsv";
  }
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto opt_parser = xsv_common_parser_options_parser{name()};
    opt_parser.add_to_parser(parser);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    TRY(auto opts, opt_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<xsv_parser>>(
      xsv_parser{std::move(opts)});
  }
};

class write_xsv : public operator_plugin2<writer_adapter<xsv_printer>> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = xsv_options{};
    auto field_sep_str = located<std::string>{};
    auto list_sep_str = located<std::string>{};
    auto null_value = located<std::string>{};
    auto no_header = bool{};
    TRY(argument_parser2::operator_(name())
          .positional("field_sep", field_sep_str)
          .positional("list_sep", list_sep_str)
          .positional("null_value", null_value)
          .named("no_header", args.no_header)
          .parse(inv, ctx));
    auto field_sep = to_xsv_sep(field_sep_str.inner);
    if (!field_sep) {
      diagnostic::error(field_sep.error())
        .primary(field_sep_str.source)
        .emit(ctx);
      return failure::promise();
    }
    auto list_sep = to_xsv_sep(list_sep_str.inner);
    if (!list_sep) {
      diagnostic::error(list_sep.error()).primary(list_sep_str.source).emit(ctx);
      return failure::promise();
    }
    if (*field_sep == *list_sep) {
      diagnostic::error("field separator and list separator must be "
                        "different")
        .primary(field_sep_str.source)
        .primary(list_sep_str.source)
        .emit(ctx);
      return failure::promise();
    }
    for (auto ch : null_value.inner) {
      if (ch == *field_sep) {
        diagnostic::error("null value conflicts with field separator")
          .primary(field_sep_str.source)
          .primary(null_value.source)
          .emit(ctx);
        return failure::promise();
      }
      if (ch == *list_sep) {
        diagnostic::error("null value conflicts with list separator")
          .primary(list_sep_str.source)
          .primary(null_value.source)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<writer_adapter<xsv_printer>>(xsv_printer{{
      .name = "xsv",
      .field_sep = *field_sep,
      .list_sep = *list_sep,
      .null_value = std::move(null_value.inner),
      .no_header = no_header,
    }});
  }
};

template <detail::string_literal Name, char Sep, char ListSep,
          detail::string_literal Null>
class configured_read_xsv_plugin final
  : public operator_plugin2<parser_adapter<xsv_parser>> {
public:
  auto name() const -> std::string override {
    return fmt::format("read_{}", Name);
  }
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto opt_parser = xsv_common_parser_options_parser{
      name(),
      Sep,
      ListSep,
      std::string{Null.str()},
    };
    opt_parser.add_to_parser(parser);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    TRY(auto opts, opt_parser.get_options(ctx.dh()));
    opts.name = Name.str();
    return std::make_unique<parser_adapter<xsv_parser>>(
      xsv_parser{std::move(opts)});
  }
};

template <detail::string_literal Name, char Sep, char ListSep,
          detail::string_literal Null>
class configured_write_xsv_plugin final
  : public operator_plugin2<writer_adapter<xsv_printer>> {
public:
  auto name() const -> std::string override {
    return fmt::format("write_{}", Name);
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto no_header = bool{};
    TRY(argument_parser2::operator_(name())
          .named("no_header", no_header)
          .parse(inv, ctx));
    return std::make_unique<writer_adapter<xsv_printer>>(xsv_printer{{
      .name = std::string{Name.str()},
      .field_sep = Sep,
      .list_sep = ListSep,
      .null_value = std::string{Null.str()},
      .no_header = no_header,
      .auto_expand = false,
      .allow_comments = false,
    }});
  }
};

using read_csv = configured_read_xsv_plugin<"csv", ',', ';', "">;
using read_tsv = configured_read_xsv_plugin<"tsv", '\t', ',', "-">;
using read_ssv = configured_read_xsv_plugin<"ssv", ' ', ',', "-">;
using write_csv = configured_write_xsv_plugin<"csv", ',', ';', "">;
using write_tsv = configured_write_xsv_plugin<"tsv", '\t', ',', "-">;
using write_ssv = configured_write_xsv_plugin<"ssv", ' ', ',', "-">;

} // namespace tenzir::plugins::xsv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::xsv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::csv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::tsv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::ssv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::read_xsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::read_csv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::read_tsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::read_ssv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::write_xsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::write_csv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::write_tsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::write_ssv)
