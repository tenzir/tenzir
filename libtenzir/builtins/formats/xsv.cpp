//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser.hpp"
#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/async.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/string_literal.hpp"
#include "tenzir/detail/to_xsv_sep.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/multi_series_builder_argument_parser.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/parser_interface.hpp"
#include "tenzir/to_lines.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/plugin_api.hpp"
#include "tenzir/view.hpp"
#include "tenzir/view3.hpp"

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <string_view>

namespace tenzir::plugins::xsv {
namespace {

struct xsv_printer_options {
  located<std::string> field_separator = {};
  located<std::string> list_separator = {};
  located<std::string> null_value = {};
  bool no_header = {};

  auto add(argument_parser2& parser) -> void {
    if (field_separator.inner.empty()) {
      /// XSV case, nothing is set
      parser.named("field_separator", field_separator);
      parser.named("list_separator", list_separator);
      parser.named("null_value", null_value);
    } else {
      /// Configured case
      TENZIR_ASSERT(not list_separator.inner.empty());
      parser.named_optional("list_separator", list_separator);
      parser.named_optional("null_value", null_value);
    }
    if (not no_header) {
      parser.named("no_header", no_header);
    }
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    TRY(check_no_substrings(dh, {{"field_separator", field_separator},
                                 {"list_separator", list_separator},
                                 {"null_value", null_value}}));
    TRY(check_non_empty("field_separator", field_separator, dh));
    TRY(check_non_empty("list_separator", list_separator, dh));
    return {};
  }

  static auto try_parse_printer_options(parser_interface& p)
    -> xsv_printer_options {
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
    if (! field_sep) {
      diagnostic::error(field_sep.error())
        .primary(field_sep_str.source)
        .throw_();
    }
    auto list_sep = to_xsv_sep(list_sep_str.inner);
    if (! list_sep) {
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
    return xsv_printer_options{
      .field_separator
      = located{std::string{1, *field_sep}, field_sep_str.source},
      .list_separator = located{std::string{1, *list_sep}, list_sep_str.source},
      .null_value = std::move(null_value),
      .no_header = no_header,
    };
  }

  friend auto inspect(auto& f, xsv_printer_options& x) -> bool {
    return f.object(x).fields(f.field("field_separator", x.field_separator),
                              f.field("field_separator", x.list_separator),
                              f.field("null_value", x.null_value),
                              f.field("no_header", x.no_header));
  }
};

struct xsv_parser_options {
  std::string name = {};
  std::string field_separator = {};
  std::string list_separator = {};
  std::string null_value = {};
  std::string quotes = "\"\'";
  bool auto_expand = {};
  bool allow_comments = {};
  std::optional<std::vector<std::string>> header = {};
  multi_series_builder::options builder_options = {};

  friend auto inspect(auto& f, xsv_parser_options& x) -> bool {
    return f.object(x).fields(
      f.field("name", x.name), f.field("field_separator", x.field_separator),
      f.field("list_separator", x.list_separator),
      f.field("null_value", x.null_value), f.field("quotes", x.quotes),
      f.field("auto_expand", x.auto_expand),
      f.field("allow_comments", x.allow_comments), f.field("header", x.header),
      f.field("builder_options", x.builder_options));
  }
};

auto warn_on_duplicate_fields(std::vector<std::string> fields, location loc,
                              diagnostic_handler& dh) -> void {
  std::ranges::sort(fields);
  for (auto it = std::ranges::adjacent_find(fields); it != fields.end();
       it = std::ranges::adjacent_find(std::next(it), fields.end())) {
    diagnostic::warning("duplicate field name `{}` in header", *it)
      .primary(loc)
      .note("later values will overwrite earlier ones")
      .emit(dh);
  }
}

auto parse_header(std::string_view line, location loc,
                  const xsv_parser_options& args,
                  const detail::quoting_escaping_policy& quoting,
                  diagnostic_handler& dh)
  -> failure_or<std::vector<std::string>> {
  auto fields = std::vector<std::string>{};
  auto field_text = std::string_view{};
  while (auto split = quoting.split_at_unquoted(line, args.field_separator)) {
    std::tie(field_text, line) = *split;
    fields.emplace_back(quoting.unquote_unescape(field_text));
  }
  fields.emplace_back(quoting.unquote_unescape(line));
  if (fields.empty() and not args.auto_expand) {
    diagnostic::error("failed to parse header").primary(loc).emit(dh);
    return failure::promise();
  }
  warn_on_duplicate_fields(fields, loc, dh);
  return fields;
}

auto extract_header(ast::expression& header_expr,
                    const xsv_parser_options& opts,
                    const detail::quoting_escaping_policy& quoting_options,
                    session ctx) -> failure_or<std::vector<std::string>> {
  TRY(auto header_data, const_eval(header_expr, ctx));
  using ret_t = failure_or<std::vector<std::string>>;
  return match(
    header_data,
    [&](const std::string& s) -> ret_t {
      return parse_header(s, header_expr.get_location(), opts, quoting_options,
                          ctx);
    },
    [&](list& l) -> ret_t {
      if (l.empty() and not opts.auto_expand) {
        diagnostic::error("`header` list is empty")
          .primary(header_expr)
          .emit(ctx);
        return failure::promise();
      }
      auto fields = std::vector<std::string>{};
      fields.reserve(l.size());
      for (auto& v : l) {
        const auto good = match(
          v,
          [&fields](std::string& s) {
            fields.push_back(std::move(s));
            return true;
          },
          [&header_expr, &ctx](auto& v) {
            auto t = type::infer(v);
            diagnostic::error("expected `list<string>`, but got `{}` in list",
                              t ? t->kind() : type_kind{})
              .primary(header_expr)
              .emit(ctx);
            return false;
          });
        if (not good) {
          return failure::promise();
        }
      }
      warn_on_duplicate_fields(fields, header_expr.get_location(), ctx);
      return fields;
    },
    [&](const auto&) -> ret_t {
      const auto t = type::infer(header_data);
      diagnostic::error("`header` must be a `string` or `list<string>`")
        .primary(header_expr, "got `{}`", t ? t->kind() : type_kind{})
        .emit(ctx);
      return failure::promise();
    });
}

struct xsv_common_parser_options_parser : multi_series_builder_argument_parser {
  xsv_common_parser_options_parser(std::string name,
                                   std::string field_sep_default,
                                   std::string list_sep_default,
                                   std::string null_value_default)
    : name_{std::move(name)},
      field_separator_{
        located{std::string{field_sep_default}, location::unknown}},
      list_separator_{
        located{std::string{list_sep_default}, location::unknown}},
      null_value_{located{std::move(null_value_default), location::unknown}},
      mode_{mode::special_optional} {
    settings_.merge = true;
  }

  xsv_common_parser_options_parser(std::string name) : name_{std::move(name)} {
    settings_.merge = true;
  }

  auto add_to_parser(argument_parser& parser) -> void {
    if (mode_ == mode::special_optional) {
      parser.add("--list-sep", list_separator_, "<list-sep>");
      parser.add("--null-value", null_value_, "<null-value>");
    } else {
      field_separator_ = located{"REQUIRED", location::unknown};
      list_separator_ = located{"REQUIRED", location::unknown};
      null_value_ = located{"REQUIRED", location::unknown};
      parser.add(*field_separator_, "<field-sep>");
      parser.add(*list_separator_, "<list-sep>");
      parser.add(*null_value_, "<null-value>");
    }
    parser.add("--allow-comments", allow_comments_);
    parser.add("--header", header_string_, "<header>");
    parser.add("--auto-expand", auto_expand_);
    multi_series_builder_argument_parser::add_policy_to_parser(parser);
    multi_series_builder_argument_parser::add_settings_to_parser(parser, true,
                                                                 false);
  }

  auto add_to_parser(argument_parser2& parser, merge_option add_merge_option,
                     bool header_required) -> void {
    if (mode_ == mode::special_optional) {
      TENZIR_ASSERT(list_separator_);
      TENZIR_ASSERT(null_value_);
      parser.named_optional("list_separator", *list_separator_);
      parser.named_optional("null_value", *null_value_);
    } else {
      field_separator_ = located{"REQUIRED", location::unknown};
      list_separator_ = located{"REQUIRED", location::unknown};
      null_value_ = located{"REQUIRED", location::unknown};
      parser.named("field_separator", *field_separator_);
      parser.named("list_separator", *list_separator_);
      parser.named("null_value", *null_value_);
    }
    if (header_required) {
      parser.named("header", header_expression_.emplace(),
                   "list<string>|string");
    } else {
      parser.named("header", header_expression_, "list<string>|string");
    }
    parser.named("quotes", quotes_);
    parser.named("comments", allow_comments_);
    parser.named("auto_expand", auto_expand_);
    multi_series_builder_argument_parser::add_policy_to_parser(parser);
    multi_series_builder_argument_parser::add_settings_to_parser(
      parser, true, add_merge_option);
  }

  auto get_options(session ctx) -> failure_or<xsv_parser_options> {
    constexpr static auto npos = std::string::npos;
    constexpr static auto overlap
      = [](const std::optional<located<std::string>>& lhs,
           const std::optional<located<std::string>>& rhs) {
          return not lhs->inner.empty() and not rhs->inner.empty()
                 and (lhs->inner.find(rhs->inner) != npos
                      or rhs->inner.find(lhs->inner) != npos);
        };
    if (list_separator_ and overlap(field_separator_, list_separator_)) {
      diagnostic::error("`field_sep` and `list_sep` must not overlap")
        .note("field_sep=`{}`, list_sep=`{}`", field_separator_->inner,
              list_separator_->inner)
        .primary(field_separator_->source)
        .primary(list_separator_->source)
        .emit(ctx);
      return failure::promise();
    }
    if (overlap(field_separator_, null_value_)) {
      diagnostic::error("`field_sep` and `null_value` must not overlap")
        .note("field_sep=`{}`, null_value=`{}`", field_separator_->inner,
              null_value_->inner)
        .primary(field_separator_->source)
        .primary(null_value_->source)
        .emit(ctx);
      return failure::promise();
    }
    if (list_separator_ and overlap(list_separator_, null_value_)) {
      diagnostic::error("`list_sep` and `null_value` must not overlap")
        .note("list_sep=`{}`, null_value=`{}`", list_separator_->inner,
              null_value_->inner)
        .primary(null_value_->source)
        .primary(list_separator_->source)
        .emit(ctx);
      return failure::promise();
    }
    for (const auto q : quotes_->inner) {
      if (field_separator_->inner.find(q) != npos) {
        diagnostic::error("quote character `{}`conflicts with "
                          "`field_sep=\"{}\"`",
                          q, field_separator_->inner)
          .primary(quotes_->source)
          .primary(null_value_->source)
          .emit(ctx);
        return failure::promise();
      }
      if (list_separator_ and list_separator_->inner.find(q) != npos) {
        diagnostic::error("quote character `{}` conflicts with "
                          "`list_sep=\"{}\"`",
                          q, list_separator_->inner)
          .primary(quotes_->source)
          .primary(list_separator_->source)
          .emit(ctx);
        return failure::promise();
      }
      if (null_value_->inner.find(q) != npos) {
        diagnostic::error("quote character `{}` conflicts with "
                          "`null_value=\"{}\"`",
                          q, null_value_->inner)
          .primary(quotes_->source)
          .primary(null_value_->source)
          .emit(ctx);
        return failure::promise();
      }
    }
    TRY(auto opts, multi_series_builder_argument_parser::get_options(ctx));
    auto header = std::optional<std::vector<std::string>>{};
    auto ret = xsv_parser_options{
      .name = "xsv",
      .field_separator = field_separator_->inner,
      .list_separator = list_separator_->inner,
      .null_value = null_value_->inner,
      .quotes = quotes_->inner,
      .auto_expand = auto_expand_,
      .allow_comments = allow_comments_,
      .header = header,
      .builder_options = std::move(opts),
    };
    auto quoting_options = detail::quoting_escaping_policy{
      .quotes = ret.quotes,
      .backslashes_escape = true,
      .doubled_quotes_escape = true,
    };
    if (header_expression_) {
      TRY(header,
          extract_header(*header_expression_, ret, quoting_options, ctx));
    } else if (header_string_) {
      TRY(header, parse_header(header_string_->inner, header_string_->source,
                               ret, quoting_options, ctx));
    }
    ret.header = std::move(header);
    return ret;
  }
  enum class mode {
    all_required,
    special_optional,
  };

protected:
  std::string name_;
  bool allow_comments_{};
  std::optional<located<std::string>> header_string_{};
  std::optional<ast::expression> header_expression_{};
  std::optional<located<std::string>> field_separator_{};
  std::optional<located<std::string>> list_separator_{};
  std::optional<located<std::string>> null_value_{};
  std::optional<located<std::string>> quotes_
    = located{xsv_parser_options{}.quotes, location::unknown};
  bool auto_expand_{};
  mode mode_ = mode::all_required;
};

// ── Args structs for the new executor ──────────────────────────────────────

struct WriteXsvArgs {
  located<std::string> field_separator;
  located<std::string> list_separator;
  located<std::string> null_value;
  bool no_header = false;
};

struct ReadXsvArgs {
  // Core separators — required for read_xsv, pre-filled for csv/tsv/ssv.
  located<std::string> field_separator;
  located<std::string> list_separator;
  located<std::string> null_value;
  // Optional parsing knobs.
  located<std::string> quotes = {"\"'", location::unknown};
  bool auto_expand = false;
  bool allow_comments = false;
  std::optional<ast::expression> header;
  // multi_series_builder policy (from docs).
  std::optional<located<std::string>> schema;
  std::optional<located<std::string>> selector;
  // multi_series_builder settings (from docs).
  bool schema_only = false;
  bool raw = false;
  std::optional<located<std::string>> unflatten_separator;
  bool merge = true;
  // Internal: used in diagnostic messages; set at Describer construction time.
  std::string name = "xsv";
};

// Handles for args that the validate() callback needs to inspect.
struct ReadXsvCommonHandles {
  Argument<ReadXsvArgs, located<std::string>> quotes;
  Argument<ReadXsvArgs, ast::expression> header;
  Argument<ReadXsvArgs, located<std::string>> schema;
  Argument<ReadXsvArgs, located<std::string>> selector;
  Argument<ReadXsvArgs, bool> schema_only;
  Argument<ReadXsvArgs, located<std::string>> unflatten_separator;
};

/// Adds all common read_xsv optional args to the Describer (everything
/// except field/list/null separators, which callers add themselves).
/// Returns handles for the args needed by the validate() callback.
template <class... Impls>
auto add_read_xsv_args(Describer<ReadXsvArgs, Impls...>& d)
  -> ReadXsvCommonHandles {
  auto quotes_arg = d.named_optional("quotes", &ReadXsvArgs::quotes);
  d.named("auto_expand", &ReadXsvArgs::auto_expand);
  d.named("comments", &ReadXsvArgs::allow_comments);
  auto header_arg
    = d.named("header", &ReadXsvArgs::header, "list<string>|string");
  auto schema_arg = d.named("schema", &ReadXsvArgs::schema);
  auto selector_arg = d.named("selector", &ReadXsvArgs::selector);
  auto so_arg = d.named("schema_only", &ReadXsvArgs::schema_only);
  d.named("raw", &ReadXsvArgs::raw);
  auto unflatten_arg
    = d.named("unflatten_separator", &ReadXsvArgs::unflatten_separator);
  d.named("merge", &ReadXsvArgs::merge);
  return {quotes_arg,   header_arg, schema_arg,
          selector_arg, so_arg,     unflatten_arg};
}

auto validate_quote_conflicts(
  DescribeCtx& ctx, const located<std::string>& quotes,
  const located<std::string>& field_separator,
  const std::optional<located<std::string>>& list_separator,
  const std::optional<located<std::string>>& null_value) -> void {
  for (const char q : quotes.inner) {
    if (field_separator.inner.find(q) != std::string::npos) {
      diagnostic::error("quote character `{}` conflicts with "
                        "`field_separator=\"{}\"`",
                        q, field_separator.inner)
        .primary(quotes.source)
        .primary(field_separator.source)
        .emit(ctx);
    }
    if (list_separator and list_separator->inner.find(q) != std::string::npos) {
      diagnostic::error("quote character `{}` conflicts with "
                        "`list_separator=\"{}\"`",
                        q, list_separator->inner)
        .primary(quotes.source)
        .primary(list_separator->source)
        .emit(ctx);
    }
    if (null_value and null_value->inner.find(q) != std::string::npos) {
      diagnostic::error("quote character `{}` conflicts with "
                        "`null_value=\"{}\"`",
                        q, null_value->inner)
        .primary(quotes.source)
        .primary(null_value->source)
        .emit(ctx);
    }
  }
}

auto validate_multi_series_builder_args(DescribeCtx& ctx,
                                        const ReadXsvCommonHandles& common)
  -> void {
  if (auto sc = ctx.get(common.schema);
      sc.has_value() and ctx.get(common.selector).has_value()) {
    diagnostic::error("`schema` and `selector` are mutually exclusive")
      .primary(sc->source)
      .emit(ctx);
  }
  if (auto so = ctx.get(common.schema_only);
      so.has_value() and *so and not ctx.get(common.schema).has_value()
      and not ctx.get(common.selector).has_value()) {
    diagnostic::error("`schema_only` requires `schema` or `selector`").emit(ctx);
  }
  if (auto u = ctx.get(common.unflatten_separator);
      u.has_value() and u->inner.empty()) {
    diagnostic::error("`unflatten_separator` must not be empty")
      .primary(u->source)
      .emit(ctx);
  }
}

// WriteXsv and ReadXsv are defined after xsv_printer_impl and parse_line.
struct xsv_printer_impl {
  xsv_printer_impl(std::string_view sep, std::string_view list_sep,
                   std::string_view null)
    : sep{sep}, list_sep{list_sep}, null{null} {
  }

  template <typename It>
  auto print_header(It& out, const view3<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [k, _] : x) {
      if (! first) {
        out = fmt::format_to(out, "{}", sep);
      } else {
        first = false;
      }
      visitor{out, *this}(k);
    }
    return true;
  }

  template <typename It>
  auto print_values(It& out, const view3<record>& x) const noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (! first) {
        out = fmt::format_to(out, "{}", sep);
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
      if (! printer.null.empty()) {
        sequence_empty = false;
        out = std::copy(printer.null.begin(), printer.null.end(), out);
      }
      return true;
    }

    auto operator()(view3<pattern>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view3<map>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view3<record>) noexcept -> bool {
      TENZIR_UNREACHABLE();
    }

    template <typename T>
    auto operator()(const T& scalar) noexcept -> bool {
      sequence_empty = false;
      auto formatted = std::string{};
      if constexpr (std::same_as<T, int64_t>) {
        formatted = std::to_string(scalar);
      } else if constexpr (std::same_as<T, view3<std::string>>) {
        formatted = scalar;
      } else {
        formatted = fmt::format("{}", data_view{scalar});
      }
      auto needs_quoting = formatted.find(printer.sep) != formatted.npos;
      needs_quoting |= formatted.find(printer.list_sep) != formatted.npos;
      needs_quoting |= formatted == printer.null;
      constexpr static auto escaper = [](auto& f, auto out) {
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
          case '\n':
            *out++ = '\\';
            *out++ = 'n';
            break;
          case '\r':
            *out++ = '\\';
            *out++ = 'r';
            break;
        }
        ++f;
        return;
      };
      constexpr static auto p = printers::escape(escaper);
      if (needs_quoting) {
        *out++ = '"';
      }
      TENZIR_ASSERT(p.print(out, formatted));
      if (needs_quoting) {
        *out++ = '"';
      }
      return true;
    }

    auto operator()(view3<blob> x) noexcept -> bool {
      return (*this)(detail::base64::encode(x));
    }

    auto operator()(const view3<list>& x) noexcept -> bool {
      sequence_empty = true;
      for (const auto& v : x) {
        if (! sequence_empty) {
          out = fmt::format_to(out, "{}", printer.list_sep);
        }
        if (! match(v, *this)) {
          return false;
        }
      }
      return true;
    }

    Iterator& out;
    const xsv_printer_impl& printer;
    bool sequence_empty{true};
  };

  std::string_view sep;
  std::string_view list_sep;
  std::string_view null;
};

auto parse_line(std::string_view line, std::vector<std::string>& fields,
                const size_t original_field_count, auto builder,
                const xsv_parser_options& args, const size_t line_counter,
                const detail::quoting_escaping_policy& quoting,
                diagnostic_handler& dh) -> void {
  auto field_idx = size_t{0};
  auto field_text = std::string_view{};
  for (field_idx = 0; true; ++field_idx) {
    if (line.empty()) {
      if (field_idx < original_field_count) {
        diagnostic::warning("{} parser found too few values in a line",
                            args.name)
          .note("line {} has {} values, but should have {} values",
                line_counter, field_idx, original_field_count)
          .emit(dh);
        builder.unflattened_field(fields[field_idx]).null();
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
        auto excess_values = 1;
        auto it = size_t{0};
        while ((it = quoting.find_not_in_quotes(
                  line, args.field_separator, it + args.field_separator.size()))
               != line.npos) {
          ++excess_values;
        }
        diagnostic::warning("{} parser skipped excess values in a line",
                            args.name)
          .note("line {}: {} extra values were skipped", line_counter,
                excess_values)
          .hint("use `auto_expand=true` to add fields for excess values")
          .emit(dh);
        break;
      }
    }
    auto field = builder.unflattened_field(fields[field_idx]);
    if (auto split = quoting.split_at_unquoted(line, args.field_separator)) {
      std::tie(field_text, line) = *split;
    } else {
      field_text = line;
      line = std::string_view{};
    }
    const auto add_value
      = [&quoting](std::string_view text, std::string_view null_value,
                   auto& builder) {
          if (text == null_value) {
            builder.null();
          } else {
            builder.data_unparsed(quoting.unquote_unescape(text));
          }
        };
    if (args.list_separator.empty()) {
      add_value(field_text, args.null_value, field);
    } else {
      if (auto split
          = quoting.split_at_unquoted(field_text, args.list_separator)) {
        auto list = field.list();
        // Iterate the list
        do {
          auto list_element_text = std::string_view{};
          std::tie(list_element_text, field_text) = *split;
          add_value(list_element_text, args.null_value, list);
        } while (
          (split = quoting.split_at_unquoted(field_text, args.list_separator)));
        // Add the final element (for which the split would have failed)
        add_value(field_text, args.null_value, list);
      } else {
        add_value(field_text, args.null_value, field);
      }
    }
  }
  for (; field_idx < fields.size(); ++field_idx) {
    builder.unflattened_field(fields[field_idx]).null();
  }
}

auto parse_loop(generator<std::optional<std::string_view>> lines,
                operator_control_plane& ctrl, xsv_parser_options args)
  -> generator<table_slice> {
  // Parse header.
  auto it = lines.begin();
  auto line = std::optional<std::string_view>{};
  size_t line_counter = 0;
  const auto quoting_options = detail::quoting_escaping_policy{
    .quotes = args.quotes,
    .backslashes_escape = true,
    .doubled_quotes_escape = true,
  };
  if (not args.header) {
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
      auto parsed_header = parse_header(*line, location::unknown, args,
                                        quoting_options, ctrl.diagnostics());
      if (not parsed_header) {
        co_return;
      } else {
        args.header = std::move(*parsed_header);
      }
      ++it;
      break;
    }
  }
  if (it == lines.end()) {
    co_return;
  }
  TENZIR_ASSERT(args.header);
  // parse the body
  const auto original_field_count = args.header->size();
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
    parse_line(*line, *args.header, original_field_count, r, args, line_counter,
               quoting_options, ctrl.diagnostics());
  }
  for (auto& v : msb.finalize_as_table_slice()) {
    co_yield std::move(v);
  }
}
} // namespace

// ── WriteXsv ────────────────────────────────────────────────────────────────

class WriteXsv final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteXsv(WriteXsvArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (input.rows() == 0) {
      co_return;
    }
    if (not args_.no_header) {
      if (not schema_) {
        schema_ = input.schema();
      } else if (*schema_ != input.schema()) {
        diagnostic::error(
          "multiple schemas are not supported when header is enabled")
          .note("got schema `{}` after schema `{}`", input.schema(), *schema_)
          .emit(ctx);
        co_return;
      }
    }
    auto metadata = chunk_metadata{.content_type = content_type()};
    auto printer = xsv_printer_impl{
      args_.field_separator.inner,
      args_.list_separator.inner,
      args_.null_value.inner,
    };
    auto buffer = std::vector<char>{};
    auto out_iter = std::back_inserter(buffer);
    auto resolved_slice = flatten(resolve_enumerations(input)).slice;
    auto array = check(to_record_batch(resolved_slice)->ToStructArray());
    for (const auto& row : values3(*array)) {
      TENZIR_ASSERT(row);
      if (first_ and not args_.no_header) {
        printer.print_header(out_iter, *row);
        first_ = false;
        out_iter = fmt::format_to(out_iter, "\n");
      }
      const auto ok = printer.print_values(out_iter, *row);
      TENZIR_ASSERT(ok);
      out_iter = fmt::format_to(out_iter, "\n");
    }
    co_await push(chunk::make(std::move(buffer), std::move(metadata)));
  }

private:
  auto content_type() const -> std::string {
    if (args_.field_separator.inner == ",") {
      return "text/csv";
    }
    if (args_.field_separator.inner == "\t") {
      return "text/tab-separated-values";
    }
    return "text/plain";
  }

  WriteXsvArgs args_;
  bool first_ = true;
  Option<type> schema_;
};

// ── ReadXsv ─────────────────────────────────────────────────────────────────

class ReadXsv final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadXsv(ReadXsvArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    quoting_ = detail::quoting_escaping_policy{
      .quotes = args_.quotes.inner,
      .backslashes_escape = true,
      .doubled_quotes_escape = true,
    };
    // ── Build multi_series_builder options from individual args ──────────────
    auto msb_opts = multi_series_builder::options{};
    msb_opts.settings.default_schema_name
      = fmt::format("tenzir.{}", args_.name);
    msb_opts.settings.schema_only = args_.schema_only;
    msb_opts.settings.merge = args_.merge;
    msb_opts.settings.raw = args_.raw;
    if (args_.unflatten_separator) {
      msb_opts.settings.unnest_separator = args_.unflatten_separator->inner;
    }
    if (args_.schema and args_.selector) {
      diagnostic::error("`schema` and `selector` are mutually exclusive")
        .emit(ctx.dh());
      co_return;
    }
    if (args_.schema) {
      auto schema = modules::get_schema(args_.schema->inner);
      if (not schema) {
        if (args_.schema_only) {
          diagnostic::error("schema `{}` does not exist, but `schema_only` "
                            "was specified",
                            args_.schema->inner)
            .primary(args_.schema->source)
            .emit(ctx.dh());
          co_return;
        }
        diagnostic::warning("schema `{}` does not exist", args_.schema->inner)
          .primary(args_.schema->source)
          .hint("if you know the input's shape, define the schema")
          .emit(ctx.dh());
      }
      msb_opts.policy
        = multi_series_builder::policy_schema{args_.schema->inner};
    } else if (args_.selector) {
      auto parsed = parse_selector_value(args_.selector->inner);
      if (not parsed) {
        diagnostic::error("invalid selector `{}`", args_.selector->inner)
          .primary(args_.selector->source)
          .emit(ctx.dh());
        co_return;
      }
      msb_opts.policy = std::move(*parsed);
    }
    // ── Build xsv_parser_options ─────────────────────────────────────────────
    opts_ = xsv_parser_options{
      .name = args_.name,
      .field_separator = args_.field_separator.inner,
      .list_separator = args_.list_separator.inner,
      .null_value = args_.null_value.inner,
      .quotes = args_.quotes.inner,
      .auto_expand = args_.auto_expand,
      .allow_comments = args_.allow_comments,
      .header = {},
      .builder_options = std::move(msb_opts),
    };
    // ── Eagerly evaluate the header expression if provided ───────────────────
    if (args_.header) {
      auto result = const_eval(*args_.header, ctx.dh());
      if (not result) {
        co_return;
      }
      auto header_data = std::move(*result);
      auto maybe_header = match(
        header_data,
        [&](const std::string& s) -> failure_or<std::vector<std::string>> {
          return parse_header(s, args_.header->get_location(), opts_, quoting_,
                              ctx.dh());
        },
        [&](list& l) -> failure_or<std::vector<std::string>> {
          if (l.empty() and not args_.auto_expand) {
            diagnostic::error("`header` list is empty")
              .primary(*args_.header)
              .emit(ctx.dh());
            return failure::promise();
          }
          auto fields = std::vector<std::string>{};
          fields.reserve(l.size());
          for (auto& v : l) {
            if (auto* s = try_as<std::string>(v)) {
              fields.push_back(std::move(*s));
            } else {
              const auto t = type::infer(v);
              diagnostic::error("expected `list<string>`, got `{}` in `header` "
                                "list",
                                t ? t->kind() : type_kind{})
                .primary(*args_.header)
                .emit(ctx.dh());
              return failure::promise();
            }
          }
          warn_on_duplicate_fields(fields, args_.header->get_location(),
                                   ctx.dh());
          return fields;
        },
        [&](const auto&) -> failure_or<std::vector<std::string>> {
          const auto t = type::infer(header_data);
          diagnostic::error("`header` must be a `string` or `list<string>`")
            .primary(*args_.header, "got `{}`", t ? t->kind() : type_kind{})
            .emit(ctx.dh());
          return failure::promise();
        });
      if (not maybe_header) {
        co_return;
      }
      header_ = std::move(*maybe_header);
      original_field_count_ = header_->size();
      opts_.header = std::optional{*header_};
    }
    // ── Build persistent wrapping diagnostic handler and MSB ─────────────────
    dh_ = std::make_unique<transforming_diagnostic_handler>(
      ctx.dh(), [this](diagnostic d) {
        d.message = fmt::format("{} parser: {}", opts_.name, d.message);
        d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                        fmt::format("line {}", line_counter_));
        return d;
      });
    msb_ = multi_series_builder(opts_.builder_options, *dh_);
    co_return;
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not msb_) {
      co_return;
    }
    if (not input or input->size() == 0) {
      co_return;
    }
    auto& dh = *dh_;
    const auto* begin = reinterpret_cast<const char*>(input->data());
    const auto* const end = begin + input->size();
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (buffer_.empty()) {
        process_line({begin, current}, dh);
      } else {
        buffer_.append(begin, current);
        process_line(buffer_, dh);
        buffer_.clear();
      }
      if (*current == '\r') {
        if (current + 1 == end) {
          ended_on_carriage_return_ = true;
        } else if (*(current + 1) == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer_.append(begin, end);
    for (auto& slice : msb_->yield_ready_as_table_slice()) {
      co_await push(std::move(slice));
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (not msb_) {
      co_return FinalizeBehavior::done;
    }
    if (not buffer_.empty()) {
      process_line(buffer_, *dh_);
      buffer_.clear();
    }
    for (auto& slice : msb_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

private:
  auto process_line(std::string_view line, diagnostic_handler& dh) -> void {
    ++line_counter_;
    if (line.empty()) {
      return;
    }
    if (opts_.allow_comments and line.front() == '#') {
      return;
    }
    if (not header_) {
      auto parsed = parse_header(line, location::unknown, opts_, quoting_, dh);
      if (not parsed) {
        return;
      }
      header_ = std::move(*parsed);
      original_field_count_ = header_->size();
      opts_.header = std::optional{*header_};
      return;
    }
    auto r = msb_->record();
    parse_line(line, *header_, *original_field_count_, r, opts_, line_counter_,
               quoting_, dh);
  }

  ReadXsvArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  Option<std::vector<std::string>> header_;
  Option<size_t> original_field_count_;
  size_t line_counter_ = 0;
  xsv_parser_options opts_;
  detail::quoting_escaping_policy quoting_;
  std::unique_ptr<transforming_diagnostic_handler> dh_;
  Option<multi_series_builder> msb_;
};

class xsv_parser final : public plugin_parser {
public:
  xsv_parser() = default;

  explicit xsv_parser(xsv_parser_options args) : args_{std::move(args)} {
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
  xsv_parser_options args_{};
};

class xsv_printer final : public plugin_printer {
public:
  xsv_printer() = default;

  explicit xsv_printer(xsv_printer_options args) : args_{std::move(args)} {
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
        auto printer = xsv_printer_impl{
          args.field_separator.inner,
          args.list_separator.inner,
          args.null_value.inner,
        };
        auto buffer = std::vector<char>{};
        auto out_iter = std::back_inserter(buffer);
        auto resolved_slice = flatten(resolve_enumerations(slice)).slice;
        auto input_schema = resolved_slice.schema();
        auto slice_type = as<record_type>(input_schema);
        auto array = check(to_record_batch(resolved_slice)->ToStructArray());
        for (const auto& row : values3(*array)) {
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
        co_yield chunk::make(std::move(buffer), meta);
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
    if (args_.field_separator.inner == ",") {
      return "text/csv";
    }
    if (args_.field_separator.inner == "\t") {
      return "text/tab-separated-values";
    }
    return "text/plain";
  }

  xsv_printer_options args_;
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
    auto sp = session_provider::make(dh);
    auto opts = opt_parser.get_options(sp.as_session());
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
    auto options = xsv_printer_options::try_parse_printer_options(p);
    return std::make_unique<xsv_printer>(std::move(options));
  }
};

template <detail::string_literal Name, detail::string_literal Sep,
          detail::string_literal ListSep, detail::string_literal Null>
class configured_xsv_plugin final : public virtual parser_parser_plugin,
                                    public virtual printer_parser_plugin {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto opt_parser = xsv_common_parser_options_parser{
      name(),
      std::string{Sep},
      std::string{ListSep},
      std::string{Null},
    };
    opt_parser.add_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto sp = session_provider::make(dh);
    auto opts = opt_parser.get_options(sp.as_session());
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
    return std::make_unique<xsv_printer>(xsv_printer_options{
      .field_separator = located{std::string{Sep}, location::unknown},
      .list_separator = located{std::string{ListSep}, location::unknown},
      .null_value = located{std::string{Null}, location::unknown},
      .no_header = no_header,
    });
  }

  auto name() const -> std::string override {
    return std::string{Name.str()};
  }
};

using csv_plugin = configured_xsv_plugin<"csv", ",", ";", "">;
using tsv_plugin = configured_xsv_plugin<"tsv", "\t", ",", "-">;
using ssv_plugin = configured_xsv_plugin<"ssv", " ", ",", "-">;

class read_xsv : public operator_plugin2<parser_adapter<xsv_parser>>,
                 public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_xsv";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto opt_parser = xsv_common_parser_options_parser{name()};
    opt_parser.add_to_parser(
      parser, multi_series_builder_argument_parser::merge_option::yes, false);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    TRY(auto opts, opt_parser.get_options(ctx));
    return std::make_unique<parser_adapter<xsv_parser>>(
      xsv_parser{std::move(opts)});
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadXsvArgs, ReadXsv>{};
    auto field_sep = d.named("field_separator", &ReadXsvArgs::field_separator);
    auto list_sep = d.named("list_separator", &ReadXsvArgs::list_separator);
    auto null_val = d.named("null_value", &ReadXsvArgs::null_value);
    auto common = add_read_xsv_args(d);
    d.validate(
      [field_sep, list_sep, null_val, common](DescribeCtx& ctx) -> Empty {
        TRY(auto fs, ctx.get(field_sep));
        TRY(auto ls, ctx.get(list_sep));
        TRY(auto nv, ctx.get(null_val));
        auto& dh = ctx;
        (void)check_non_empty("field_separator", fs, dh);
        (void)check_no_substrings(dh, {{"field_separator", fs},
                                       {"list_separator", ls},
                                       {"null_value", nv}});
        auto qs = ctx.get(common.quotes).value_or(ReadXsvArgs{}.quotes);
        validate_quote_conflicts(ctx, qs, fs, std::optional{ls},
                                 std::optional{nv});
        validate_multi_series_builder_args(ctx, common);
        return {};
      });
    return d.without_optimize();
  }
};

template <detail::string_literal Name, detail::string_literal Sep,
          detail::string_literal ListSep, detail::string_literal Null,
          detail::string_literal... mimes>
class configured_read_xsv_plugin final
  : public operator_plugin2<parser_adapter<xsv_parser>>,
    public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return fmt::format("read_{}", Name);
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto opt_parser = xsv_common_parser_options_parser{
      name(),
      std::string{Sep},
      std::string{ListSep},
      std::string{Null.str()},
    };
    opt_parser.add_to_parser(
      parser, multi_series_builder_argument_parser::merge_option::yes, false);
    auto result = parser.parse(inv, ctx);
    TRY(result);
    TRY(auto opts, opt_parser.get_options(ctx));
    opts.name = Name.str();
    return std::make_unique<parser_adapter<xsv_parser>>(
      xsv_parser{std::move(opts)});
  }

  auto describe() const -> Description override {
    // Pre-fill separator defaults; field_separator is fixed and not exposed.
    auto defaults = ReadXsvArgs{};
    defaults.field_separator = {std::string{Sep}, location::unknown};
    defaults.list_separator = {std::string{ListSep}, location::unknown};
    defaults.null_value = {std::string{Null}, location::unknown};
    defaults.name = std::string{Name};
    auto d = Describer<ReadXsvArgs, ReadXsv>{std::move(defaults)};
    auto list_sep
      = d.named_optional("list_separator", &ReadXsvArgs::list_separator);
    auto null_val = d.named_optional("null_value", &ReadXsvArgs::null_value);
    auto common = add_read_xsv_args(d);
    d.validate([list_sep, null_val, common,
                fixed_sep = std::string{Sep}](DescribeCtx& ctx) -> Empty {
      auto fs_loc = located{fixed_sep, location::unknown};
      auto ls = ctx.get(list_sep);
      auto nv = ctx.get(null_val);
      if (ls) {
        (void)check_no_substrings(ctx, {{"field_separator", fs_loc},
                                        {"list_separator", *ls}});
      }
      if (nv) {
        (void)check_no_substrings(ctx, {{"field_separator", fs_loc},
                                        {"null_value", *nv}});
      }
      auto effective_ls
        = ls.value_or(located{std::string{ListSep}, location::unknown});
      auto effective_nv
        = nv.value_or(located{std::string{Null}, location::unknown});
      (void)check_no_substrings(ctx, {{"list_separator", effective_ls},
                                      {"null_value", effective_nv}});
      auto qs = ctx.get(common.quotes).value_or(ReadXsvArgs{}.quotes);
      validate_quote_conflicts(ctx, qs, fs_loc, ls, nv);
      validate_multi_series_builder_args(ctx, common);
      return {};
    });
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {std::string{Name}}, .mime_types = {mimes...}};
  }
};

class write_xsv : public operator_plugin2<writer_adapter<xsv_printer>>,
                  public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_xsv";
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = xsv_printer_options{};
    auto parser = argument_parser2::operator_(name());
    args.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<writer_adapter<xsv_printer>>(
      xsv_printer{std::move(args)});
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteXsvArgs, WriteXsv>{};
    auto field_sep = d.named("field_separator", &WriteXsvArgs::field_separator);
    auto list_sep = d.named("list_separator", &WriteXsvArgs::list_separator);
    auto null_val = d.named("null_value", &WriteXsvArgs::null_value);
    d.named("no_header", &WriteXsvArgs::no_header);
    d.validate([field_sep, list_sep, null_val](DescribeCtx& ctx) -> Empty {
      TRY(auto fs, ctx.get(field_sep));
      TRY(auto ls, ctx.get(list_sep));
      TRY(auto nv, ctx.get(null_val));
      auto& dh = ctx;
      (void)check_non_empty("field_separator", fs, dh);
      (void)check_non_empty("list_separator", ls, dh);
      (void)check_no_substrings(dh, {{"field_separator", fs},
                                     {"list_separator", ls},
                                     {"null_value", nv}});
      return {};
    });
    return d.without_optimize();
  }
};

template <detail::string_literal Name, detail::string_literal Sep,
          detail::string_literal ListSep, detail::string_literal Null>
class configured_write_xsv_plugin final
  : public operator_plugin2<writer_adapter<xsv_printer>>,
    public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return fmt::format("write_{}", Name);
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto opts = xsv_printer_options{
      .field_separator = located{std::string{Sep}, inv.self.get_location()},
      .list_separator = located{std::string{ListSep}, inv.self.get_location()},
      .null_value = located{std::string{Null}, inv.self.get_location()},
    };
    auto parser = argument_parser2::operator_(name());
    opts.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(opts.validate(ctx));
    return std::make_unique<writer_adapter<xsv_printer>>(
      xsv_printer{std::move(opts)});
  }

  auto describe() const -> Description override {
    // Pre-fill separator defaults; field_separator is fixed and not exposed.
    auto d = Describer<WriteXsvArgs, WriteXsv>{WriteXsvArgs{
      .field_separator = {std::string{Sep}, location::unknown},
      .list_separator = {std::string{ListSep}, location::unknown},
      .null_value = {std::string{Null}, location::unknown},
    }};
    auto list_sep
      = d.named_optional("list_separator", &WriteXsvArgs::list_separator);
    auto null_val = d.named_optional("null_value", &WriteXsvArgs::null_value);
    d.named("no_header", &WriteXsvArgs::no_header);
    d.validate([list_sep, null_val,
                fixed_sep = std::string{Sep}](DescribeCtx& ctx) -> Empty {
      auto fs_loc = located{fixed_sep, location::unknown};
      auto ls = ctx.get(list_sep);
      auto nv = ctx.get(null_val);
      if (ls) {
        (void)check_non_empty("list_separator", *ls, ctx);
        (void)check_no_substrings(ctx, {{"field_separator", fs_loc},
                                        {"list_separator", *ls}});
      }
      if (nv) {
        (void)check_no_substrings(ctx, {{"field_separator", fs_loc},
                                        {"null_value", *nv}});
      }
      auto effective_ls
        = ls.value_or(located{std::string{ListSep}, location::unknown});
      auto effective_nv
        = nv.value_or(located{std::string{Null}, location::unknown});
      (void)check_no_substrings(ctx, {{"list_separator", effective_ls},
                                      {"null_value", effective_nv}});
      return {};
    });
    return d.without_optimize();
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {std::string{Name}}};
  }
};

auto make_xsv_parsing_function(ast::expression input, xsv_parser_options opts,
                               detail::quoting_escaping_policy quoting_options)
  -> function_ptr {
  return function_use::make(
    [input = std::move(input), original_field_count = opts.header->size(),
     opts = std::move(opts), quoting = std::move(quoting_options)](
      function_plugin::evaluator eval, session ctx) mutable {
      return map_series(eval(input), [&](series data) -> multi_series {
        if (data.type.kind().is<null_type>()) {
          return data;
        }
        auto strings = try_as<arrow::StringArray>(&*data.array);
        if (not strings) {
          diagnostic::warning("expected `string`, got `{}`", data.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, data.length());
        }
        auto builder = multi_series_builder{opts.builder_options, ctx};
        for (auto&& line : values(string_type{}, *strings)) {
          if (not line) {
            builder.null();
            continue;
          }
          parse_line(*line, *opts.header, original_field_count,
                     builder.record(), opts, 0, quoting, ctx);
        }
        return multi_series{builder.finalize()};
      });
    });
}

class parse_xsv : public function_plugin {
  auto name() const -> std::string override {
    return "parse_xsv";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("input", input, "string");
    auto opt_parser = xsv_common_parser_options_parser{name()};
    opt_parser.add_to_parser(
      parser, multi_series_builder_argument_parser::merge_option::hidden, true);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, opt_parser.get_options(ctx));
    opts.name = "xsv";
    auto quoting_options = detail::quoting_escaping_policy{
      .quotes = opts.quotes,
      .backslashes_escape = true,
      .doubled_quotes_escape = true,
    };
    return make_xsv_parsing_function(std::move(input), std::move(opts),
                                     std::move(quoting_options));
  }
};

template <detail::string_literal Name, detail::string_literal Sep,
          detail::string_literal ListSep, detail::string_literal Null>
class configured_parse_xsv_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return fmt::format("parse_{}", Name);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    parser.positional("input", input, "string");
    auto opt_parser = xsv_common_parser_options_parser{
      name(),
      std::string{Sep},
      std::string{ListSep},
      std::string{Null.str()},
    };
    opt_parser.add_to_parser(
      parser, multi_series_builder_argument_parser::merge_option::hidden, true);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, opt_parser.get_options(ctx));
    opts.name = Name.str();
    auto quoting_options = detail::quoting_escaping_policy{
      .quotes = opts.quotes,
      .backslashes_escape = true,
      .doubled_quotes_escape = true,
    };
    return make_xsv_parsing_function(std::move(input), std::move(opts),
                                     std::move(quoting_options));
  }
};

auto make_xsv_printing_function(ast::expression input, xsv_printer_options opts)
  -> function_ptr {
  return function_use::make([input = std::move(input), opts = std::move(opts)](
                              function_plugin::evaluator eval, session ctx) {
    return map_series(eval(input), [&](series data) -> multi_series {
      if (data.type.kind().is<null_type>()) {
        return series::null(string_type{}, data.length());
      }
      if (data.type.kind() != type{record_type{}}.kind()) {
        diagnostic::warning("expected `record`, got `{}`", data.type.kind())
          .primary(input)
          .emit(ctx);
        return series::null(string_type{}, data.length());
      }
      const auto struct_array
        = std::dynamic_pointer_cast<arrow::StructArray>(data.array);
      TENZIR_ASSERT(struct_array);
      auto [flattend_type, flattend_array, _]
        = flatten(data.type, struct_array, ".");
      auto [resolved_type, resolved_array]
        = resolve_enumerations(as<record_type>(flattend_type), flattend_array);
      auto builder = type_to_arrow_builder_t<string_type>{};
      auto printer = xsv_printer_impl{
        opts.field_separator.inner,
        opts.list_separator.inner,
        opts.null_value.inner,
      };
      auto buffer = std::string{};
      for (auto row : values3(*resolved_array)) {
        if (not row) {
          check(builder.AppendNull());
          continue;
        }
        buffer.clear();
        auto out = std::back_inserter(buffer);
        printer.print_values(out, *row);
        check(builder.Append(buffer));
      }
      return series{string_type{}, check(builder.Finish())};
    });
  });
}

class print_xsv : public function_plugin {
  auto name() const -> std::string override {
    return "print_xsv";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto opts = xsv_printer_options{
      .no_header = true,
    };
    auto parser = argument_parser2::function(name());
    parser.positional("input", input, "record");
    opts.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(opts.validate(ctx));
    return make_xsv_printing_function(std::move(input), std::move(opts));
  }
};

template <detail::string_literal Name, detail::string_literal Sep,
          detail::string_literal ListSep, detail::string_literal Null>
class configured_print_xsv_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return fmt::format("print_{}", Name);
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto opts = xsv_printer_options{
      .field_separator = located{std::string{Sep}, inv.call.get_location()},
      .list_separator = located{std::string{ListSep}, inv.call.get_location()},
      .null_value = located{std::string{Null}, inv.call.get_location()},
      .no_header = true,
    };
    auto parser = argument_parser2::function(name());
    parser.positional("input", input, "record");
    opts.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(opts.validate(ctx));
    return make_xsv_printing_function(std::move(input), std::move(opts));
  }
};

using read_csv = configured_read_xsv_plugin<"csv", ",", ";", "", "text/csv">;
using read_tsv = configured_read_xsv_plugin<"tsv", "\t", ",", "-",
                                            "text/tab-separated-values">;
using read_ssv = configured_read_xsv_plugin<"ssv", " ", ",", "-">;
using write_csv = configured_write_xsv_plugin<"csv", ",", ";", "">;
using write_tsv = configured_write_xsv_plugin<"tsv", "\t", ",", "-">;
using write_ssv = configured_write_xsv_plugin<"ssv", " ", ",", "-">;
using parse_csv = configured_parse_xsv_plugin<"csv", ",", ";", "">;
using parse_tsv = configured_parse_xsv_plugin<"tsv", "\t", ",", "-">;
using parse_ssv = configured_parse_xsv_plugin<"ssv", " ", ",", "-">;
using print_csv = configured_print_xsv_plugin<"csv", ",", ";", "">;
using print_tsv = configured_print_xsv_plugin<"tsv", "\t", ",", "-">;
using print_ssv = configured_print_xsv_plugin<"ssv", " ", ",", "-">;

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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::parse_xsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::parse_csv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::parse_tsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::parse_ssv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::print_xsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::print_csv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::print_tsv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::xsv::print_ssv)
