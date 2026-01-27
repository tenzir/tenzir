//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/view3.hpp>

#include <arrow/api.h>
#include <re2/re2.h>

#include <string_view>

namespace tenzir::plugins::kv {

namespace {

constexpr auto docs = "https://docs.tenzir.com/formats/kv";

class splitter {
public:
  splitter() = default;

  splitter(const splitter& other)
    : regex_{std::make_unique<re2::RE2>(other.regex_->pattern())} {
  }
  splitter(splitter&&) = default;
  splitter& operator=(splitter&&) = default;

  explicit splitter(located<std::string_view> pattern) {
    auto regex = std::make_unique<re2::RE2>(
      re2::StringPiece{pattern.inner.data(), pattern.inner.size()},
      re2::RE2::CannedOptions::Quiet);
    if (not regex->ok()) {
      diagnostic::error("could not parse regex: {}", regex->error())
        .primary(pattern.source)
        .note("regex must be supported by RE2")
        .docs("https://github.com/google/re2/wiki/Syntax")
        .throw_();
    }
    auto groups = regex->NumberOfCapturingGroups();
    if (groups > 1) {
      diagnostic::error("regex must have at most one capturing group")
        .primary(pattern.source)
        .docs(docs)
        .throw_();
    }
    if (groups != 1) {
      regex = std::make_unique<re2::RE2>(fmt::format("({})", pattern.inner),
                                         re2::RE2::CannedOptions::Quiet);
      if (not regex->ok()) {
        diagnostic::error("internal error: regex could not be parsed "
                          "after adding a capture group")
          .primary(pattern.source)
          .note(regex->error())
          .throw_();
      }
    }
    regex_ = std::move(regex);
  }

  struct separator_info {
    size_t start = 0;
    size_t end = 0;

    auto found() const {
      return end > start;
    }

    auto length() const {
      return end - start;
    }
  };

  using split_result
    = std::tuple<std::string_view, std::string_view, separator_info>;

  static auto make_no_split(std::string_view input) -> split_result {
    return {input, {}, {std::string_view::npos, std::string_view::npos}};
  }

  static auto make_split(std::string_view input, const re2::StringPiece& group)
    -> split_result {
    const auto head = std::string_view{input.data(), group.data()};
    const auto tail = std::string_view{group.data() + group.size(),
                                       input.data() + input.size()};
    const auto sep_start = static_cast<size_t>(group.data() - input.data());
    const auto sep_end = sep_start + group.size();
    return {head, tail, {sep_start, sep_end}};
  }

  auto
  split(std::string_view input, const detail::quoting_escaping_policy& quoting,
        size_t start_offset = 0) const -> split_result {
    TENZIR_ASSERT(regex_);
    TENZIR_ASSERT(regex_->NumberOfCapturingGroups() == 1);
    auto group = re2::StringPiece{};
    while (true) {
      const auto ss = input.substr(start_offset);
      if (not re2::RE2::PartialMatch({ss.data(), ss.size()}, *regex_, &group)) {
        return make_no_split(input);
      }
      auto head = std::string_view{input.data(), group.data()};
      auto is_valid = not quoting.is_inside_of_quotes(input, head.size());
      if (is_valid) {
        return make_split(input, group);
      } else {
        start_offset = head.size() + group.size();
      }
      if (head.size() + group.size() == 0) {
        return make_no_split(input);
      }
    }
    return make_no_split(input);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, splitter& x) -> bool {
    if constexpr (Inspector::is_loading) {
      auto str = std::string{};
      if (not f.apply(str)) {
        return false;
      }
      x.regex_
        = std::make_unique<re2::RE2>(str, re2::RE2::CannedOptions::Quiet);
      if (not x.regex_->ok()) {
        f.set_error(caf::make_error(ec::serialization_error,
                                    fmt::format("could not parse regex: {}",
                                                x.regex_->error())));
        return false;
      }
      if (x.regex_->NumberOfCapturingGroups() != 1) {
        f.set_error(caf::make_error(
          ec::serialization_error,
          fmt::format("expected regex to have 1 capture group, but it has {}",
                      x.regex_->NumberOfCapturingGroups())));
        return false;
      }
      return true;
    } else {
      return f.apply(x.regex_->pattern());
    }
  }

private:
  std::unique_ptr<re2::RE2> regex_;
};

struct kv_args {
  multi_series_builder::options msb_opts_;
  detail::quoting_escaping_policy quoting_;
  splitter field_split_;
  splitter value_split_;

  friend auto inspect(auto& f, kv_args& x) -> bool {
    return f.object(x)
      .pretty_name("kv_parser")
      .fields(f.field("msb_options", x.msb_opts_),
              f.field("quoting", x.quoting_),
              f.field("field_split", x.field_split_),
              f.field("value_split", x.value_split_));
  }
};

class kv_parser;
auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl, kv_parser parser)
  -> generator<table_slice>;

class kv_parser final : public plugin_parser {
public:
  kv_parser() = default;

  explicit kv_parser(kv_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "kv";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl, *this);
  }

  auto parse_line(multi_series_builder& builder, diagnostic_handler& dh,
                  std::string_view line) const -> void {
    auto event = builder.record();
    struct previous_t {
      std::string_view key;
      std::string_view value;
    };
    auto previous = std::optional<previous_t>{};
    const auto commit = [this, &event, &previous]() {
      if (not previous) {
        return;
      }
      auto key = args_.quoting_.unquote_unescape(previous->key);
      if (previous->value.empty()) {
        event.unflattened_field(key).null();
        return;
      }
      auto value = args_.quoting_.unquote_unescape(previous->value);
      event.unflattened_field(key).data_unparsed(std::move(value));
    };
    while (not line.empty()) {
      const auto [head, tail, field_sep]
        = args_.field_split_.split(line, args_.quoting_);
      const auto [key_view, value_view, value_sep]
        = args_.value_split_.split(head, args_.quoting_);
      if (value_sep.found()) {
        commit();
        previous.emplace(key_view, value_view);
      } else if (previous) {
        if (previous->value.empty()) {
          previous->value = value_view;
        } else {
          previous->value = std::string_view{
            previous->value.data(),
            previous->value.size() + field_sep.length() + key_view.length(),
          };
        }
      } else {
        previous.emplace(key_view, value_view);
      }
      if (line == tail) {
        diagnostic::error("`kv` parsing did not make progress")
          .note("check your field splitter")
          .emit(dh);
        return;
      }
      line = tail;
    }
    commit();
  }

  auto parse_strings(const arrow::StringArray& input,
                     diagnostic_handler& diagnostics) const
    -> std::vector<series> {
    auto dh = transforming_diagnostic_handler{
      diagnostics,
      [](auto diag) {
        diag.message = fmt::format("parse_kv: {}", diag.message);
        return diag;
      },
    };
    auto builder = multi_series_builder{args_.msb_opts_, dh};
    for (auto&& line : values(string_type{}, input)) {
      if (not line) {
        builder.null();
        continue;
      }
      parse_line(builder, dh, *line);
    }
    return builder.finalize();
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     operator_control_plane& ctrl) const
    -> std::vector<series> override {
    TENZIR_ASSERT(input);
    return parse_strings(*input, ctrl.diagnostics());
  }

  friend auto inspect(auto& f, kv_parser& x) -> bool {
    return f.apply(x.args_);
  }

  kv_args args_;
};

auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl, kv_parser parser)
  -> generator<table_slice> {
  auto dh = transforming_diagnostic_handler{
    ctrl.diagnostics(),
    [](auto diag) {
      diag.message = fmt::format("read_kv: {}", diag.message);
      return diag;
    },
  };
  auto builder = multi_series_builder(parser.args_.msb_opts_, dh);
  for (auto&& line : input) {
    if (not line) {
      co_yield {};
      continue;
    }
    for (auto&& slice : builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    parser.parse_line(builder, ctrl.diagnostics(), *line);
  }
  for (auto&& slice : builder.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

struct kv_writer {
  location operator_location;
  located<std::string> field_sep = {" ", operator_location};
  located<std::string> value_sep = {"=", operator_location};
  located<std::string> list_sep = {",", operator_location};
  located<std::string> flatten = {".", operator_location};
  located<std::string> null = {"", operator_location};

  friend auto inspect(auto& f, kv_writer& x) -> bool {
    return f.object(x)
      .pretty_name("write_kv_args")
      .fields(f.field("operator_location", x.operator_location),
              f.field("field_sep", x.field_sep),
              f.field("value_sep", x.value_sep),
              f.field("list_sep", x.list_sep), f.field("flatten", x.flatten),
              f.field("null", x.null));
  }

  auto add(argument_parser2& parser) {
    parser.named_optional("field_separator", field_sep);
    parser.named_optional("value_separator", value_sep);
    parser.named_optional("list_separator", list_sep);
    parser.named_optional("flatten_separator", flatten);
    parser.named_optional("null_value", null);
  };

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    TRY(check_no_substrings(dh, {{"flatten_separator", flatten},
                                 {"field_separator", field_sep},
                                 {"value_separator", value_sep},
                                 {"list_separator", list_sep},
                                 {"null_value", null}}));
    TRY(check_non_empty("field_separator", field_sep, dh));
    TRY(check_non_empty("value_separator", field_sep, dh));
    TRY(check_non_empty("list_separator", field_sep, dh));
    return {};
  }

  auto print(auto out, view3<record> r) const {
    auto it = r.begin();
    if (it == r.end()) {
      return out;
    }
    {
      const auto& [k, v] = *it;
      // We dispatch the key through `print`, in order to deal with separators.
      out = print(out, k);
      out = fmt::format_to(out, "{}", value_sep.inner);
      out = print(out, v);
      ++it;
    }
    for (; it != r.end(); ++it) {
      const auto& [k, v] = *it;
      out = fmt::format_to(out, "{}", field_sep.inner);
      out = print(out, k);
      out = fmt::format_to(out, "{}", value_sep.inner);
      out = print(out, v);
    }
    return out;
  }

  auto print(auto out, view3<list> l) const {
    auto it = l.begin();
    if (it == l.end()) {
      return out;
    }
    out = print(out, *it);
    ++it;
    for (; it != l.end(); ++it) {
      out = fmt::format_to(out, "{}", list_sep.inner);
      out = print(out, *it);
    }
    return out;
  }

  template <typename It>
  auto print(It out, data_view3 v) const -> It {
    return match(
      v,
      [&](const caf::none_t&) -> It {
        return fmt::format_to(out, "{}", null.inner);
      },
      [&](const auto& scalar) -> It {
        auto formatted = fmt::format("{}", scalar);
        auto needs_quoting
          = formatted.find(field_sep.inner) != formatted.npos
            or formatted.find(value_sep.inner) != formatted.npos
            or formatted.find(list_sep.inner) != formatted.npos
            or (not null.inner.empty()
                and formatted.find(null.inner) != formatted.npos);
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
        return out;
      },
      [&](const view3<list>& l) -> It {
        return print(out, l);
      },
      [&](const view3<record>&) -> It {
        // We assume that the record has been flattend. Hence we should never
        // enter this recursion.
        TENZIR_UNREACHABLE();
        return out;
      });
  }
};

class write_kv_operator final : public crtp_operator<write_kv_operator> {
public:
  auto name() const -> std::string override {
    return "write_kv";
  }

  write_kv_operator() = default;
  write_kv_operator(kv_writer writer) : writer_{std::move(writer)} {
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto operator()(generator<table_slice> input, operator_control_plane&) const
    -> generator<chunk_ptr> {
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto resolved_slice
        = flatten(resolve_enumerations(slice), writer_.flatten.inner).slice;
      auto out = std::vector<char>{};
      auto out_iter = std::back_inserter(out);
      for (auto&& row : values3(resolved_slice)) {
        out_iter = writer_.print(out_iter, row);
        *out_iter++ = '\n';
      }
      co_yield chunk::make(std::exchange(out, {}));
    }
  }

  friend auto inspect(auto& f, write_kv_operator& x) -> bool {
    return f.apply(x.writer_);
  }

private:
  kv_writer writer_;
};

class kv_plugin final : public virtual parser_plugin<kv_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"kv", docs};
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    parser.add(field_split, "<field_split>");
    parser.add(value_split, "<value_split>");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto msb_opts = msb_parser.get_options(dh);
    for (auto&& diag : std::move(dh).collect()) {
      if (diag.severity == severity::error) {
        throw diag;
      }
    }
    msb_opts->settings.default_schema_name = "tenzir.kv";
    return std::make_unique<kv_parser>(kv_args{
      std::move(*msb_opts),
      detail::quoting_escaping_policy{},
      splitter{*field_split},
      splitter{*value_split},
    });
  }
};

class read_kv : public operator_plugin2<parser_adapter<kv_parser>> {
public:
  auto name() const -> std::string override {
    return "read_kv";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    parser.named("field_split", field_split);
    parser.named("value_split", value_split);
    auto msb_parser = multi_series_builder_argument_parser{};
    auto quoting = detail::quoting_escaping_policy{};
    msb_parser.add_all_to_parser(parser);
    parser.named_optional("quotes", quoting.quotes);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    opts.settings.default_schema_name = "tenzir.kv";
    return std::make_unique<parser_adapter<kv_parser>>(kv_parser{{
      std::move(opts),
      std::move(quoting),
      splitter{std::move(*field_split)},
      splitter{std::move(*value_split)},
    }});
  }
};

class write_kv : public operator_plugin2<write_kv_operator> {
public:
  auto name() const -> std::string override {
    return "write_kv";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto writer = kv_writer{inv.self.get_location()};
    writer.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(writer.validate(ctx));
    return std::make_unique<write_kv_operator>(std::move(writer));
  }
};

class parse_kv : public function_plugin {
public:
  auto name() const -> std::string override {
    return "parse_kv";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto field_split = std::optional<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = std::optional<located<std::string>>{
      std::in_place,
      "=",
      location::unknown,
    };
    auto quoting = detail::quoting_escaping_policy{};
    parser.positional("input", input, "string");
    parser.named("field_split", field_split);
    parser.named("value_split", value_split);
    parser.named_optional("quotes", quoting.quotes);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_policy_to_parser(parser);
    msb_parser.add_settings_to_parser(
      parser, true, multi_series_builder_argument_parser::merge_option::hidden);
    TRY(parser.parse(inv, ctx));
    TRY(auto msb_opts, msb_parser.get_options(ctx));
    return function_use::make([input = std::move(input),
                               parser = kv_parser{{
                                 std::move(msb_opts),
                                 std::move(quoting),
                                 splitter{std::move(*field_split)},
                                 splitter{std::move(*value_split)},
                               }}](evaluator eval, session ctx) {
      return map_series(eval(input), [&](series values) -> multi_series {
        if (values.type.kind().is<null_type>()) {
          return values;
        }
        auto strings = try_as<arrow::StringArray>(&*values.array);
        if (not strings) {
          diagnostic::warning("expected `string`, got `{}`", values.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, values.length());
        }
        auto output = parser.parse_strings(*strings, ctx.dh());
        return multi_series{std::move(output)};
      });
    });
  }
};

class print_kv : public function_plugin {
public:
  auto name() const -> std::string override {
    return "print_kv";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto writer = kv_writer{};
    parser.positional("input", input, "record");
    writer.add(parser);
    TRY(parser.parse(inv, ctx));
    TRY(writer.validate(ctx));
    return function_use::make([input = std::move(input),
                               writer = std::move(writer)](evaluator eval,
                                                           session ctx) {
      return map_series(eval(input), [&](series values) -> multi_series {
        if (values.type.kind().is<null_type>()) {
          return series::null(string_type{}, values.length());
        }
        if (values.type.kind() != type{record_type{}}.kind()) {
          diagnostic::warning("expected `record`, got `{}`", values.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(string_type{}, values.length());
        }
        const auto struct_array
          = std::dynamic_pointer_cast<arrow::StructArray>(values.array);
        TENZIR_ASSERT(struct_array);
        auto [flattend_type, flattend_array, _]
          = flatten(values.type, struct_array, writer.flatten.inner);
        auto [resolved_type, resolved_array] = resolve_enumerations(
          as<record_type>(flattend_type), flattend_array);
        auto builder = type_to_arrow_builder_t<string_type>{};
        auto buffer = std::string{};
        for (auto row : values3(*resolved_array)) {
          if (not row) {
            check(builder.AppendNull());
            continue;
          }
          buffer.clear();
          writer.print(std::back_inserter(buffer), *row);
          check(builder.Append(buffer));
        }
        return series{string_type{}, check(builder.Finish())};
      });
    });
  }
};
} // namespace

} // namespace tenzir::plugins::kv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::kv_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::read_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::write_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::parse_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::print_kv)
