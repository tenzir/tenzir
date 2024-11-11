//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql2/plugin.hpp>

// Both Boost.Regex and RE2 are used:
//  - Boost.Regex is used for actual grokking
//  - RE2 is used for parsing the patterns we're given
//
// RE2 can't be used for both, because it doesn't support all the regex features
// the built-in patterns need.
//
// Boost.Regex _could_ be used for both, but it's slow, so we're using RE2 where
// we can.
#include <boost/regex.hpp>
#include <caf/make_copy_on_write.hpp>
#include <re2/re2.h>

#include <algorithm>
#include <ranges>
#include <span>

namespace caf {
template <>
struct inspector_access<boost::regex> : inspector_access_base<boost::regex> {
  template <typename Inspector>
  static auto apply(Inspector& f, boost::regex& x) {
    auto str = x.str();
    auto result = f.apply(str);
    if constexpr (Inspector::is_loading) {
      x.assign(str);
    }
    return result;
  }
};
} // namespace caf

namespace tenzir::plugins::grok {

namespace {

constexpr std::string_view builtin_patterns_strings[] =
#include "./grok-patterns/patterns.inc"
  ;

enum class capture_type {
  // The three options below (string, int, float)
  // are grok standard options

  // %{::string}
  string,
  // %{::int}
  integer,
  // %{::float}
  floating,

  // Tenzir extension: use simple_data_parser to infer types
  infer,

  // Replacement fields without a NAME, only a SYNTAX
  unnamed,

  // "implicit" named regex capture group,
  // without %{...},
  // but with (?<NAME>...) or (?'NAME'...)
  // Also used when no explicit CONVERSION is set
  implicit,
};

template <typename Inspector>
bool inspect(Inspector& f, capture_type& x) {
  return detail::inspect_enum_str(
    f, x, {"string", "integer", "floating", "infer", "unnamed", "implicit"});
}

struct pattern_store;

struct pattern {
  pattern() = default;

  explicit pattern(std::string p, location loc = location::unknown)
    : raw_pattern(std::move(p)), loc{std::move(loc)} {
  }
  explicit pattern(located<std::string> p)
    : pattern{std::move(p.inner), std::move(p.source)} {
  }

  // Resolve this pattern, using `patterns` pattern store.
  //
  // Replaces %{replacement fields} in `raw_pattern` with the corresponding
  // pattern found in `patterns`. Stores the complete regex in
  // `resolved_pattern`, and all the found named captures (both implicit regex
  // ones like `(?<NAME>EXPRESSION)` and replacement fields) in `named_captures`.
  void resolve(const pattern_store& patterns, bool allow_recursion);

  friend auto inspect(auto& f, pattern& x) -> bool {
    return f.object(x)
      .pretty_name("grok_pattern")
      .fields(f.field("raw", x.raw_pattern),
              f.field("resolved", x.resolved_pattern),
              f.field("named_captures", x.named_captures));
  }

  // The grok pattern itself
  std::string raw_pattern;
  location loc;
  // Resolved regex
  std::optional<boost::regex> resolved_pattern{std::nullopt};
  // List of all the named captures in `resolved_pattern`
  std::vector<std::pair<std::string, capture_type>> named_captures{};
};

struct pattern_store : public caf::ref_counted {
  pattern_store() = default;

  explicit pattern_store(std::span<const std::string_view> input) {
    add(input);
  }

  auto copy() const -> pattern_store* {
    return new pattern_store{patterns};
  }

  void add(std::span<const std::string_view> input) {
    for (auto&& in : input) {
      for (auto&& line : detail::split(in, "\n")) {
        parse_line(line);
      }
    }
    resolve_all();
  }

  void add(std::string_view input) {
    for (auto&& line : detail::split(input, "\n")) {
      parse_line(line);
    }
    resolve_all();
  }

  void resolve_all() {
    for (auto& [name, pattern] : patterns) {
      if (pattern.resolved_pattern) {
        continue;
      }
      pattern.resolve(*this, true);
    }
  }

  friend auto inspect(auto& f, pattern_store& x) -> bool {
    return f.apply(x.patterns);
  }

  std::unordered_map<std::string, pattern> patterns{};

private:
  pattern_store(const std::unordered_map<std::string, pattern>& other)
    : patterns(other) {
  }

  void parse_line(std::string_view line);
};

void pattern::resolve(const pattern_store& patterns, bool allow_recursion) {
  auto is_escaped = []<typename T>(const T* pattern_begin,
                                   const T* match_begin) {
    int backslash_count = 0;
    auto end = std::make_reverse_iterator(pattern_begin);
    for (auto it = std::make_reverse_iterator(match_begin); it != end; ++it) {
      if (*it != '\\') {
        break;
      }
      ++backslash_count;
    }
    // If the number of backslashes before the pattern is odd
    //  (like "\(?<foo>..." or "\\\(?<foo>...")
    //  -> '(' is actually escaped
    //  -> not a named capture, skip
    return (backslash_count % 2) == 1;
  };
  // First, find all "implicit named captures":
  // (?<NAME>EXPRESSION), (?P<NAME>EXPRESSION), or (?'NAME'EXPRESSION),
  // and add them to the list of named captures
  //
  // Boost.Regex doesn't give us a way of iterating through all the named
  // captures in a match_result, and retrieving their names.
  // Only the hashes of the names are stored, so we can only query by-name.
  // We can't query the list of all the names of the captures.
  // Thus, we need to maintain a list of named captures ourselves.
  {
    static const auto expr
      = re2::RE2(R"((\(\?(?:P?<(\w+)>|'(\w+)')))", re2::RE2::Quiet);
    TENZIR_ASSERT(expr.ok());
    re2::StringPiece str_re2{raw_pattern.data(), raw_pattern.size()};
    re2::StringPiece capture{}, name{};
    while (re2::RE2::FindAndConsume(&str_re2, expr, &capture, &name)) {
      if (is_escaped(raw_pattern.data(), capture.data())) {
        continue;
      }
      named_captures.emplace_back(std::string{name}, capture_type::implicit);
    }
  }
  // Then, find all "replacement fields"
  // %{SYNTAX:NAME:CONVERSION},
  // and resolve them
  {
    static const auto expr = re2::RE2("(%{.*?})", re2::RE2::Quiet);
    TENZIR_ASSERT(expr.ok());
    std::string result_pattern{};
    re2::StringPiece str_re2{raw_pattern.data(), raw_pattern.size()};
    const auto* previous_match_end = str_re2.begin();
    re2::StringPiece replacement_field{};
    while (re2::RE2::FindAndConsume(&str_re2, expr, &replacement_field)) {
      if (is_escaped(raw_pattern.data(), replacement_field.data())) {
        continue;
      }
      // RE2 is slow with subpattern captures:
      //  -> get the part inside the {braces} manually
      auto replacement_field_inner
        = replacement_field.substr(2, replacement_field.size() - 3);
      if (replacement_field_inner.empty()) {
        diagnostic::error("invalid replacement field")
          .note("empty fields are disallowed")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      }
      auto elems = detail::split(replacement_field_inner, ":");
      TENZIR_ASSERT(not elems.empty());
      if (elems.size() > 3) {
        diagnostic::error("invalid replacement field")
          .note("up to three :colon-delimited: fields allowed")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      }
      if (elems[0].empty()) {
        diagnostic::error("invalid replacement field")
          .note("SYNTAX-field can't be empty")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      }
      // Find matching pattern for SYNTAX
      auto syntax = std::string{elems[0]};
      auto subpattern_it = patterns.patterns.find(syntax);
      if (subpattern_it == patterns.patterns.end()) {
        diagnostic::error("invalid replacement field")
          .note("SYNTAX not found")
          .hint("field: `{}`, SYNTAX: `{}`", std::string{replacement_field},
                syntax)
          .throw_();
      }
      auto& subpattern = subpattern_it->second;
      if (not subpattern.resolved_pattern) {
        // SYNTAX hasn't been resolved, recurse
        //
        // Nasty const_cast to allow us to take `patterns` by const-ref.
        //
        // This code path will only be reached when resolving patterns that may
        // reference one another, either built-in or user-provided ones.
        // In these cases, we have exclusive ownership of `patterns`,
        // so we're free to modify it, despite the CoW semantics of `patterns`.
        //
        // If we don't have exclusive ownership, we're resolving the
        // user-provided pattern used for parsing input. That pattern can only
        // reference other patterns that either exist and have already been
        // resolved, or don't exist at all. We thus can never reach here.
        TENZIR_ASSERT(allow_recursion);
        TENZIR_ASSERT(patterns.get_reference_count() == 1);
        const_cast<pattern&>(subpattern).resolve(patterns, allow_recursion);
      }
      auto name = elems.size() > 1 ? std::string{elems[1]} : "";
      if (name.starts_with('[')) {
        // Elastic Common Schema name:
        // [foo][bar] -> foo.bar
        auto parser = +(ignore(parsers::ch<'['>) >> +(parsers::printable - ']')
                        >> ignore(parsers::ch<']'>))
                      >> ~ignore(parsers::ch<'?'>); // Sometimes there's a '?'
                                                    // at the end of the field.
                                                    // I have no idea what that
                                                    // means, so just ignore it.
        std::vector<std::string> items;
        auto f = name.begin();
        bool s = parser(f, name.end(), items);
        if (!s || f != name.end()) {
          diagnostic::error("invalid replacement field")
            .note("invalid NAME")
            .hint("field: `{}`, NAME: `{}`", std::string{replacement_field},
                  name)
            .throw_();
        }
        name = fmt::to_string(fmt::join(items, "."));
      }
      // Handle CONVERSION field, default to `implicit`,
      // which will be turned to `infer` or `string`, based on the `--raw` flag
      capture_type conversion{capture_type::implicit};
      if (elems.size() > 2) {
        if (elems[2] == "infer") {
          conversion = capture_type::infer;
        } else if (elems[2] == "string") {
          conversion = capture_type::string;
        }
        if (elems[2] == "int") {
          conversion = capture_type::integer;
        } else if (elems[2] == "long") {
          conversion = capture_type::integer;
        } else if (elems[2] == "float") {
          conversion = capture_type::floating;
        } else {
          diagnostic::error("invalid replacement field")
            .note("invalid CONVERSION")
            .hint("field: `{}`, CONVERSION: `{}`",
                  std::string{replacement_field}, elems[2])
            .throw_();
        }
      }
      const auto get_duplicate_capture_name = [&](const std::string& name) {
        auto&& r = named_captures | std::views::keys;
        return std::ranges::find(r, name).base();
      };
      // Replace the replacement field
      result_pattern.append(previous_match_end, replacement_field.begin());
      if (not name.empty()) {
        // NAME given, save it to named_captures,
        // and replace with (?<NAME>EXPRESSION)
        //
        // Replace any possible previous occurrence with the same name
        // in `named_captures`
        if (auto duplicate_it = get_duplicate_capture_name(name);
            duplicate_it != named_captures.end()) {
          named_captures.erase(duplicate_it);
        }
        named_captures.emplace_back(name, conversion);
        auto replacement
          = fmt::format("(?<{}>{})", name, subpattern.resolved_pattern->str());
        result_pattern.append(replacement);
      } else {
        // No NAME given, use SYNTAX as the name
        if (auto duplicate_it = get_duplicate_capture_name(syntax);
            duplicate_it != named_captures.end()) {
          named_captures.erase(duplicate_it);
        }
        named_captures.emplace_back(syntax, capture_type::unnamed);
        auto replacement = fmt::format("(?<{}>{})", syntax,
                                       subpattern.resolved_pattern->str());
        result_pattern.append(replacement);
      }
      // We'll also have all the same named captures as the subpattern,
      // except if they have a name that we already have saved:
      // we don't want subpattern captures to overwrite anything we have in the
      // main pattern.
      //
      // We expect `named_captures` to be quite small, so O(n) lookup is okay
      std::ranges::copy_if(subpattern.named_captures,
                           std::back_inserter(named_captures),
                           [&](const auto& capture) {
                             return get_duplicate_capture_name(capture.first)
                                    == named_captures.end();
                           });
      previous_match_end = replacement_field.end();
    }
    result_pattern.append(previous_match_end, str_re2.end());
    resolved_pattern.emplace(result_pattern, boost::regex_constants::no_except);
    if (resolved_pattern->empty()) {
      diagnostic::error("invalid regular expression")
        .hint("regex: `{}`", result_pattern)
        .throw_();
    }
  }
}

void pattern_store::parse_line(std::string_view line) {
  if (line.empty()) {
    return;
  }
  if (line.starts_with('#')) {
    return;
  }
  auto parts = detail::split(line, " ", 1);
  TENZIR_ASSERT(not patterns.contains(std::string{parts[0]}));
  patterns.emplace(std::string{parts[0]}, std::string{parts[1]});
}

auto& get_builtin_pattern_store() {
  // Using CoW to avoid copying the builtin patterns unless we need to
  static auto store = caf::make_copy_on_write<pattern_store>(
    std::span{builtin_patterns_strings});
  return store;
}

class grok_parser final : public plugin_parser {
  friend auto parse_loop(generator<std::optional<std::string_view>> input,
                         diagnostic_handler& dh,
                         grok_parser parser) -> generator<table_slice>;

public:
  grok_parser() = default;

  grok_parser(std::optional<std::string> pattern_definitions,
              located<std::string> pattern, bool indexed_captures,
              bool include_unnamed, multi_series_builder::options opts)
    : patterns_{get_builtin_pattern_store()},
      input_pattern_{std::move(pattern)},
      indexed_captures_{indexed_captures},
      include_unnamed_{include_unnamed},
      opts_{std::move(opts)} {
    if (pattern_definitions) {
      patterns_.unshared().add(*pattern_definitions);
    }
    TENZIR_ASSERT_EXPENSIVE(std::ranges::all_of(
      patterns_->patterns | std::views::values, [](const auto& p) -> bool {
        return p.resolved_pattern.has_value();
      }));
    input_pattern_.resolve(*patterns_, false);
  }

  auto name() const -> std::string override {
    return "grok";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl.diagnostics(), *this);
  }

  auto parse_line(multi_series_builder& builder, diagnostic_handler& dh,
                  std::string_view line) const -> bool {
    auto matches = boost::cmatch{};
    try {
      if (not boost::regex_match(line.begin(), line.end(), matches,
                                 *input_pattern_.resolved_pattern)) {
        diagnostic::warning("pattern could not be matched")
          .hint("input: `{}`", line)
          .hint("pattern: `{}`", input_pattern_.resolved_pattern->str())
          .primary(input_pattern_.loc)
          .emit(dh);
        builder.null();
        return false;
      }
    } catch (const boost::regex_error& e) {
      if (e.code() != boost::regex_constants::error_complexity) {
        throw;
      }
      diagnostic::warning("failed to apply grok pattern due to its complexity")
        .note("example input: {:?}", line)
        .hint("try to simplify or optimize your grok pattern")
        .hint("pattern: `{}`", input_pattern_.resolved_pattern->str())
        .primary(input_pattern_.loc)
        .emit(dh);
      builder.null();
      return false;
    }
    auto record = builder.record();
    auto add_field = [&](std::string_view name, const boost::csub_match& match,
                         capture_type type) {
      if (!match.matched) {
        if (type != capture_type::unnamed or include_unnamed_) {
          record.field(name).null();
        }
        return;
      }
      switch (type) {
        case capture_type::unnamed:
          if (not include_unnamed_) {
            return;
          }
          [[fallthrough]];
        case capture_type::implicit:
          record.field(name).data_unparsed(match.str());
          return;
        case capture_type::infer:
          record.field(name).data_unparsed(
            std::string_view{match.first, match.second});
          return;
        case capture_type::string:
          record.field(name).data(std::string{match.first, match.second});
          return;
        case capture_type::integer:
          if (auto r = to<int64_t>(match.str())) {
            record.field(name).data(*r);
            return;
          }
          // TODO: Should this be a warning?
          record.field(name).null();
          return;
        case capture_type::floating:
          if (auto r = to<double>(match.str())) {
            record.field(name).data(*r);
            return;
          }
          record.field(name).null();
          return;
      }
      TENZIR_UNREACHABLE();
    };
    if (indexed_captures_) {
      for (int i = 0; i < static_cast<int>(
                        input_pattern_.resolved_pattern->mark_count() + 1);
           ++i) {
        const auto& match = matches[i];
        // Find the same capture as a named capture,
        // to get the name and conversion type to use.
        // If there isn't a matching named capture,
        // use the (stringified) index as the field name
        if (auto named_capture_it
            = std::ranges::find_if(input_pattern_.named_captures,
                                   [&](const auto& elem) {
                                     const auto& other_match
                                       = matches[elem.first];
                                     return match == other_match;
                                   });
            named_capture_it != input_pattern_.named_captures.end()) {
          const auto& [name, type] = *named_capture_it;
          TENZIR_ASSERT(not name.empty());
          add_field(name, match, type);
        } else {
          const auto type = capture_type::implicit;
          add_field(std::to_string(i), match, type);
        }
      }
    } else {
      for (auto&& [name, type] : input_pattern_.named_captures) {
        TENZIR_ASSERT(not name.empty());
        add_field(name, matches[name], type);
      }
    }
    return true;
  }

  auto parse_strings(const arrow::StringArray& input,
                     diagnostic_handler& dh) const -> std::vector<series> {
    auto tdh = transforming_diagnostic_handler{
      dh, [](auto diag) {
        diag.message = fmt::format("grok parser: {}", diag.message);
        return diag;
      }};
    auto builder = multi_series_builder{opts_, tdh};
    for (auto&& line : values(string_type{}, input)) {
      if (not line) {
        builder.null();
        continue;
      }
      parse_line(builder, tdh, *line);
    }
    return builder.finalize();
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     operator_control_plane& ctrl) const
    -> std::vector<series> override {
    TENZIR_ASSERT(input);
    return parse_strings(*input, ctrl.diagnostics());
  }

  friend auto inspect(auto& f, grok_parser& x) -> bool {
    auto get_patterns = [&x]() -> decltype(auto) {
      return *x.patterns_;
    };
    auto set_patterns = [&x](auto p) {
      x.patterns_ = caf::make_copy_on_write<pattern_store>(p);
      return true;
    };
    return f.object(x)
      .pretty_name("grok_parser")
      .fields(f.field("patterns", get_patterns, set_patterns),
              f.field("input_pattern", x.input_pattern_),
              f.field("indexed_captures", x.indexed_captures_),
              f.field("include_unnamed", x.include_unnamed_),
              f.field("opts", x.opts_));
  }

private:
  // FIXME: The CoW semantics aren't really being taken advantage of here,
  // because inspect() has to create a copy of this every time.
  caf::intrusive_cow_ptr<pattern_store> patterns_{
    caf::make_copy_on_write<pattern_store>()};
  pattern input_pattern_{};
  bool indexed_captures_{false};
  bool include_unnamed_{false};
  multi_series_builder::options opts_;
};

auto parse_loop(generator<std::optional<std::string_view>> input,
                diagnostic_handler& dh,
                grok_parser parser) -> generator<table_slice> {
  auto tdh = transforming_diagnostic_handler{
    dh,
    [](auto diag) {
      diag.message = fmt::format("grok parser: {}", diag.message);
      return diag;
    },
  };
  auto builder = multi_series_builder(parser.opts_, tdh);
  for (auto&& line : input) {
    if (not line) {
      co_yield {};
      continue;
    }
    for (auto&& slice : builder.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    if (not parser.parse_line(builder, tdh, *line)) {
      builder.remove_last();
    }
  }
  for (auto&& slice : builder.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

class plugin final : public virtual parser_plugin<grok_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser
      = argument_parser{"grok", "https://docs.tenzir.com/operators/grok"};
    auto pattern_definitions = std::optional<std::string>{};
    auto raw_pattern = located<std::string>{};
    auto indexed_captures = false;
    auto include_unnamed = false;
    parser.add(raw_pattern, "<pattern>");
    parser.add("--pattern-definitions", pattern_definitions, "<patterns>");
    parser.add("--indexed-captures", indexed_captures);
    parser.add("--include-unnamed", include_unnamed);
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
    msb_opts->settings.default_schema_name = "tenzir.grok";
    return std::make_unique<grok_parser>(std::move(pattern_definitions),
                                         std::move(raw_pattern),
                                         indexed_captures, include_unnamed,
                                         std::move(*msb_opts));
  }
};

class read_grok_plugin : public operator_plugin2<parser_adapter<grok_parser>> {
public:
  auto name() const -> std::string override {
    return "tql2.read_grok";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_(name());
    auto pattern_definitions = std::optional<std::string>{};
    auto raw_pattern = located<std::string>{};
    auto indexed_captures = false;
    auto include_unnamed = false;
    parser.add(raw_pattern, "<pattern>");
    parser.add("pattern_definitions", pattern_definitions);
    parser.add("indexed_captures", indexed_captures);
    parser.add("include_unnamed", include_unnamed);
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx));
    opts.settings.default_schema_name = "tenzir.grok";
    return std::make_unique<parser_adapter<grok_parser>>(grok_parser{
      std::move(pattern_definitions),
      std::move(raw_pattern),
      indexed_captures,
      include_unnamed,
      std::move(opts),
    });
  }
};

class parse_grok_plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parse_grok";
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto pattern = located<std::string>{};
    auto indexed_captures = false;
    auto include_unnamed = false;
    TRY(argument_parser2::function("grok")
          .add(input, "<input>")
          .add(pattern, "<pattern>")
          .add("indexed_captures", indexed_captures)
          .add("include_unnamed", include_unnamed)
          .parse(inv, ctx));
    auto parser = grok_parser{
      std::nullopt,
      std::move(pattern),
      indexed_captures,
      include_unnamed,
      multi_series_builder::options{},
    };
    return function_use::make(
      [input = std::move(input),
       parser = std::move(parser)](evaluator eval, session ctx) -> series {
        auto values = eval(input);
        if (values.type.kind().is<null_type>()) {
          return values;
        }
        auto strings = try_as<arrow::StringArray>(&*values.array);
        if (not strings) {
          diagnostic::warning("expected string, got `{}`", values.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, eval.length());
        }
        auto output = parser.parse_strings(*strings, ctx.dh());
        // TODO: Evaluator can only handle single type atm.
        if (output.size() != 1) {
          diagnostic::warning("varying type within batch is not yet supported")
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, eval.length());
        }
        return std::move(output[0]);
      });
  }
};

} // namespace

} // namespace tenzir::plugins::grok

TENZIR_REGISTER_PLUGIN(tenzir::plugins::grok::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::grok::read_grok_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::grok::parse_grok_plugin)
