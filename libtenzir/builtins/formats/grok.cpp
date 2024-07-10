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
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
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
    if constexpr (Inspector::is_loading)
      x.assign(str);
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

  explicit pattern(std::string p) : raw_pattern(std::move(p)) {
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
      if (pattern.resolved_pattern)
        continue;
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
      if (*it != '\\')
        break;
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
      if (is_escaped(raw_pattern.data(), capture.data()))
        continue;
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
      if (is_escaped(raw_pattern.data(), replacement_field.data()))
        continue;
      // RE2 is slow with subpattern captures:
      //  -> get the part inside the {braces} manually
      auto replacement_field_inner
        = replacement_field.substr(2, replacement_field.size() - 3);
      if (replacement_field_inner.empty())
        diagnostic::error("invalid replacement field")
          .note("empty fields are disallowed")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      auto elems = detail::split(replacement_field_inner, ":");
      TENZIR_ASSERT(not elems.empty());
      if (elems.size() > 3)
        diagnostic::error("invalid replacement field")
          .note("up to three :colon-delimited: fields allowed")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      if (elems[0].empty())
        diagnostic::error("invalid replacement field")
          .note("SYNTAX-field can't be empty")
          .hint("field: `{}`", std::string{replacement_field})
          .throw_();
      // Find matching pattern for SYNTAX
      auto syntax = std::string{elems[0]};
      auto subpattern_it = patterns.patterns.find(syntax);
      if (subpattern_it == patterns.patterns.end())
        diagnostic::error("invalid replacement field")
          .note("SYNTAX not found")
          .hint("field: `{}`, SYNTAX: `{}`", std::string{replacement_field},
                syntax)
          .throw_();
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
        if (!s || f != name.end())
          diagnostic::error("invalid replacement field")
            .note("invalid NAME")
            .hint("field: `{}`, NAME: `{}`", std::string{replacement_field},
                  name)
            .throw_();
        name = fmt::to_string(fmt::join(items, "."));
      }
      // Handle CONVERSION field, default to `implicit`,
      // which will be turned to `infer` or `string`, based on the `--raw` flag
      capture_type conversion{capture_type::implicit};
      if (elems.size() > 2) {
        if (elems[2] == "infer")
          conversion = capture_type::infer;
        else if (elems[2] == "string")
          conversion = capture_type::string;
        if (elems[2] == "int")
          conversion = capture_type::integer;
        else if (elems[2] == "long")
          conversion = capture_type::integer;
        else if (elems[2] == "float")
          conversion = capture_type::floating;
        else
          diagnostic::error("invalid replacement field")
            .note("invalid CONVERSION")
            .hint("field: `{}`, CONVERSION: `{}`",
                  std::string{replacement_field}, elems[2])
            .throw_();
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
            duplicate_it != named_captures.end())
          named_captures.erase(duplicate_it);
        named_captures.emplace_back(name, conversion);
        auto replacement
          = fmt::format("(?<{}>{})", name, subpattern.resolved_pattern->str());
        result_pattern.append(replacement);
      } else {
        // No NAME given, use SYNTAX as the name
        if (auto duplicate_it = get_duplicate_capture_name(syntax);
            duplicate_it != named_captures.end())
          named_captures.erase(duplicate_it);
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
    if (resolved_pattern->empty())
      diagnostic::error("invalid regular expression")
        .hint("regex: `{}`", result_pattern)
        .throw_();
  }
}

void pattern_store::parse_line(std::string_view line) {
  if (line.empty())
    return;
  if (line.starts_with('#'))
    return;
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
public:
  grok_parser() = default;

  explicit grok_parser(parser_interface& p)
    : patterns_(get_builtin_pattern_store()) {
    auto parser
      = argument_parser{"grok", "https://docs.tenzir.com/operators/grok"};
    parser.add(input_pattern_.raw_pattern, "<input_pattern>");
    std::optional<std::string> pattern_definitions{};
    parser.add("--pattern-definitions", pattern_definitions, "<patterns>");
    parser.add("--indexed-captures", indexed_captures_);
    parser.add("--include-unnamed", include_unnamed_);
    parser.add("--raw", raw_);
    parser.parse(p);
    if (pattern_definitions)
      patterns_.unshared().add(*pattern_definitions);
    TENZIR_ASSERT_EXPENSIVE(std::ranges::all_of(
      patterns_->patterns | std::views::values, [](const auto& p) -> bool {
        return p.resolved_pattern.has_value();
      }));
    input_pattern_.resolve(*patterns_, false);
  }

  grok_parser(std::string pattern, bool indexed_captures, bool include_unnamed,
              bool raw)
    : patterns_{get_builtin_pattern_store()},
      indexed_captures_{indexed_captures},
      include_unnamed_{include_unnamed},
      raw_{raw} {
    input_pattern_.raw_pattern = std::move(pattern);
    input_pattern_.resolve(*patterns_, false);
  }

  auto name() const -> std::string override {
    return "grok";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    (void)input;
    diagnostic::error("`{}` cannot be used here", name())
      .emit(ctrl.diagnostics());
    return {};
  }

  auto parse_strings(const arrow::StringArray& input,
                     diagnostic_handler& dh) const -> std::vector<series> {
    auto too_complex = std::optional<std::string_view>{};
    auto builder = series_builder{type{record_type{}}};
    for (auto&& string : values(string_type{}, input)) {
      if (not string) {
        builder.null();
        continue;
      }
      boost::cmatch matches{};
      try {
        if (not boost::regex_match(string->begin(), string->end(), matches,
                                   *input_pattern_.resolved_pattern)) {
          diagnostic::warning("pattern could not be matched")
            .hint("input: `{}`", *string)
            .hint("pattern: `{}`", input_pattern_.resolved_pattern->str())
            .emit(dh);
          builder.null();
          continue;
        }
      } catch (const boost::regex_error& e) {
        if (e.code() != boost::regex_constants::error_complexity) {
          throw;
        }
        if (not too_complex) {
          too_complex = string;
        }
        builder.null();
        continue;
      }
      auto record = builder.record();
      auto infer_match = [&](std::string_view in) -> data {
        const auto* f = in.begin();
        const auto* const l = in.end();
        constexpr auto parser = parsers::simple_data;
        if (data d{}; parser(f, l, d) && f == l)
          return d;
        return data{std::string{in}};
      };
      auto convert_match
        = [&](const boost::csub_match& match, capture_type type) -> data {
        if (!match.matched)
          return caf::none;
        switch (type) {
          case capture_type::implicit:
          case capture_type::unnamed:
            if (not raw_)
              return infer_match(std::string_view{match.first, match.second});
            return data{std::string{match.first, match.second}};
          case capture_type::infer:
            return infer_match(std::string_view{match.first, match.second});
          case capture_type::string:
            return data{std::string{match.first, match.second}};
          case capture_type::integer:
            if (auto r = to<int64_t>(match.str()))
              return data{*r};
            // TODO: Should this be an error/warning?
            return caf::none;
          case capture_type::floating:
            if (auto r = to<double>(match.str()))
              return data{*r};
            return caf::none;
        }
        TENZIR_UNREACHABLE();
      };
      auto add_field
        = [&](std::string_view name, data_view2 d, capture_type type) {
            if (include_unnamed_ || type != capture_type::unnamed)
              record.field(name, std::move(d));
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
            add_field(name, convert_match(match, type), type);
          } else {
            const auto type = capture_type::implicit;
            add_field(std::to_string(i), convert_match(match, type), type);
          }
        }
      } else {
        for (auto&& [name, type] : input_pattern_.named_captures) {
          TENZIR_ASSERT(not name.empty());
          add_field(name, convert_match(matches[name], type), type);
        }
      }
    }
    if (too_complex) {
      diagnostic::warning("failed to apply grok pattern due to its complexity")
        .note("example input: {:?}", *too_complex)
        .hint("try to simplify or optimize your grok pattern")
        .emit(dh);
    }
    return builder.finish();
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     operator_control_plane& ctrl) const
    -> std::vector<series> override {
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
              f.field("raw", x.raw_));
  }

private:
  // FIXME: The CoW semantics aren't really being taken advantage of here,
  // because inspect() has to create a copy of this every time.
  caf::intrusive_cow_ptr<pattern_store> patterns_{
    caf::make_copy_on_write<pattern_store>()};
  pattern input_pattern_{};
  bool indexed_captures_{false}, include_unnamed_{false}, raw_{false};
};

class plugin final : public virtual parser_plugin<grok_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    return std::make_unique<grok_parser>(p);
  }
};

class plugin2 final : public virtual method_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.grok";
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto pattern = std::string{};
    auto indexed_captures = false;
    auto include_unnamed = false;
    auto raw = false;
    TRY(argument_parser2::method("grok")
          .add(input, "<input>")
          .add(pattern, "<pattern>")
          .add("indexed_captures", indexed_captures)
          .add("include_unnamed", include_unnamed)
          .add("raw", raw)
          .parse(inv, ctx));
    auto parser = grok_parser{
      std::move(pattern),
      indexed_captures,
      include_unnamed,
      raw,
    };
    return function_use::make(
      [input = std::move(input),
       parser = std::move(parser)](evaluator eval, session ctx) -> series {
        auto values = eval(input);
        if (values.type.kind().is<null_type>()) {
          return values;
        }
        auto strings = caf::get_if<arrow::StringArray>(&*values.array);
        if (not strings) {
          diagnostic::warning("expected string, got `{}`", values.type.kind())
            .primary(input)
            .emit(ctx);
          return series::null(null_type{}, eval.length());
        }
        auto output = parser.parse_strings(*strings, ctx);
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
TENZIR_REGISTER_PLUGIN(tenzir::plugins::grok::plugin2)
