//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/to_lines.hpp>

#include <arrow/api.h>
#include <re2/re2.h>

namespace tenzir::plugins::kv {

namespace {

constexpr auto docs = "https://docs.tenzir.com/formats/kv";

/// @brief Checks whether the index `idx` in `text` is escaped
/// TODO the precise-parsers PR contains this in string.hpp
inline auto is_escaped(size_t idx, std::string_view text) -> bool {
  if (idx >= text.size()) {
    return false;
  }
  // An odd number of preceding backslashes means it is escaped, for example:
  // `x\n` => true, `x\\n` => false, `x\\\n` => true, 'x\\\\n' => false.
  auto backslashes = size_t{0};
  while (idx > 0 and text[idx - 1] == '\\') {
    ++backslashes;
    --idx;
  }
  return backslashes % 2 == 1;
}

inline auto is_quoted(std::string_view text) -> bool {
  if (text.size() < 2) {
    return false;
  }
  return text.front() == text.back() and text.front() == '\"'
         and not is_escaped(text.size() - 1, text);
}

[[nodiscard]] auto trim_quotes(std::string_view txt) -> std::string_view {
  if (txt.size() < 2) {
    return txt;
  }
  if (txt.front() != txt.back()) {
    return txt;
  }
  if (txt.front() == '\"' and not is_escaped(txt.size() - 1, txt)) {
    txt.remove_prefix(1);
    txt.remove_suffix(1);
    return txt;
  }
  return txt;
}

class splitter {
public:
  splitter() = default;

  splitter(const splitter& other)
    : regex_{std::make_unique<re2::RE2>(other.regex_->pattern())} {
  }
  splitter(splitter&&) = default;
  splitter& operator=(splitter&&) = default;

  explicit splitter(located<std::string_view> pattern) {
    auto regex = std::make_unique<re2::RE2>(pattern.inner,
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

  auto split(std::string_view input) const
    -> std::pair<std::string_view, std::string_view> {
    TENZIR_ASSERT(regex_);
    TENZIR_ASSERT(regex_->NumberOfCapturingGroups() == 1);
    auto group = re2::StringPiece{};
    auto start_offset = 0;
    while (true) {
      if (not re2::RE2::PartialMatch(input.substr(start_offset), *regex_,
                                     &group)) {
        return {input, {}};
      }
      if (group.empty()) {
        return {input, {}};
      }
      auto head = std::string_view{input.data(), group.data()};
      auto tail = std::string_view{group.data() + group.size(),
                                   input.data() + input.size()};
      auto quote_count = 0;
      for (size_t i = 0; i < head.size(); ++i) {
        if (head[i] == '\"' and not is_escaped(i, head)) {
          ++quote_count;
        }
      }
      auto is_valid = quote_count == 0 or quote_count == 2
                      or (quote_count == 4 and head[0] == '"');
      if (is_valid) {
        return {head, tail};
      } else {
        start_offset = head.size() + group.size();
      }
    }
    return {input, {}};
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

class kv_parser;
auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl,
                kv_parser parser) -> generator<table_slice>;

class kv_parser final : public plugin_parser {
public:
  kv_parser() = default;

  explicit kv_parser(parser_interface& p) {
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
    parser.parse(p);
    field_split_ = splitter{*field_split};
    value_split_ = splitter{*value_split};
  }

  auto name() const -> std::string override {
    return "kv";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl, *this);
  }

  auto parse_line(series_builder& builder, operator_control_plane& ctrl,
                  std::string_view line) const -> void {
    auto event = builder.record();
    while (not line.empty()) {
      // TODO: We ignore split failures here. There might be better ways
      // to handle this.
      auto [head, tail] = field_split_.split(line);
      auto [key_view, value_view] = value_split_.split(head);
      auto key = is_quoted(key_view) ? detail::json_unescape(key_view)
                                     : std::string{key_view};
      auto value = is_quoted(value_view) ? detail::json_unescape(value_view)
                                         : std::string{value_view};
      if (auto d = data{}; parsers::simple_data(value, d)) {
        event.field(key, d);
      } else {
        event.field(key, value);
      }
      if (line == tail) {
        diagnostic::error("`kv` did not make progress")
          .note("check your field splitter")
          .emit(ctrl.diagnostics());
      }
      line = tail;
    }
  }

  auto parse_strings(std::shared_ptr<arrow::StringArray> input,
                     operator_control_plane& ctrl) const
    -> std::vector<series> override {
    auto builder = series_builder{type{record_type{}}};
    for (auto&& line : values(string_type{}, *input)) {
      if (not line) {
        builder.null();
        continue;
      }
      parse_line(builder, ctrl, *line);
    }
    return builder.finish();
  }

  friend auto inspect(auto& f, kv_parser& x) -> bool {
    return f.object(x)
      .pretty_name("kv_parser")
      .fields(f.field("field_split", x.field_split_),
              f.field("value_split", x.value_split_));
  }

protected:
  splitter field_split_;
  splitter value_split_;
};

auto parse_loop(generator<std::optional<std::string_view>> input,
                operator_control_plane& ctrl,
                kv_parser parser) -> generator<table_slice> {
  auto builder = series_builder{type{record_type{}}};
  for (auto&& line : input) {
    if (not line) {
      co_yield {};
      continue;
    }
    parser.parse_line(builder, ctrl, *line);
  }
  co_yield builder.finish_assert_one_slice("kv");
}

class plugin final : public virtual parser_plugin<kv_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    return std::make_unique<kv_parser>(p);
  }
};

} // namespace

} // namespace tenzir::plugins::kv

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::plugin)
