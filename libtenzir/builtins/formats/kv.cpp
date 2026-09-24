//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/pusher.hpp>
#include <tenzir/box.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/nova/array.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/view3.hpp>

#include <arrow/api.h>
#include <re2/re2.h>

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace tenzir::plugins::kv {

namespace {

constexpr auto docs = "https://tenzir.com/docs/formats/kv";

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

class kv_parser final {
public:
  kv_parser() = default;

  explicit kv_parser(kv_args args) : args_{std::move(args)} {
  }

  auto parse_line(multi_series_builder& builder, diagnostic_handler& dh,
                  std::string_view line) const -> void {
    auto event = builder.record();
    struct previous_t {
      std::string_view key;
      std::string_view value;
    };
    auto previous = Option<previous_t>{};
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
          .note("make sure `field_split` is a regular expression")
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

  friend auto inspect(auto& f, kv_parser& x) -> bool {
    return f.apply(x.args_);
  }

  kv_args args_;
};

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
    TRY(check_non_empty("value_separator", value_sep, dh));
    TRY(check_non_empty("list_separator", list_sep, dh));
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

struct ReadKvArgs {
  multi_series_builder::options msb_options
    = {.settings = {.default_schema_name = "tenzir.kv"}};
  located<std::string> field_split = {"\\s", location::unknown};
  located<std::string> value_split = {"=", location::unknown};
  located<std::string> quotes
    = {detail::quoting_escaping_policy{}.quotes, location::unknown};
  location operator_location = location::unknown;
};

class ReadKv final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadKv(ReadKvArgs args)
    : args_{std::move(args)},
      quoting_{.quotes = args_.quotes.inner},
      field_split_{located<std::string_view>{args_.field_split.inner,
                                             args_.field_split.source}},
      value_split_{located<std::string_view>{args_.value_split.inner,
                                             args_.value_split.source}} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    dh_.emplace(std::in_place, ctx.dh(), [this](diagnostic d) {
      if (args_.operator_location and not d.has_location()) {
        d.annotations.emplace_back(true, std::string{},
                                   args_.operator_location);
      }
      d.notes.emplace(d.notes.begin(), diagnostic_note_kind::note,
                      fmt::format("line {}", line_counter_));
      return d;
    });
    msb_ = multi_series_builder{args_.msb_options, *dh_};
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_await pusher_.wait();
    co_return {};
  }

  auto process_task(Any, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(msb_);
    co_await pusher_.push(msb_->yield_ready_as_table_slice(), push);
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(msb_);
    auto const* begin = reinterpret_cast<const char*>(input->data());
    auto const* const end = begin + input->size();
    // Accumulate ready slices for the whole chunk and hand them to the
    // executor once at the end, matching the read_cef/read_leef/read_xsv
    // parser pattern.
    auto ready = series_builder::YieldReadyResult{};
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    auto now = multi_series_builder::clock::now();
    for (auto const* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      if (buffer_.empty()) {
        process_line({begin, current});
      } else {
        buffer_.append(begin, current);
        process_line(buffer_);
        buffer_.clear();
      }
      ready.merge(msb_->yield_ready_as_table_slice(now));
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
    ready.merge(msb_->yield_ready_as_table_slice(now));
    co_await pusher_.push(std::move(ready), push);
  }

  auto finalize(Push<table_slice>& push, OpCtx&)
    -> Task<FinalizeBehavior> override {
    TENZIR_ASSERT(msb_);
    if (not buffer_.empty()) {
      process_line(buffer_);
      buffer_.clear();
    }
    for (auto& slice : msb_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    TENZIR_ASSERT(msb_);
    for (auto& slice : msb_->finalize_as_table_slice()) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
    serde("line_counter", line_counter_);
  }

private:
  auto process_line(std::string_view line) -> void {
    TENZIR_ASSERT(msb_);
    TENZIR_ASSERT(dh_);
    ++line_counter_;
    auto event = msb_->record();
    struct previous_t {
      std::string_view key;
      std::string_view value;
    };
    auto previous = Option<previous_t>{};
    const auto commit = [this, &event, &previous]() {
      if (not previous) {
        return;
      }
      auto key = quoting_.unquote_unescape(previous->key);
      if (previous->value.empty()) {
        event.unflattened_field(key).null();
        return;
      }
      auto value = quoting_.unquote_unescape(previous->value);
      event.unflattened_field(key).data_unparsed(std::move(value));
    };
    while (not line.empty()) {
      const auto [head, tail, field_sep] = field_split_.split(line, quoting_);
      const auto [key_view, value_view, value_sep]
        = value_split_.split(head, quoting_);
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
          .note("make sure `field_split` is a regular expression")
          .emit(**dh_);
        return;
      }
      line = tail;
    }
    commit();
  }

  ReadKvArgs args_;
  detail::quoting_escaping_policy quoting_ = {};
  splitter field_split_;
  splitter value_split_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  size_t line_counter_ = 0;
  Option<Box<transforming_diagnostic_handler>> dh_;
  Option<multi_series_builder> msb_;
  SeriesPusher pusher_;
};

struct WriteKvArgs {
  located<std::string> field_separator = {" ", location::unknown};
  located<std::string> value_separator = {"=", location::unknown};
  located<std::string> list_separator = {",", location::unknown};
  located<std::string> flatten_separator = {".", location::unknown};
  located<std::string> null_value = {"", location::unknown};
};

class WriteKv final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteKv(WriteKvArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx&)
    -> Task<void> override {
    auto resolved_slice
      = flatten(resolve_enumerations(input), args_.flatten_separator.inner)
          .slice;
    auto out = std::vector<char>{};
    auto out_iter = std::back_inserter(out);
    for (auto&& row : values3(resolved_slice)) {
      out_iter = print(out_iter, row);
      *out_iter++ = '\n';
    }
    co_await push(chunk::make(std::move(out)));
  }

private:
  template <class It>
  auto print(It out, view3<record> record) const -> It {
    auto it = record.begin();
    if (it == record.end()) {
      return out;
    }
    {
      const auto& [key, value] = *it;
      out = print(out, key);
      out = fmt::format_to(out, "{}", args_.value_separator.inner);
      out = print(out, value);
      ++it;
    }
    for (; it != record.end(); ++it) {
      const auto& [key, value] = *it;
      out = fmt::format_to(out, "{}", args_.field_separator.inner);
      out = print(out, key);
      out = fmt::format_to(out, "{}", args_.value_separator.inner);
      out = print(out, value);
    }
    return out;
  }

  template <class It>
  auto print(It out, view3<list> list) const -> It {
    auto it = list.begin();
    if (it == list.end()) {
      return out;
    }
    out = print(out, *it);
    ++it;
    for (; it != list.end(); ++it) {
      out = fmt::format_to(out, "{}", args_.list_separator.inner);
      out = print(out, *it);
    }
    return out;
  }

  template <class It>
  auto print(It out, data_view3 value) const -> It {
    return match(
      value,
      [&](const caf::none_t&) -> It {
        return fmt::format_to(out, "{}", args_.null_value.inner);
      },
      [&](const auto& scalar) -> It {
        auto formatted = fmt::format("{}", scalar);
        auto needs_quoting
          = formatted.find(args_.field_separator.inner) != formatted.npos
            or formatted.find(args_.value_separator.inner) != formatted.npos
            or formatted.find(args_.list_separator.inner) != formatted.npos
            or (not args_.null_value.inner.empty()
                and formatted.find(args_.null_value.inner) != formatted.npos);
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
      [&](const view3<list>& list) -> It {
        return print(out, list);
      },
      [&](const view3<record>&) -> It {
        TENZIR_UNREACHABLE();
        return out;
      });
  }

  WriteKvArgs args_;
};

/// Prints Nova rows as key-value pairs.
///
/// Nova records have per-row shapes, so the printer flattens every row while
/// walking it rather than flattening a schema up front. It follows the
/// semantics of `flatten`: nested records become fields with joined names,
/// records in lists become one list per leaf field, and nested lists become a
/// single list.
class NovaKvPrinter {
public:
  explicit NovaKvPrinter(WriteKvArgs const& args) : args_{args} {
  }

  auto print(nova::RowView<nova::Record> const& row) -> void {
    fields_.clear();
    names_.clear();
    paths_.clear();
    name_.clear();
    collect_fields(row);
    auto first = true;
    if (not has_duplicate_names()) {
      for (auto const& field : fields_) {
        print_field(first, name_of(field), field);
      }
    } else {
      auto names = unique_names();
      for (auto i = size_t{0}; i < fields_.size(); ++i) {
        print_field(first, names[i], fields_[i]);
      }
    }
    out_.push_back('\n');
  }

  auto take() && -> std::string {
    return std::move(out_);
  }

private:
  using Path = std::vector<std::string_view>;

  /// A flattened field of the current row.
  struct Field {
    /// The range of the field name in `names_`.
    size_t name_begin;
    size_t name_end;
    nova::RowView<nova::Data> value;
    /// The leaf path within the records of a list in `paths_`, if any.
    Option<size_t> path;
  };

  auto name_of(Field const& field) const -> std::string_view {
    return std::string_view{names_}.substr(field.name_begin,
                                           field.name_end - field.name_begin);
  }

  auto path_of(Field const& field) const -> std::span<std::string_view const> {
    if (not field.path) {
      return {};
    }
    return paths_[*field.path];
  }

  auto add_field(nova::RowView<nova::Data> value, Option<size_t> path) -> void {
    auto const begin = names_.size();
    names_ += name_;
    fields_.push_back(Field{begin, names_.size(), std::move(value), path});
  }

  /// Collects the fields of `record`, prefixing their names with `name_`.
  auto collect_fields(nova::RowView<nova::Record> const& record) -> void {
    auto const prefix = name_.size();
    for (auto field : record) {
      if (prefix != 0) {
        name_ += args_.flatten_separator.inner;
      }
      name_ += field.first;
      match(
        field.second,
        [&](nova::RowView<nova::Record> const& nested) {
          collect_fields(nested);
        },
        [&](nova::RowView<nova::List> const& list) {
          collect_list_field(list);
        },
        [&](auto const&) {
          add_field(field.second, None{});
        });
      name_.resize(prefix);
    }
  }

  /// Collects a list field named `name_`. A list that contains records turns
  /// into one field per leaf path of those records.
  auto collect_list_field(nova::RowView<nova::List> const& list) -> void {
    auto path = Path{};
    auto paths = std::vector<Path>{};
    if (not collect_paths(list, path, paths)) {
      add_field(list, None{});
      return;
    }
    auto const prefix = name_.size();
    for (auto& leaf : paths) {
      for (auto segment : leaf) {
        name_ += args_.flatten_separator.inner;
        name_ += segment;
      }
      add_field(list, paths_.size());
      paths_.push_back(std::move(leaf));
      name_.resize(prefix);
    }
  }

  /// Returns whether two fields of the current row share a flattened name,
  /// e.g., for `{"a.b": 1, a: {b: 2}}`.
  auto has_duplicate_names() -> bool {
    seen_.clear();
    for (auto const& field : fields_) {
      if (not seen_.insert(name_of(field)).second) {
        return true;
      }
    }
    return false;
  }

  /// Renames duplicate field names like `flatten`: the first occurrence keeps
  /// its name, and later ones get the suffix `_1`, `_2`, and so on, skipping
  /// suffixed names that already exist in the row.
  auto unique_names() const -> std::vector<std::string> {
    auto result = std::vector<std::string>{};
    result.reserve(fields_.size());
    auto existing = std::unordered_set<std::string_view>{};
    for (auto const& field : fields_) {
      result.emplace_back(name_of(field));
      existing.insert(name_of(field));
    }
    auto renamed = std::vector<bool>(fields_.size(), false);
    for (auto i = size_t{0}; i < fields_.size(); ++i) {
      if (renamed[i]) {
        continue;
      }
      auto const name = name_of(fields_[i]);
      auto occurrences = size_t{0};
      for (auto j = i + 1; j < fields_.size(); ++j) {
        if (name_of(fields_[j]) != name) {
          continue;
        }
        auto candidate = std::string{};
        do {
          ++occurrences;
          candidate = fmt::format("{}_{}", name, occurrences);
        } while (existing.contains(candidate));
        result[j] = std::move(candidate);
        renamed[j] = true;
      }
    }
    return result;
  }

  /// Collects the leaf paths of all records in `value` in first-seen order and
  /// returns whether `value` contains a record.
  static auto collect_paths(nova::RowView<nova::Data> const& value, Path& path,
                            std::vector<Path>& paths) -> bool {
    return match(
      value,
      [&](nova::RowView<nova::List> const& list) {
        auto found = false;
        for (auto element : list) {
          found = collect_paths(element, path, paths) or found;
        }
        return found;
      },
      [&](nova::RowView<nova::Record> const& record) {
        for (auto field : record) {
          path.push_back(field.first);
          if (not collect_paths(field.second, path, paths)
              and std::ranges::find(paths, path) == paths.end()) {
            paths.push_back(path);
          }
          path.pop_back();
        }
        return true;
      },
      [](auto const&) {
        return false;
      });
  }

  static auto is_list(nova::RowView<nova::Data> const& value) -> bool {
    return match(
      value,
      [](nova::RowView<nova::List> const&) {
        return true;
      },
      [](auto const&) {
        return false;
      });
  }

  static auto is_null(nova::RowView<nova::Data> const& value) -> bool {
    return match(
      value,
      [](nova::RowView<nova::Null>) {
        return true;
      },
      [](auto const&) {
        return false;
      });
  }

  /// Prints the values at `path` within `value` as list elements. Nested
  /// lists splice their elements into the enclosing list, and a null in place
  /// of a nested list contributes nothing. Missing values print as null.
  auto print_elements(nova::RowView<nova::Data> const& value,
                      std::span<std::string_view const> path, bool& first)
    -> void {
    match(
      value,
      [&](nova::RowView<nova::List> const& list) {
        auto nested = false;
        for (auto element : list) {
          if (is_list(element)) {
            nested = true;
            break;
          }
        }
        for (auto element : list) {
          if (not nested or not is_null(element)) {
            print_elements(element, path, first);
          }
        }
      },
      [&](nova::RowView<nova::Record> const& record) {
        if (not path.empty()) {
          for (auto field : record) {
            if (field.first == path.front()) {
              print_elements(field.second, path.subspan(1), first);
              return;
            }
          }
        }
        print_element(first, [&] {
          out_ += args_.null_value.inner;
        });
      },
      [&](nova::RowView<nova::Null>) {
        print_element(first, [&] {
          out_ += args_.null_value.inner;
        });
      },
      [&](auto const& scalar) {
        print_element(first, [&] {
          if (path.empty()) {
            print_scalar(*scalar);
          } else {
            out_ += args_.null_value.inner;
          }
        });
      });
  }

  auto print_element(bool& first, auto print_value) -> void {
    if (not std::exchange(first, false)) {
      out_ += args_.list_separator.inner;
    }
    print_value();
  }

  auto print_field(bool& first, std::string_view name, Field const& field)
    -> void {
    if (not std::exchange(first, false)) {
      out_ += args_.field_separator.inner;
    }
    print_scalar(name);
    out_ += args_.value_separator.inner;
    auto first_element = true;
    print_elements(field.value, path_of(field), first_element);
  }

  auto print_scalar(auto const& value) -> void {
    scratch_.clear();
    fmt::format_to(std::back_inserter(scratch_), "{}", value);
    auto const contains = [&](std::string const& needle) {
      return scratch_.find(needle) != std::string::npos;
    };
    auto const needs_quoting = contains(args_.field_separator.inner)
                               or contains(args_.value_separator.inner)
                               or contains(args_.list_separator.inner)
                               or (not args_.null_value.inner.empty()
                                   and contains(args_.null_value.inner));
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
    };
    constexpr static auto p = printers::escape(escaper);
    if (needs_quoting) {
      out_.push_back('"');
    }
    auto out = std::back_inserter(out_);
    TENZIR_ASSERT(p.print(out, scratch_));
    if (needs_quoting) {
      out_.push_back('"');
    }
  }

  WriteKvArgs const& args_;
  std::string out_;
  /// The flattened name of the field being collected.
  std::string name_;
  /// The fields of the current row, their names, and their leaf paths.
  std::vector<Field> fields_;
  std::string names_;
  std::vector<Path> paths_;
  std::unordered_set<std::string_view> seen_;
  std::string scratch_;
};

class WriteKvEvents final : public Operator<nova::Events, chunk_ptr> {
public:
  explicit WriteKvEvents(WriteKvArgs args) : args_{std::move(args)} {
  }

  auto process(nova::Events input, Push<chunk_ptr>& push, OpCtx&)
    -> Task<void> override {
    auto printer = NovaKvPrinter{args_};
    auto printed = false;
    for (auto row : nova::storage::true_bits(input.mask)) {
      printer.print(input.data.get(row));
      printed = true;
    }
    if (not printed) {
      co_return;
    }
    co_await push(chunk::make(std::move(printer).take()));
  }

private:
  WriteKvArgs args_;
};

auto validate_split_expression(const located<std::string>& split,
                               diagnostic_handler& dh) -> failure_or<void> {
  auto const test = [&](char c) -> failure_or<void> {
    if (split.inner.size() == 1 and split.inner.front() == c) {
      diagnostic::error("regular expression `{}` is not a valid splitter", c)
        .primary(split)
        .hint("use `\\{}` if you want to split on the literal character `{}`",
              c, c)
        .emit(dh);
      return failure::promise();
    }
    return {};
  };
  TRY(test('|'));
  TRY(test('.'));
  return {};
}

auto validate_splitter(const located<std::string>& split,
                       diagnostic_handler& dh) -> failure_or<void> {
  TRY(validate_split_expression(split, dh));
  try {
    auto validated
      = splitter{located<std::string_view>{split.inner, split.source}};
    TENZIR_UNUSED(validated);
  } catch (diagnostic d) {
    dh.emit(std::move(d));
    return failure::promise();
  }
  return {};
}

class read_kv : public virtual operator_factory_plugin,
                public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_kv";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadKvArgs, ReadKv>{};
    auto defaults = ReadKvArgs{};
    auto field_split
      = d.named_optional("field_split", &ReadKvArgs::field_split);
    auto value_split
      = d.named_optional("value_split", &ReadKvArgs::value_split);
    d.named_optional("quotes", &ReadKvArgs::quotes);
    auto msb = add_msb_to_describer(d, &ReadKvArgs::msb_options);
    d.operator_location(&ReadKvArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      // `DescribeCtx::get()` returns `nullopt` for omitted named arguments,
      // even when `ReadKvArgs` carries member defaults.
      auto fs = ctx.get(field_split);
      auto vs = ctx.get(value_split);
      (void)validate_splitter(fs ? *fs : defaults.field_split, ctx);
      (void)validate_splitter(vs ? *vs : defaults.value_split, ctx);
      msb(ctx);
      return {};
    });
    return d.without_optimize();
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"kv"}};
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate("read_kv", read_detection::specificity::keyed,
                                detect_kv),
    };
  }

private:
  static auto detect_kv(read_detection_input input) -> read_detection_result {
    namespace rd = read_detection;
    if (not detail::is_valid_utf8(input.bytes)) {
      if (not input.eof and detail::is_valid_utf8_prefix(input.bytes)) {
        return rd::need_more();
      }
      return rd::reject();
    }
    auto sample = rd::sample_lines(input, 2);
    std::erase_if(sample.complete, [](std::string_view& line) {
      line = detail::trim_front(line);
      return line.empty();
    });
    if (sample.complete.empty()) {
      return input.eof ? rd::reject() : rd::need_more();
    }
    // Dry-run the reader's splitters with their default configuration. The
    // parser itself tolerates lines without a value separator by folding
    // them into the previous value, which makes it accept arbitrary prose;
    // for detection, require every line to yield at least one real
    // key-value assignment.
    static auto const field_split
      = splitter{located<std::string_view>{"\\s", location::unknown}};
    static auto const value_split
      = splitter{located<std::string_view>{"=", location::unknown}};
    auto const quoting = detail::quoting_escaping_policy{};
    auto has_assignment = [&](std::string_view line) {
      while (not line.empty()) {
        auto const [head, tail, field_sep] = field_split.split(line, quoting);
        auto const [key, value, value_sep] = value_split.split(head, quoting);
        if (value_sep.found() and not key.empty() and not value.empty()) {
          return true;
        }
        if (line == tail) {
          break;
        }
        line = tail;
      }
      return false;
    };
    if (std::ranges::all_of(sample.complete, has_assignment)) {
      return rd::match();
    }
    return rd::reject();
  }
};

class write_kv : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_kv";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteKvArgs, WriteKv, WriteKvEvents>{};
    auto field_sep
      = d.named_optional("field_separator", &WriteKvArgs::field_separator);
    auto value_sep
      = d.named_optional("value_separator", &WriteKvArgs::value_separator);
    auto list_sep
      = d.named_optional("list_separator", &WriteKvArgs::list_separator);
    auto flatten_sep
      = d.named_optional("flatten_separator", &WriteKvArgs::flatten_separator);
    auto null_value = d.named_optional("null_value", &WriteKvArgs::null_value);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto fs = ctx.get(field_sep).value_or(WriteKvArgs{}.field_separator);
      auto vs = ctx.get(value_sep).value_or(WriteKvArgs{}.value_separator);
      auto ls = ctx.get(list_sep).value_or(WriteKvArgs{}.list_separator);
      auto fl = ctx.get(flatten_sep).value_or(WriteKvArgs{}.flatten_separator);
      auto nv = ctx.get(null_value).value_or(WriteKvArgs{}.null_value);
      auto& dh = ctx;
      (void)check_no_substrings(dh, {{"flatten_separator", fl},
                                     {"field_separator", fs},
                                     {"value_separator", vs},
                                     {"list_separator", ls},
                                     {"null_value", nv}});
      (void)check_non_empty("field_separator", fs, dh);
      (void)check_non_empty("value_separator", vs, dh);
      (void)check_non_empty("list_separator", ls, dh);
      return {};
    });
    return d.without_optimize();
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

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto input = ast::expression{};
    auto parser = argument_parser2::function(name());
    auto field_split = Option<located<std::string>>{
      std::in_place,
      "\\s",
      location::unknown,
    };
    auto value_split = Option<located<std::string>>{
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
    TRY(validate_split_expression(*field_split, ctx));
    TRY(validate_split_expression(*value_split, ctx));
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

  auto make_function(function_invocation inv, session ctx) const
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

TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::read_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::write_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::parse_kv)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::kv::print_kv)
