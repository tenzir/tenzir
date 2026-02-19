//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/defaults.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/split_at_regex.hpp>
#include <tenzir/split_at_string.hpp>
#include <tenzir/split_nulls.hpp>
#include <tenzir/to_lines.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/util/utf8.h>

#include <optional>

namespace tenzir::plugins::lines {

namespace {

struct parser_args {
  parser_args() = default;

  explicit parser_args(location self) : self{self} {
  }

  location self;
  bool binary{false};
  std::optional<location> skip_empty;
  std::optional<location> null;
  std::optional<located<std::string>> split_at_regex;
  std::optional<located<std::string>> split_at_string;
  bool include_separator{false};
  std::string field_name{"line"};

  template <class Inspector>
  friend auto inspect(Inspector& f, parser_args& x) -> bool {
    return f.object(x)
      .pretty_name("parser_args")
      .fields(f.field("self", x.self), f.field("skip_empty", x.skip_empty),
              f.field("null", x.null),
              f.field("split_at_regex", x.split_at_regex),
              f.field("split_at_string", x.split_at_string),
              f.field("include_separator", x.include_separator),
              f.field("field_name", x.field_name));
  }
};

class lines_parser final : public plugin_parser {
public:
  lines_parser() = default;

  explicit lines_parser(parser_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "lines";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto make = [](operator_control_plane& ctrl, generator<chunk_ptr> input,
                   location self, bool binary, bool skip_empty, bool nulls,
                   std::optional<located<std::string>> split_at_regex,
                   std::optional<located<std::string>> split_at_string,
                   bool include_separator,
                   const std::string& field_name) -> generator<table_slice> {
      TENZIR_UNUSED(ctrl);
      auto builder = series_builder{};
      auto last_finish = std::chrono::steady_clock::now();
      auto cutter
        = [&]()
            ->std::function<auto(generator<chunk_ptr>)
                              -> generator<std::optional<std::string_view>>> {
              if (nulls) {
                return split_nulls;
              }
              if (split_at_regex) {
                return tenzir::split_at_regex(split_at_regex->inner,
                                              include_separator);
              }
              if (split_at_string) {
                return tenzir::split_at_string(split_at_string->inner,
                                               include_separator);
              }
              return to_lines;
            }();
      for (auto line : cutter(std::move(input))) {
        if (not line) {
          co_yield {};
          continue;
        }
        if (line->empty() and skip_empty) {
          continue;
        }
        if (binary) {
          builder.record().field(field_name, as_bytes(*line));
        } else {
          if (not arrow::util::ValidateUTF8(*line)) {
            diagnostic::warning("got invalid UTF-8")
              .primary(self)
              .hint("use `binary=true` if you are reading binary data")
              .emit(ctrl.diagnostics());
            continue;
          }
          builder.record().field(field_name, *line);
        }
        const auto now = std::chrono::steady_clock::now();
        if (builder.length() >= detail::narrow_cast<int64_t>(
              defaults::import::table_slice_size)
            or last_finish + defaults::import::batch_timeout < now) {
          last_finish = now;
          co_yield builder.finish_assert_one_slice();
        }
      }
      if (builder.length() > 0) {
        co_yield builder.finish_assert_one_slice("tenzir.line");
      }
    };
    return make(ctrl, std::move(input), args_.self, args_.binary,
                args_.skip_empty.has_value(), args_.null.has_value(),
                args_.split_at_regex, args_.split_at_string,
                args_.include_separator, args_.field_name);
  }

  friend auto inspect(auto& f, lines_parser& x) -> bool {
    return f.object(x)
      .pretty_name("lines_parser")
      .fields(f.field("args", x.args_));
  }

private:
  parser_args args_;
};

struct lines_printer_impl {
  template <typename It>
  auto print_values(It& out, const view3<record>& x) const -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (is<caf::none_t>(v)) {
        continue;
      }
      if (! first) {
        ++out = ' ';
      } else {
        first = false;
      }
      match(v, visitor{out});
    }
    return true;
  }

  template <class Iterator>
  struct visitor {
    visitor(Iterator& out) : out{out} {
    }

    auto operator()(caf::none_t) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const auto& x) -> bool {
      sequence_empty = false;
      make_printer<std::remove_cvref_t<decltype(x)>> p;
      return p.print(out, x);
    }

    auto operator()(const view3<pattern>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const view3<map>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(const view3<record>&) -> bool {
      TENZIR_UNREACHABLE();
    }

    auto operator()(view3<std::string> x) -> bool {
      sequence_empty = false;
      out = std::copy(x.begin(), x.end(), out);
      return true;
    }

    auto operator()(view3<blob> x) -> bool {
      return (*this)(detail::base64::encode(x));
    }

    auto operator()(const view3<list>& x) -> bool {
      sequence_empty = true;
      for (const auto& v : x) {
        if (is<caf::none_t>(v)) {
          continue;
        }
        if (! sequence_empty) {
          ++out = ',';
        }
        if (! match(v, *this)) {
          return false;
        }
      }
      return true;
    }

    Iterator& out;
    bool sequence_empty{true};
  };
};

class lines_printer final : public plugin_printer {
public:
  auto name() const -> std::string override {
    return "lines";
  }

  auto instantiate(type, operator_control_plane&) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return printer_instance::make(
      [](table_slice slice) -> generator<chunk_ptr> {
        if (slice.rows() == 0) {
          co_yield {};
          co_return;
        }
        auto printer = lines_printer_impl{};
        auto buffer = std::vector<char>{};
        auto out_iter = std::back_inserter(buffer);
        auto resolved_slice = flatten(resolve_enumerations(slice)).slice;
        auto array = check(to_record_batch(resolved_slice)->ToStructArray());
        for (const auto& row : values3(*array)) {
          TENZIR_ASSERT(row);
          const auto ok = printer.print_values(out_iter, *row);
          TENZIR_ASSERT(ok);
          out_iter = fmt::format_to(out_iter, "\n");
        }
        auto chunk = chunk::make(std::move(buffer),
                                 chunk_metadata{.content_type = "text/plain"});
        co_yield std::move(chunk);
      });
  }

  auto allows_joining() const -> bool override {
    return true;
  }

  auto prints_utf8() const -> bool override {
    return true;
  }

  friend auto inspect(auto& f, lines_printer& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual parser_plugin<lines_parser>,
                     public virtual printer_plugin<lines_printer> {
public:
  auto name() const -> std::string override {
    return "lines";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto args = parser_args{};
    parser.add("-s,--skip-empty", args.skip_empty);
    parser.add("--null", args.null);
    parser.parse(p);
    return std::make_unique<lines_parser>(std::move(args));
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    if (not p.at_end()) {
      diagnostic::error("'lines' printer doesn't accept any arguments")
        .primary(p.current_span())
        .docs(fmt::format("https://docs.tenzir.com/formats/{}", name()))
        .throw_();
    }
    return std::make_unique<lines_printer>();
  }
};

/// Arguments for the read_lines operator (new async API).
struct ReadLinesArgs {
  bool binary = false;
  bool skip_empty = false;

  template <class Inspector>
  friend auto inspect(Inspector& f, ReadLinesArgs& x) -> bool {
    return f.object(x)
      .pretty_name("ReadLinesArgs")
      .fields(f.field("binary", x.binary), f.field("skip_empty", x.skip_empty));
  }
};

/// The read_lines operator using the new async execution API.
/// Transforms chunk_ptr input into table_slice output by splitting on newlines.
class ReadLines final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadLines(ReadLinesArgs args) : args_{args} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not input or input->size() == 0) {
      co_return;
    }
    const auto* begin = reinterpret_cast<const char*>(input->data());
    const auto* const end = begin + input->size();
    // Handle case where previous chunk ended on carriage return.
    if (ended_on_carriage_return_ and *begin == '\n') {
      ++begin;
    }
    ended_on_carriage_return_ = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' and *current != '\r') {
        continue;
      }
      // Found a line ending.
      auto line = std::string_view{};
      if (buffer_.empty()) {
        line = std::string_view{begin, current};
      } else {
        buffer_.append(begin, current);
        line = buffer_;
      }
      emit_line(line, ctx);
      if (not buffer_.empty()) {
        buffer_.clear();
      }
      // Handle \r\n sequence.
      if (*current == '\r') {
        const auto* next = current + 1;
        if (next == end) {
          ended_on_carriage_return_ = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    // Buffer remaining data for the next chunk.
    buffer_.append(begin, end);
    // Flush if we've accumulated enough data.
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice("tenzir.line"));
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx) -> Task<void> override {
    // Emit any remaining buffered data as the final line.
    if (not buffer_.empty()) {
      emit_line(buffer_, ctx);
      buffer_.clear();
    }
    // Flush any remaining events.
    if (builder_.length() > 0) {
      co_await push(builder_.finish_assert_one_slice("tenzir.line"));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    serde("buffer", buffer_);
    serde("ended_on_carriage_return", ended_on_carriage_return_);
  }

private:
  auto emit_line(std::string_view line, OpCtx& ctx) -> void {
    if (line.empty() and args_.skip_empty) {
      return;
    }
    if (args_.binary) {
      builder_.record().field("line", as_bytes(line));
    } else {
      if (not arrow::util::ValidateUTF8(line)) {
        diagnostic::warning("got invalid UTF-8")
          .hint("use `binary=true` if you are reading binary data")
          .emit(ctx);
        return;
      }
      builder_.record().field("line", line);
    }
  }

  ReadLinesArgs args_;
  std::string buffer_;
  bool ended_on_carriage_return_ = false;
  series_builder builder_;
};

} // namespace

class read_lines_plugin final
  : public virtual operator_plugin2<parser_adapter<lines_parser>>,
    public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "read_lines";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{inv.self.get_location()};
    argument_parser2::operator_(name())
      .named("binary", args.binary)
      .named("skip_empty", args.skip_empty)
      .named("split_at_null", args.null)
      .named("split_at_regex", args.split_at_regex)
      .parse(inv, ctx)
      .ignore();
    if (args.split_at_regex && args.null) {
      diagnostic::error(
        "cannot use `split_at_regex` and `split_at_null` at the same time")
        .primary(*args.split_at_regex)
        .primary(*args.null)
        .emit(ctx);
      return failure::promise();
    }
    if (args.null) {
      diagnostic::warning("the `split_at_null` option is deprecated, use "
                          "`read_delimited` instead")
        .primary(*args.null)
        .emit(ctx);
    }
    if (args.split_at_regex) {
      diagnostic::warning("the `split_at_regex` option is deprecated, use "
                          "`read_delimited_regex` instead")
        .primary(*args.split_at_regex)
        .emit(ctx);
    }
    return std::make_unique<parser_adapter<lines_parser>>(
      lines_parser{std::move(args)});
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadLinesArgs, ReadLines>{};
    d.named("binary", &ReadLinesArgs::binary);
    d.named("skip_empty", &ReadLinesArgs::skip_empty);
    return d.without_optimize();
  }
};

struct WriteLinesArgs {
  // write_lines takes no arguments.
};

class WriteLines final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteLines(WriteLinesArgs args) : args_{args} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (input.rows() == 0) {
      co_return;
    }
    auto printer = lines_printer_impl{};
    auto buffer = std::vector<char>{};
    auto const expected_size
      = total_size_written / total_rows_written * input.rows();
    buffer.reserve(expected_size);
    auto out_iter = std::back_inserter(buffer);
    auto resolved_slice = flatten(resolve_enumerations(input)).slice;
    auto array = check(to_record_batch(resolved_slice)->ToStructArray());
    for (const auto& row : values3(*array)) {
      TENZIR_ASSERT(row);
      const auto ok = printer.print_values(out_iter, *row);
      TENZIR_ASSERT(ok);
      out_iter = fmt::format_to(out_iter, "\n");
    }
    auto chunk = chunk::make(std::move(buffer),
                             chunk_metadata{.content_type = "text/plain"});
    total_size_written += buffer.size();
    total_rows_written += input.rows();
    co_await push(std::move(chunk));
  }

private:
  /// The total size the operator has written
  std::size_t total_size_written = 0;
  /// The total number of events the operator has written
  std::size_t total_rows_written = 1;
  [[maybe_unused, no_unique_address]] WriteLinesArgs args_;
};

class write_lines final
  : public virtual operator_plugin2<writer_adapter<lines_printer>>,
    public virtual OperatorPlugin {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_("write_lines").parse(inv, ctx));
    return std::make_unique<writer_adapter<lines_printer>>(lines_printer{});
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteLinesArgs, WriteLines>{};
    return d.without_optimize();
  }
};

class read_delimited_regex final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "read_delimited_regex";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{inv.self.get_location()};
    args.field_name = "data";
    auto regex = ast::expression{};
    auto binary_flag = std::optional<located<bool>>{};
    TRY(argument_parser2::operator_(name())
          .positional("regex", regex, "string")
          .named("binary", binary_flag)
          .named("include_separator", args.include_separator)
          .parse(inv, ctx));
    TRY(auto result, const_eval(regex, ctx));
    auto failed = false;
    match(
      result,
      [&](std::string x) {
        args.split_at_regex = located{std::move(x), regex.get_location()};
      },
      [&](const blob& x) {
        args.split_at_regex = located{
          std::string{reinterpret_cast<const char*>(x.data()), x.size()},
          regex.get_location()};
        binary_flag = binary_flag.value_or(located{true, location::unknown});
      },
      [&](const auto& x) {
        failed = true;
        diagnostic::error("expected `string` or `blob`, but got `{}`",
                          type::infer(x).value_or(type{}).kind())
          .primary(regex)
          .emit(ctx);
      });
    if (failed) {
      return failure::promise();
    }
    args.binary = binary_flag ? binary_flag->inner : false;
    try {
      const auto expr = boost::regex{
        args.split_at_regex->inner.data(),
        args.split_at_regex->inner.size(),
        boost::regex_constants::optimize,
      };
    } catch (const std::exception& e) {
      diagnostic::error("Invalid regex: {}", e.what())
        .primary(*args.split_at_regex)
        .note("regex: {}", args.split_at_regex->inner)
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<parser_adapter<lines_parser>>(
      lines_parser{std::move(args)});
  }
};

class read_delimited final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "read_delimited";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = parser_args{inv.self.get_location()};
    args.field_name = "data";
    auto separator = ast::expression{};
    auto binary_flag = std::optional<located<bool>>{};
    TRY(argument_parser2::operator_(name())
          .positional("separator", separator, "string")
          .named("binary", binary_flag)
          .named("include_separator", args.include_separator)
          .parse(inv, ctx));
    TRY(auto result, const_eval(separator, ctx));
    auto failed = false;
    match(
      result,
      [&](std::string x) {
        args.split_at_string = located{
          std::move(x),
          separator.get_location(),
        };
      },
      [&](const blob& x) {
        args.split_at_string = located{
          std::string{reinterpret_cast<const char*>(x.data()), x.size()},
          separator.get_location(),
        };
        binary_flag = binary_flag.value_or(located{true, location::unknown});
      },
      [&](const auto& x) {
        failed = true;
        diagnostic::error("expected `string` or `blob`, but got `{}`",
                          type::infer(x).value_or(type{}).kind())
          .primary(separator)
          .emit(ctx);
      });
    if (failed) {
      return failure::promise();
    }
    args.binary = binary_flag ? binary_flag->inner : false;
    return std::make_unique<parser_adapter<lines_parser>>(
      lines_parser{std::move(args)});
  }
};

class read_all final : public virtual operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "read_all";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto binary_flag = std::optional<located<bool>>{};
    TRY(argument_parser2::operator_(name())
          .named("binary", binary_flag)
          .parse(inv, ctx));
    auto args = parser_args{inv.self.get_location()};
    args.field_name = "data";
    args.split_at_regex = located{"(?!)", location::unknown};
    args.binary = binary_flag ? binary_flag->inner : false;
    return std::make_unique<parser_adapter<lines_parser>>(
      lines_parser{std::move(args)});
  }
};

} // namespace tenzir::plugins::lines

TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_lines_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::write_lines)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_delimited_regex)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_delimited)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::lines::read_all)
