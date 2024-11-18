//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/concept/printable/std/chrono.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/multi_series_builder_argument_parser.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/to_lines.hpp>

#include <ranges>

namespace tenzir::plugins::syslog {

namespace {

/// A parser that parses an optional value whose nullopt is presented as a dash.
template <class Parser>
struct maybe_null_parser : parser_base<maybe_null_parser<Parser>> {
  using value_type = typename std::decay_t<Parser>::attribute;
  using attribute = std::conditional_t<concepts::container<value_type>,
                                       value_type, std::optional<value_type>>;

  explicit maybe_null_parser(Parser parser) : parser_{std::move(parser)} {
  }

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    // clang-format off
       auto p = ('-'_p >> &(' '_p)) ->*[] { return attribute{}; }
              | parser_ ->*[](value_type in) { return attribute{in}; };
    // clang-format on
    return p(f, l, x);
  }

  Parser parser_;
};

/// Wraps a parser and allows it to be null.
/// @relates maybe_null_parser
template <class Parser>
auto maybe_null(Parser&& parser) {
  return maybe_null_parser<Parser>{std::forward<Parser>(parser)};
}

/// A Syslog message header.
struct header {
  uint16_t facility;
  uint16_t severity;
  uint16_t version;
  std::optional<time> ts;
  std::string hostname;
  std::string app_name;
  std::string process_id;
  std::string msg_id;
};

/// Parser for Syslog message headers.
/// @relates header
struct header_parser : parser_base<header_parser> {
  using attribute = header;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using parsers::printable, parsers::rep;
    auto is_prival = [](uint16_t in) {
      return in <= 191;
    };
    auto to_facility_and_severity = [&](uint16_t in) {
      // Retrieve facillity and severity from prival.
      if constexpr (!std::is_same_v<Attribute, unused_type>) {
        x.facility = in / 8;
        x.severity = in % 8;
      }
    };
    auto prival = ignore(
      integral_parser<uint16_t, 3>{}.with(is_prival)->*to_facility_and_severity);
    auto pri = '<' >> prival >> '>';
    auto is_version = [](uint16_t in) {
      return in > 0;
    };
    auto version = integral_parser<uint16_t, 3>{}.with(is_version);
    auto hostname = maybe_null(rep(printable - ' ', 1, 255));
    auto app_name = maybe_null(rep(printable - ' ', 1, 48));
    auto process_id = maybe_null(rep(printable - ' ', 1, 128));
    auto msg_id = maybe_null(rep(printable - ' ', 1, 32));
    auto timestamp = maybe_null(parsers::time);
    auto p = pri >> version >> ' ' >> timestamp >> ' ' >> hostname >> ' '
             >> app_name >> ' ' >> process_id >> ' ' >> msg_id;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.version, x.ts, x.hostname, x.app_name, x.process_id,
               x.msg_id);
    }
  }
};

/// A parameter of a structured data element.
struct parameter {
  std::string key;
  data value;
};

/// Parser for one structured data element parameter.
/// @relates parameter
struct parameter_parser : parser_base<parameter_parser> {
  using attribute = parameter;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::printable, parsers::rep, parsers::ch;
    // space, =, ", and ] are not allowed in the key of the parameter.
    auto key = rep(printable - '=' - ' ' - ']' - '"', 1, 32);
    // \ is used to escape characters.
    auto esc = ignore(ch<'\\'>);
    // ], ", \ must to be escaped.
    auto escaped = esc >> (ch<']'> | ch<'\\'> | ch<'"'>);
    auto value = escaped | (printable - ']' - '"' - '\\');
    auto value_data = (*value)->*[](std::string val) {
      data d{};
      if (not parsers::simple_data(val, d)) {
        return data{std::move(val)};
      }
      return d;
    };
    auto p = ' ' >> key >> '=' >> '"' >> value_data >> '"';
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.key, x.value);
    }
  }
};

/// All parameters of a structured data element.
using parameters = record;

/// Parser for all structured data element parameters.
struct parameters_parser : parser_base<parameters_parser> {
  using attribute = parameters;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    auto param = parameter_parser{}->*[&](parameter in) {
      if constexpr (!std::is_same_v<Attribute, unused_type>) {
        x.emplace(std::move(in.key), data{std::move(in.value)});
      }
    };
    auto p = +param;
    return p(f, l, unused);
  }
};

/// A structured data element.
struct structured_data_element {
  std::string id;
  parameters params;
};

/// Parser for structured data elements.
/// @relates structured_data_element
struct structured_data_element_parser
  : parser_base<structured_data_element_parser> {
  using attribute = structured_data_element;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::printable, parsers::rep;
    auto is_sd_name_char = [](char in) {
      return in != '=' && in != ' ' && in != ']' && in != '"';
    };
    auto sd_name = printable - ' ';
    auto sd_name_char = sd_name.with(is_sd_name_char);
    auto sd_id = rep(sd_name_char, 1, 32);
    auto params = parameters_parser{};
    auto p = '[' >> sd_id >> params >> ']';
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.id, x.params);
    }
  }
};

/// Structured data of a Syslog message.
using structured_data = record;

/// Parser for structured data of a Syslog message.
/// @relates structured_data
struct structured_data_parser : parser_base<structured_data_parser> {
  using attribute = structured_data;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parsers;
    auto sd
      = structured_data_element_parser{}->*[&](structured_data_element in) {
          if constexpr (!std::is_same_v<Attribute, unused_type>) {
            x.emplace(std::move(in.id), tenzir::data{std::move(in.params)});
          }
        };
    auto p = maybe_null(+sd);
    return p(f, l, unused);
  }
};

/// Content of a Syslog message.
using message_content = std::string;

/// Parser for Syslog message content.
/// @relates message_content
struct message_content_parser : parser_base<message_content_parser> {
  using attribute = message_content;
  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    auto bom = "\xEF\xBB\xBF"_p;
    auto p = (bom >> +parsers::any) | +parsers::any | parsers::eoi;
    return p(f, l, x);
  }
};

/// A Syslog message.
struct message {
  header hdr;
  structured_data data;
  std::optional<message_content> msg;
};

/// Parser for Syslog messages.
/// @relates message
struct message_parser : parser_base<message_parser> {
  using attribute = message;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parsers;
    auto p = header_parser{} >> ' ' >> structured_data_parser{}
             >> -(' ' >> message_content_parser{});
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.hdr, x.data, x.msg);
    }
  }
};

/// A legacy (RFC 3164) Syslog message.
struct legacy_message {
  std::optional<uint16_t> facility;
  std::optional<uint16_t> severity;
  std::string timestamp;
  std::optional<std::string> host;
  std::optional<std::string> tag;
  std::optional<std::string> process_id;
  std::string content;
};

// Timestamp as specified by RFC3164:
// Mmm dd hh:mm:ss
struct legacy_message_timestamp_parser
  : parser_base<legacy_message_timestamp_parser> {
  using attribute = std::string;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    const auto word = +(parsers::printable - parsers::space);
    const auto ws = +parsers::space;
    const auto is_month = [](const std::string& mon) {
      return mon == "Jan" || mon == "Feb" || mon == "Mar" || mon == "Apr"
             || mon == "May" || mon == "Jun" || mon == "Jul" || mon == "Aug"
             || mon == "Sep" || mon == "Oct" || mon == "Nov" || mon == "Dec";
    };
    const auto is_day = [&](const std::string& day) -> bool {
      const auto p = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t day) {
        return day <= 31;
      });
      return p(day, unused);
    };
    const auto is_year = [&](const std::string& year) -> bool {
      const auto p = integral_parser<uint16_t, 4>{}.with([](uint16_t year) {
        // Reasonable-ish assumption for a year
        return year >= 1900 && year <= 2100;
      });
      return p(year, unused);
    };
    const auto is_time = [&](const std::string& time) -> bool {
      const auto hour_parser
        = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t hour) {
            return hour <= 23;
          });
      const auto minsec_parser
        = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t x) {
            return x <= 59;
          });
      const auto p
        = hour_parser >> ':' >> minsec_parser >> ':' >> minsec_parser;
      auto sv = std::string_view{time};
      const auto* f = sv.begin();
      const auto* const l = sv.end();
      return p(f, l, unused) && f == l;
    };
    auto p = word.with(is_month) >> ws >> word.with(is_day) >> ws
             >> ~(word.with(is_year) >> ws) >> word.with(is_time);
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      std::array<std::string, 6> elems{};
      if (not p(f, l, elems[0], elems[1], elems[2], elems[3], elems[4],
                elems[5])) {
        return false;
      }
      x = fmt::to_string(fmt::join(elems, ""));
      return true;
    }
  }
};

/// Parser for legacy Syslog messages.
/// @relates legacy_message
struct legacy_message_parser : parser_base<legacy_message_parser> {
  using attribute = legacy_message;

  template <typename Iterator, typename Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto word = +(parsers::printable - (parsers::space | ':'));
    const auto ws = +parsers::space;
    const auto wsignore = ignore(ws);
    const auto is_prival = [](uint16_t in) {
      return in <= 191;
    };
    const auto to_facility_and_severity = [&](uint16_t in) {
      if constexpr (!std::is_same_v<Attribute, unused_type>) {
        x.facility = in / 8;
        x.severity = in % 8;
      }
    };
    // PRIORITY is delimited by <angle brackets>, and is optional
    const auto prival = ignore(
      integral_parser<uint16_t, 3>{}.with(is_prival)->*to_facility_and_severity);
    const auto priority_parser = '<' >> prival >> '>';
    // TIMESTAMP is as specified by RFC (see above)
    // Alternatively, try anything that parsers::time would also accept
    const auto timestamp_parser = legacy_message_timestamp_parser{}
                                  | (parsers::time->*([](tenzir::time t) {
                                      return tenzir::to_string(t);
                                    }));
    // HOST is just whitespace-delimited characters without colon, because the
    // colon comes typically after the TAG.
    const auto host_parser = word;
    // Then, comes the MESSAGE itself.
    //
    // We're diverging from the RFC to produce potentially a little more
    // user-friendly results.
    //
    // In the RFC, TAG is up to 32 alnum characters, and CONTENT is the rest.
    // So, in a message like "foo[123]: bar", TAG is "foo", and CONTENT is
    // "[123]: bar". Because the TAG is terminated by the first non-alnum
    // character, in a message like "foo: bar", the RFC-behavior is even more
    // odd: TAG is "foo", and CONTENT is ": bar".
    //
    // Instead, we try to detect a tag ("foo"), and process id ("123"), and
    // include the content without any of these, and any preceding whitespace.
    // Additionally, we include the MESSAGE in its entirety, for the case that
    // there's really no app name and pid.
    const auto p = ~(priority_parser >> ignore(*parsers::space))
                   >> timestamp_parser >> wsignore
                   >> -(host_parser >> wsignore);
    std::string_view message;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      if (not p(f, l, unused)) {
        return false;
      }
    } else {
      if (not p(f, l, x.timestamp, x.host)) {
        return false;
      }
      message = std::string_view{f, l};
    }
    // Parse MESSAGE into its constituent parts: TAG, PROCESS_ID, and CONTENT.
    if constexpr (not std::is_same_v<Attribute, unused_type>) {
      // Even though alnum characters are the only one that the RFC specifies,
      // the reality is more diverse, e.g.,
      // Microsoft-Windows-Security-Mitigations[4340] is a thing.
      const auto tag_id_parser
        = +(parsers::alnum | parsers::ch<'-'> | parsers::ch<'_'>);
      const auto process_id_parser = '[' >> +parsers::alnum >> ']';
      // To assess whether a TAG is present, we want at least one whitespace
      // character after the ":". Otherwise we may end up in a situation where
      // we eagerly grab characters from CONTENT when it has a prefix of alnum
      // characters followed by a colon, e.g., as in the CEF and LEEF formats.
      const auto tag_parser = -tag_id_parser >> -process_id_parser >> ':'
                              >> (wsignore | parsers::eoi);
      auto begin = message.begin();
      const auto end = message.end();
      if (not tag_parser(begin, end, x.tag, x.process_id)) {
        x.tag = std::nullopt;
        x.process_id = std::nullopt;
      }
      x.content.assign(begin, end);
    }
    return true;
  }
};

template <typename Message>
struct syslog_row {
  syslog_row(Message msg, size_t line_no)
    : parsed(std::move(msg)), starting_line_no(line_no) {
  }

  void emit_diag(std::string_view parser_name, diagnostic_handler& diag) const {
    if (line_count == 1) {
      diagnostic::error("syslog parser ({}) failed", parser_name)
        .note("input line number {}", starting_line_no)
        .emit(diag);
      return;
    }
    diagnostic::error("syslog parser ({}) failed", parser_name)
      .note("input lines number {} to {}", starting_line_no,
            starting_line_no + line_count - 1)
      .emit(diag);
  }

  Message parsed;
  size_t starting_line_no;
  size_t line_count{1};
};

struct syslog_builder {
public:
  using message_type = message;
  using row_type = syslog_row<message_type>;

  syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row));
  }

  auto add_line_to_latest(std::string_view line) -> void {
    TENZIR_ASSERT(not rows_.empty());
    auto& latest = rows_.back();
    if (not latest.parsed.msg) {
      latest.parsed.msg.emplace(line);
    } else {
      latest.parsed.msg->push_back('\n');
      latest.parsed.msg->append(line);
    }
    ++latest.line_count;
  }

  static auto
  finish_single(row_type& row, multi_series_builder& builder) -> bool {
    auto r = builder.record();
    auto& msg = row.parsed;
    r.exact_field("facility").data(msg.hdr.facility);
    r.exact_field("severity").data(msg.hdr.severity);
    r.exact_field("version").data(msg.hdr.version);
    r.exact_field("timestamp").data(std::move(msg.hdr.ts));
    r.exact_field("hostname").data(std::move(msg.hdr.hostname));
    r.exact_field("app_name").data(std::move(msg.hdr.app_name));
    r.exact_field("process_id").data(std::move(msg.hdr.process_id));
    r.exact_field("message_id").data(std::move(msg.hdr.msg_id));
    r.exact_field("structured_data").data(std::move(msg.data));
    r.exact_field("message").data(std::move(msg.msg));
    return true;
  }
  std::vector<row_type> rows_{};
};

struct legacy_syslog_builder {
public:
  using message_type = legacy_message;
  using row_type = syslog_row<message_type>;

  legacy_syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row));
  }

  auto add_line_to_latest(std::string_view line) -> void {
    TENZIR_ASSERT(not rows_.empty());
    auto& latest = rows_.back();
    latest.parsed.content.push_back('\n');
    latest.parsed.content.append(line);
    ++latest.line_count;
  }

  static auto
  finish_single(row_type& row, multi_series_builder& builder) -> bool {
    auto& msg = row.parsed;
    auto r = builder.record();
    r.exact_field("facility").data(msg.facility);
    r.exact_field("severity").data(msg.severity);
    r.exact_field("timestamp").data_unparsed(std::move(msg.timestamp));
    r.exact_field("hostname").data(std::move(msg.host));
    r.exact_field("app_name").data(std::move(msg.tag));
    r.exact_field("process_id").data(std::move(msg.process_id));
    r.exact_field("content").data(msg.content);
    // if (not builder.add(msg.facility, msg.severity, msg.timestamp, msg.host,
    //                     msg.tag, msg.process_id, msg.content)) {
    //   row.emit_diag("RFC 3164", diag);
    //   return false;
    // }
    return true;
  }

  std::vector<row_type> rows_{};
};

struct unknown_syslog_builder {
public:
  using message_type = std::string;
  using row_type = syslog_row<message_type>;

  unknown_syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row.parsed));
  }

  static auto add_line_to_latest(std::string_view) -> void {
    TENZIR_UNREACHABLE();
  }

  static auto finish_all_but_last(multi_series_builder&) -> void {
    TENZIR_UNREACHABLE();
  }

  static auto finish_single(message_type& row, multi_series_builder& msb) {
    msb.record().exact_field("syslog_message").data(std::move(row));
    return true;
  }

  std::vector<std::string> rows_;
};

auto parse_loop(generator<std::optional<std::string_view>> lines,
                operator_control_plane& ctrl,
                multi_series_builder::options opts) -> generator<table_slice> {
  std::variant<syslog_builder, legacy_syslog_builder, unknown_syslog_builder>
    builder{std::in_place_type<unknown_syslog_builder>};
  auto dh = transforming_diagnostic_handler{
    ctrl.diagnostics(), [](auto diag) {
      diag.message = fmt::format("syslog parser: {}", diag.message);
      return diag;
    }};
  const auto timeout = opts.settings.timeout;
  auto msb = multi_series_builder{std::move(opts), dh};
  // This will be called when switching the builder and at the very end of
  // execution
  const auto finish_all = [&]() {
    return std::visit(
      [&](auto& b) {
        for (auto& row : b.rows_) {
          if (not b.finish_single(row, msb)) {
            return;
          }
        }
        b.rows_.clear();
      },
      builder);
  };
  // This will be called periodically to move events from the syslog aggregator
  // into the multi_series_builder. It doesnt finish the last event, because the
  // next line *may* be a continuation of the previous message
  const auto finish_all_but_last = [&]() {
    return std::visit(
      [&](auto& b) {
        for (auto& row : std::views::take(b.rows_, b.rows_.size() - 1)) {
          if (not b.finish_single(row, msb)) {
            return;
          }
        }
        if (not b.rows_.empty()) {
          b.rows_.erase(b.rows_.begin(), b.rows_.end() - 1);
        }
      },
      builder);
  };
  // adds a new event to the currently active syslog builder
  const auto add_new = [&]<typename Message>(Message&& msg, size_t line_no) {
    std::visit(
      [&]<typename Builder>(Builder& b) {
        if constexpr (std::is_constructible_v<typename Builder::message_type,
                                              std::remove_cvref_t<Message>>) {
          b.add_new(
            typename Builder::row_type{std::forward<Message>(msg), line_no});
        } else {
          TENZIR_UNREACHABLE();
        }
      },
      builder);
  };
  // changes the currently active syslog builder
  const auto change_builder = [&]<typename Builder>(tag<Builder>) {
    if (std::holds_alternative<Builder>(builder)) {
      return;
    }
    finish_all();
    builder.template emplace<Builder>();
  };
  auto line_nr = size_t{0};
  auto last_finish = time::clock::now();
  for (auto&& line : lines) {
    for (auto&& slice : msb.yield_ready_as_table_slice()) {
      co_yield std::move(slice);
    }
    // We need our own timeout logic here, because we dont directly write events
    // into the MSB. Only on a `finish_all` or `finish_all_but_last` call events
    // are actually written into the MSB.
    auto now = time::clock::now();
    if (now - last_finish > timeout) {
      finish_all_but_last();
      last_finish = now;
    }
    if (not line) {
      co_yield {};
      continue;
    }
    ++line_nr;
    if (line->empty()) {
      continue;
    }
    const auto* f = line->begin();
    const auto* const l = line->end();
    message msg{};
    legacy_message legacy_msg{};
    if (auto parser = message_parser{}; parser(f, l, msg)) {
      // This line is a valid new-RFC (5424) syslog message.
      // Store it in the builder
      change_builder(tag_v<syslog_builder>);
      add_new(std::move(msg), line_nr);
    } else if (auto legacy_parser = legacy_message_parser{};
               legacy_parser(f, l, legacy_msg)) {
      // Same as above, except it's an old-RFC (3164) syslog message.
      change_builder(tag_v<legacy_syslog_builder>);
      add_new(std::move(legacy_msg), line_nr);
    } else if (std::holds_alternative<unknown_syslog_builder>(builder)) {
      // This line is not a valid syslog message.
      // The current builder is `unknown_syslog_builder`,
      // so this line will also become an event of type `syslog.unknown`.
      add_new(std::string{*line}, line_nr);
    } else {
      // This line is not a valid syslog message,
      // but the previous line was.
      // Let's assume that we have a multiline syslog message,
      // and append this current line to the previous message.
      std::visit(
        [&](auto& b) {
          b.add_line_to_latest(*line);
        },
        builder);
    }
  }
  finish_all();
  for (auto&& slice : msb.finalize_as_table_slice()) {
    co_yield std::move(slice);
  }
}

class syslog_parser final : public plugin_parser {
public:
  syslog_parser() = default;

  syslog_parser(multi_series_builder::options opts) : opts_{std::move(opts)} {
  }

  auto name() const -> std::string override {
    return "syslog";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_loop(to_lines(std::move(input)), ctrl, opts_);
  }

  friend auto inspect(auto& f, syslog_parser& x) -> bool {
    return f.apply(x.opts_);
  }

private:
  multi_series_builder::options opts_;
};

class plugin final : public virtual parser_plugin<syslog_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    parser.parse(p);
    auto dh = collecting_diagnostic_handler{};
    auto msb_opts = msb_parser.get_options(dh);
    msb_opts->settings.default_schema_name = "tenzir.syslog";
    for (auto&& diag : std::move(dh).collect()) {
      if (diag.severity == severity::error) {
        throw diag;
      }
    }
    return std::make_unique<syslog_parser>(std::move(*msb_opts));
  }
};

class read_syslog final
  : public virtual operator_plugin2<parser_adapter<syslog_parser>> {
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto parser = argument_parser2::operator_("read_syslog");
    auto msb_parser = multi_series_builder_argument_parser{};
    msb_parser.add_all_to_parser(parser);
    TRY(parser.parse(inv, ctx));
    TRY(auto opts, msb_parser.get_options(ctx.dh()));
    return std::make_unique<parser_adapter<syslog_parser>>(
      syslog_parser{std::move(opts)});
  }
};

} // namespace
} // namespace tenzir::plugins::syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::read_syslog)
