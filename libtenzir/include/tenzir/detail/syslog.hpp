//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// @file
/// Shared syslog parser combinators and multi-schema builder types used by
/// both the legacy `syslog_parser` and the new `ReadSyslog` operator.

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/numeric.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/tenzir/data.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/concept/printable/std/chrono.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/tql2/ast.hpp>

#include <string_view>

namespace tenzir::plugins::syslog {

/// Maximum allowed syslog message size for octet-counting (16MB).
constexpr auto max_syslog_message_size = size_t{16 * 1024 * 1024};

/// A parser that parses an optional value whose nullopt is presented as a dash.
template <class Parser>
struct maybe_null_parser : parser_base<maybe_null_parser<Parser>> {
  using attribute = typename std::decay_t<Parser>::attribute;

  explicit maybe_null_parser(Parser parser) : parser_{std::move(parser)} {
  }

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    // clang-format off
    auto p = ('-'_p >> &(' '_p)) ->*[] { return Attribute{}; }
             | parser_ ->*[](attribute in) { return Attribute{in}; };
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
  std::optional<std::string> hostname;
  std::optional<std::string> app_name;
  std::optional<std::string> process_id;
  std::optional<std::string> msg_id;

  friend auto inspect(auto& f, header& x) -> bool {
    return f.object(x).fields(
      f.field("facility", x.facility), f.field("severity", x.severity),
      f.field("version", x.version), f.field("ts", x.ts),
      f.field("hostname", x.hostname), f.field("app_name", x.app_name),
      f.field("process_id", x.process_id), f.field("msg_id", x.msg_id));
  }
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
      if constexpr (not std::is_same_v<Attribute, unused_type>) {
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
using parameter = std::pair<std::string, std::string>;

/// Parser for one structured data element parameter.
/// @relates parameter
struct parameter_parser : parser_base<parameter_parser> {
  using attribute = parameter;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::printable, parsers::rep, parsers::ch;
    // space, =, ", and ] are not allowed in the key of the parameter.
    auto key = +(printable - '=' - ' ' - ']' - '"');
    // \ is used to escape characters.
    auto esc = ignore(ch<'\\'>);
    // ], ", \ must be escaped.
    auto escaped = esc >> (ch<']'> | ch<'\\'> | ch<'"'>);
    // We allow not escaping it in some situations to be more permissive.
    auto can_come_after_closing_bracket = parsers::eoi | ' ' | '\n' | '[';
    auto can_come_after_quote = (' ' | ("]" >> can_come_after_closing_bracket));
    auto not_escaped = printable - ('"' >> can_come_after_quote);
    auto value = escaped | not_escaped;
    auto quoted_value = '"' >> *value >> '"';
    // Some emitters omit quotes around PARAM-VALUE entirely.
    auto bare_value = ! ch<'"'> >> +(printable - ' ' - ';' - ']');
    auto p = ' ' >> key >> '=' >> (quoted_value | bare_value);
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.first, x.second);
    }
  }
};

/// A structured data element.
struct structured_data_element {
  std::string id;
  std::vector<parameter> params;

  friend auto inspect(auto& f, structured_data_element& x) -> bool {
    return f.object(x).fields(f.field("id", x.id), f.field("params", x.params));
  }
};

/// Parser for structured data elements.
/// @relates structured_data_element
struct structured_data_element_parser
  : parser_base<structured_data_element_parser> {
  using attribute = structured_data_element;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto sd_id = +(parsers::printable - ' ' - '=' - ']' - '"');
    auto p = '[' >> sd_id >> +parameter_parser{} >> ']';
    return p(f, l, x.id, x.params);
  }
};

/// Parser for structured data of a Syslog message.
/// @relates structured_data
struct structured_data_parser : parser_base<structured_data_parser> {
  using attribute = std::vector<structured_data_element>;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parsers;
    auto p = maybe_null(+structured_data_element_parser{});
    return p(f, l, x);
  }
};

// Check Point logs frequently omit the SD-ID and only emit parameters.
// We normalize such payloads under a stable synthetic SD-ID.
inline auto const checkpoint_default_sdid = std::string{"checkpoint_2620"};

/// Parser for one Check Point structured data element parameter.
///
/// Examples:
/// - `action="Accept"`
/// - `action:"Accept"`
/// @relates parameter
struct checkpoint_param : parser_base<checkpoint_param> {
  using attribute = parameter;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::printable, parsers::rep, parsers::ch;
    using namespace parser_literals;
    auto key_char = printable - '='_p - ' '_p - ']'_p - '"'_p;
    auto key = +key_char;
    auto checkpoint_key_char = key_char - ':'_p;
    auto non_terminal_colon = ch<':'> >> ! '\"'_p;
    auto checkpoint_key
      = rep(checkpoint_key_char | non_terminal_colon, 1, 31) >> ':'_p;
    auto esc = '\\'_p;
    auto escaped = esc >> (ch<']'> | ch<'\\'> | ch<'"'>);
    auto can_come_after_closing_bracket
      = parsers::eoi | ' '_p | '\n'_p | '['_p | ';'_p;
    auto value_terminator
      = '"'_p >> (' '_p | ';'_p | (']'_p >> can_come_after_closing_bracket));
    auto value_char = escaped | (! value_terminator >> printable);
    auto quoted_value = '"'_p >> *value_char >> '"'_p;
    auto bare_value = ! '"'_p >> +(printable - ' '_p - ';'_p - ']'_p);
    auto rfc_parameter = key >> '='_p >> (quoted_value | bare_value);
    auto checkpoint_parameter = checkpoint_key >> (quoted_value | bare_value);
    auto p = rfc_parameter | checkpoint_parameter;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return p(f, l, unused);
    } else {
      return p(f, l, x.first, x.second);
    }
  }
};

/// Parser for all Check Point structured data element parameters.
///
/// Example: `action:"Accept"; conn_direction:"Incoming"`
struct checkpoint_params : parser_base<checkpoint_params> {
  using attribute = std::vector<parameter>;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    auto sep = +' '_p | (';'_p >> *' '_p);
    auto p = checkpoint_param{} % sep;
    return p(f, l, x);
  }
};

/// Parser for Check Point structured data elements.
/// @relates structured_data_element
struct checkpoint_structured_data_element_parser
  : parser_base<checkpoint_structured_data_element_parser> {
  using attribute = structured_data_element;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    using parsers::printable, parsers::rep, parsers::ch;
    auto sd_id = rep(printable - '=' - ' ' - ']' - '"', 1, 32);
    auto opt_sd_id = -(sd_id >> +' '_p)->*[](std::optional<std::string> id) {
      return id ? *id : checkpoint_default_sdid;
    };
    auto params = checkpoint_params{};
    auto p = '[' >> opt_sd_id >> params >> ']';
    return p(f, l, x.id, x.params);
  }
};

/// Parser for Check Point structured data of a Syslog message.
/// @relates structured_data
struct checkpoint_structured_data_parser
  : parser_base<checkpoint_structured_data_parser> {
  using attribute = std::vector<structured_data_element>;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parsers;
    auto p = maybe_null(+checkpoint_structured_data_element_parser{});
    return p(f, l, x);
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
    if (f == l) {
      return false;
    }
    auto remaining = std::string_view{&*f, static_cast<size_t>(l - f)};
    auto bom = std::string_view{"\xEF\xBB\xBF"};
    if (remaining.starts_with(bom)) {
      remaining.remove_prefix(bom.size());
    }
    if (remaining.empty()) {
      return false;
    }
    if constexpr (not std::is_same_v<Attribute, unused_type>) {
      x = std::string{remaining};
    }
    f = l;
    return true;
  }
};

/// A Syslog message.
struct message {
  header hdr;
  std::vector<structured_data_element> data;
  std::optional<message_content> msg;

  friend auto inspect(auto& f, message& x) -> bool {
    return f.object(x).fields(f.field("hdr", x.hdr), f.field("data", x.data),
                              f.field("msg", x.msg));
  }
};

inline auto merge_duplicate_sd_ids(std::vector<structured_data_element>& data)
  -> void {
  // Combine duplicate SD element IDs. Deduplicate param keys (last wins)
  // to avoid writing the same field twice to the builder.
  // Typically 0-3 elements, so linear scan is optimal.
  for (size_t i = 1; i < data.size();) {
    auto merged = false;
    for (size_t j = 0; j < i; ++j) {
      if (data[j].id == data[i].id) {
        auto& target = data[j].params;
        auto& source = data[i].params;
        for (auto& [sk, sv] : source) {
          auto found = false;
          for (auto& [tk, tv] : target) {
            if (tk == sk) {
              tv = std::move(sv);
              found = true;
              break;
            }
          }
          if (not found) {
            target.emplace_back(std::move(sk), std::move(sv));
          }
        }
        data.erase(data.begin() + detail::narrow_cast<ptrdiff_t>(i));
        merged = true;
        break;
      }
    }
    if (not merged) {
      ++i;
    }
  }
}

/// Parser for RFC 5424 Syslog messages.
/// @relates message
struct message_parser : parser_base<message_parser> {
  using attribute = message;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parsers;
    auto p = header_parser{} >> ' '
             >> (structured_data_parser{} | checkpoint_structured_data_parser{})
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
  std::vector<structured_data_element> data;
  std::string content;

  friend auto inspect(auto& f, legacy_message& x) -> bool {
    return f.object(x).fields(
      f.field("facility", x.facility), f.field("severity", x.severity),
      f.field("timestamp", x.timestamp), f.field("host", x.host),
      f.field("tag", x.tag), f.field("process_id", x.process_id),
      f.field("data", x.data), f.field("content", x.content));
  }
};

/// Timestamp parser for RFC 3164 (Mmm dd hh:mm:ss).
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

/// Parser for legacy (RFC 3164) Syslog messages.
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
      if constexpr (not std::is_same_v<Attribute, unused_type>) {
        x.facility = in / 8;
        x.severity = in % 8;
      }
    };
    const auto prival = ignore(
      integral_parser<uint16_t, 3>{}.with(is_prival)->*to_facility_and_severity);
    const auto priority_parser = '<' >> prival >> '>';
    const auto timestamp_parser = legacy_message_timestamp_parser{}
                                  | (parsers::time->*([](tenzir::time t) {
                                      return tenzir::to_string(t);
                                    }));
    const auto host_parser = word;
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
    if constexpr (not std::is_same_v<Attribute, unused_type>) {
      const auto tag_id_parser = +(parsers::alnum | parsers::ch<'-'>
                                   | parsers::ch<'_'> | parsers::ch<'.'>);
      const auto process_id_parser = '[' >> +(parsers::printable - ']') >> ']';
      const auto tag_parser = -tag_id_parser >> -process_id_parser >> ':'
                              >> (wsignore | parsers::eoi);
      const auto* begin = message.begin();
      const auto* end = message.end();
      if (not tag_parser(begin, end, x.tag, x.process_id)) {
        x.tag = std::nullopt;
        x.process_id = std::nullopt;
      }
      const auto structured_data_prefix
        = &'['_p
          >> (structured_data_parser{} | checkpoint_structured_data_parser{})
          >> -(' '_p >> message_content_parser{}) >> *' '_p >> parsers::eoi;
      auto data = std::vector<structured_data_element>{};
      auto msg = std::optional<message_content>{};
      if (structured_data_prefix(begin, end, data, msg)) {
        x.data = std::move(data);
        x.content = std::move(msg).value_or("");
      } else {
        x.content.assign(begin, end);
      }
    }
    return true;
  }
};

struct cisco_datetime {
  uint16_t year = 0;
  std::string month;
  uint16_t day = 0;
  uint16_t hour = 0;
  uint16_t minute = 0;
  uint16_t second = 0;
  std::string timezone;
};

struct cisco_short_datetime {
  std::string month;
  uint16_t day = 0;
  uint16_t hour = 0;
  uint16_t minute = 0;
  uint16_t second = 0;
  std::string subsecond;
  std::optional<std::string> timezone;
};

struct cisco_mmdd_datetime {
  uint16_t month = 0;
  uint16_t day = 0;
  uint16_t hour = 0;
  uint16_t minute = 0;
  uint16_t second = 0;
};

struct cisco_uptime_short_datetime {
  uint16_t hour = 0;
  uint16_t minute = 0;
  uint16_t second = 0;
};

struct cisco_uptime_long_datetime {
  uint32_t day = 0;
  uint16_t hour = 0;
};

/// Parser for Cisco date-time fields like:
/// `2026 Apr 14 08:45:52 UTC`
struct cisco_datetime_parser : parser_base<cisco_datetime_parser> {
  using attribute = cisco_datetime;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, Iterator const& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto is_month = [](std::string const& mon) {
      return mon == "Jan" or mon == "Feb" or mon == "Mar" or mon == "Apr"
             or mon == "May" or mon == "Jun" or mon == "Jul" or mon == "Aug"
             or mon == "Sep" or mon == "Oct" or mon == "Nov" or mon == "Dec";
    };
    const auto is_year = [](uint16_t y) {
      return y >= 1900 and y <= 2100;
    };
    const auto ws = +parsers::space;
    const auto word = +(parsers::printable - ' ' - ':');
    // We intentionally do not use `std::chrono::parse` here:
    // Cisco timezone tokens are free-form abbreviations (for example `UTC`,
    // `PDT`, `IST`) that are not consistently validated across standard
    // library implementations and tzdb configurations.
    const auto year_parser = integral_parser<uint16_t, 4>{}.with(is_year);
    const auto day_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t d) {
          return d >= 1 and d <= 31;
        });
    const auto hour_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t h) {
          return h <= 23;
        });
    const auto minsec_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t s) {
          return s <= 59;
        });
    auto year = uint16_t{};
    auto month = std::string{};
    auto day = uint16_t{};
    auto hour = uint16_t{};
    auto minute = uint16_t{};
    auto second = uint16_t{};
    auto timezone = std::string{};
    if (not year_parser(f, l, year)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not word.with(is_month)(f, l, month)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not day_parser(f, l, day)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not hour_parser(f, l, hour)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, minute)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, second)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not word(f, l, timezone)) {
      return false;
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return true;
    }
    x = {};
    x.year = year;
    x.month = std::move(month);
    x.day = day;
    x.hour = hour;
    x.minute = minute;
    x.second = second;
    x.timezone = std::move(timezone);
    return true;
  }
};

/// Parser for Cisco date-time fields like:
/// `Apr 14 08:45:52.113 UTC`
/// `Apr 14 08:45:52.113`
struct cisco_short_datetime_parser : parser_base<cisco_short_datetime_parser> {
  using attribute = cisco_short_datetime;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, Iterator const& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto is_month = [](std::string const& mon) {
      return mon == "Jan" or mon == "Feb" or mon == "Mar" or mon == "Apr"
             or mon == "May" or mon == "Jun" or mon == "Jul" or mon == "Aug"
             or mon == "Sep" or mon == "Oct" or mon == "Nov" or mon == "Dec";
    };
    const auto ws = +parsers::space;
    const auto word = +(parsers::printable - ' ' - ':');
    const auto day_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t d) {
          return d >= 1 and d <= 31;
        });
    const auto hour_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t h) {
          return h <= 23;
        });
    const auto minsec_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t s) {
          return s <= 59;
        });
    auto month = std::string{};
    auto day = uint16_t{};
    auto hour = uint16_t{};
    auto minute = uint16_t{};
    auto second = uint16_t{};
    auto subsecond = std::string{};
    auto timezone = std::optional<std::string>{};
    if (not word.with(is_month)(f, l, month)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not day_parser(f, l, day)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not hour_parser(f, l, hour)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, minute)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, second)) {
      return false;
    }
    if (f != l and *f == '.') {
      ++f;
      auto const* const begin = f;
      while (f != l and *f >= '0' and *f <= '9') {
        ++f;
      }
      if (begin == f) {
        return false;
      }
      subsecond.assign(begin, f);
    }
    {
      auto it = f;
      if (ws(it, l, unused)) {
        auto tz = std::string{};
        if (not word(it, l, tz)) {
          return false;
        }
        timezone = std::move(tz);
        f = it;
      }
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return true;
    }
    x = {};
    x.month = std::move(month);
    x.day = day;
    x.hour = hour;
    x.minute = minute;
    x.second = second;
    x.subsecond = std::move(subsecond);
    x.timezone = std::move(timezone);
    return true;
  }
};

/// Parser for Cisco date-time fields like:
/// `3/1 18:47:02`
struct cisco_mmdd_datetime_parser : parser_base<cisco_mmdd_datetime_parser> {
  using attribute = cisco_mmdd_datetime;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, Iterator const& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto ws = +parsers::space;
    const auto month_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t m) {
          return m >= 1 and m <= 12;
        });
    const auto day_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t d) {
          return d >= 1 and d <= 31;
        });
    const auto hour_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t h) {
          return h <= 23;
        });
    const auto minsec_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t s) {
          return s <= 59;
        });
    auto month = uint16_t{};
    auto day = uint16_t{};
    auto hour = uint16_t{};
    auto minute = uint16_t{};
    auto second = uint16_t{};
    if (not month_parser(f, l, month)) {
      return false;
    }
    if (not '/'_p(f, l, unused)) {
      return false;
    }
    if (not day_parser(f, l, day)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not hour_parser(f, l, hour)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, minute)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, second)) {
      return false;
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return true;
    }
    x = {};
    x.month = month;
    x.day = day;
    x.hour = hour;
    x.minute = minute;
    x.second = second;
    return true;
  }
};

/// Parser for Cisco uptime fields like:
/// `18:47:02`
struct cisco_uptime_short_datetime_parser
  : parser_base<cisco_uptime_short_datetime_parser> {
  using attribute = cisco_uptime_short_datetime;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, Iterator const& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto hour_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t h) {
          return h <= 23;
        });
    const auto minsec_parser
      = integral_parser<uint16_t, 2, 2>{}.with([](uint16_t s) {
          return s <= 59;
        });
    auto hour = uint16_t{};
    auto minute = uint16_t{};
    auto second = uint16_t{};
    if (not hour_parser(f, l, hour)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, minute)) {
      return false;
    }
    if (not ':'_p(f, l, unused)) {
      return false;
    }
    if (not minsec_parser(f, l, second)) {
      return false;
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return true;
    }
    x = {};
    x.hour = hour;
    x.minute = minute;
    x.second = second;
    return true;
  }
};

/// Parser for Cisco long uptime fields like:
/// `2 18`
struct cisco_uptime_long_datetime_parser
  : parser_base<cisco_uptime_long_datetime_parser> {
  using attribute = cisco_uptime_long_datetime;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, Iterator const& l, Attribute& x) const -> bool {
    const auto ws = +parsers::space;
    const auto day_parser = integral_parser<uint32_t, 10, 1>{};
    const auto hour_parser
      = integral_parser<uint16_t, 2, 1>{}.with([](uint16_t h) {
          return h <= 23;
        });
    auto day = uint32_t{};
    auto hour = uint16_t{};
    if (not day_parser(f, l, day)) {
      return false;
    }
    if (not ws(f, l, unused)) {
      return false;
    }
    if (not hour_parser(f, l, hour)) {
      return false;
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      return true;
    }
    x = {};
    x.day = day;
    x.hour = hour;
    return true;
  }
};

/// Parser for Cisco dialect lines like:
/// `<189>: 2026 Apr 14 08:45:52 UTC: %FOO-5-BAR: message`
/// `000199: *Apr 14 08:45:52.113 UTC: %SYS-5-CONFIG_I: message`
struct cisco_legacy_message_parser : parser_base<cisco_legacy_message_parser> {
  using attribute = legacy_message;

  template <typename Iterator, typename Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute& x) const -> bool {
    using namespace parser_literals;
    const auto is_prival = [](uint16_t in) {
      return in <= 191;
    };
    const auto ws = +parsers::space;
    const auto pri_parser
      = '<' >> integral_parser<uint16_t, 3>{}.with(is_prival) >> '>' >> ':';
    const auto seqnum_parser = integral_parser<uint32_t, 9, 1>{};
    auto parsed_pri = std::optional<uint16_t>{};
    auto it = f;
    {
      auto pri = uint16_t{};
      if (pri_parser(it, l, pri)) {
        if (not ws(it, l, unused)) {
          return false;
        }
        parsed_pri = pri;
      }
    }
    auto parse_timestamp = [&](auto& cursor, std::string& out) -> bool {
      auto it2 = cursor;
      if (it2 != l and *it2 == '*') {
        ++it2;
      }
      {
        auto ts = cisco_datetime{};
        auto save = it2;
        if (cisco_datetime_parser{}(it2, l, ts)) {
          out = fmt::format("{} {} {} {:02}:{:02}:{:02} {}", ts.year, ts.month,
                            ts.day, ts.hour, ts.minute, ts.second, ts.timezone);
          cursor = it2;
          return true;
        }
        it2 = save;
      }
      {
        auto ts = cisco_short_datetime{};
        auto save = it2;
        if (cisco_short_datetime_parser{}(it2, l, ts)) {
          auto const subsecond = ts.subsecond.empty()
                                   ? std::string{}
                                   : fmt::format(".{}", ts.subsecond);
          auto const timezone
            = ts.timezone ? fmt::format(" {}", *ts.timezone) : std::string{};
          out = fmt::format("{} {} {:02}:{:02}:{:02}{}{}", ts.month, ts.day,
                            ts.hour, ts.minute, ts.second, subsecond, timezone);
          cursor = it2;
          return true;
        }
        it2 = save;
      }
      {
        auto ts = cisco_mmdd_datetime{};
        auto save = it2;
        if (cisco_mmdd_datetime_parser{}(it2, l, ts)) {
          out = fmt::format("{}/{} {:02}:{:02}:{:02}", ts.month, ts.day,
                            ts.hour, ts.minute, ts.second);
          cursor = it2;
          return true;
        }
        it2 = save;
      }
      {
        auto ts = cisco_uptime_short_datetime{};
        auto save = it2;
        if (cisco_uptime_short_datetime_parser{}(it2, l, ts)) {
          out = fmt::format("{:02}:{:02}:{:02}", ts.hour, ts.minute, ts.second);
          cursor = it2;
          return true;
        }
        it2 = save;
      }
      {
        auto ts = cisco_uptime_long_datetime{};
        auto save = it2;
        if (cisco_uptime_long_datetime_parser{}(it2, l, ts)) {
          out = fmt::format("{} {}", ts.day, ts.hour);
          cursor = it2;
          return true;
        }
        it2 = save;
      }
      return false;
    };
    auto timestamp = std::string{};
    if (not parse_timestamp(it, timestamp)) {
      auto it_with_seq = it;
      auto seq = uint32_t{};
      if (not seqnum_parser(it_with_seq, l, seq)) {
        return false;
      }
      if (not ':'_p(it_with_seq, l, unused)) {
        return false;
      }
      std::ignore = ignore(*parsers::space)(it_with_seq, l, unused);
      if (not parse_timestamp(it_with_seq, timestamp)) {
        return false;
      }
      it = it_with_seq;
    }
    if (not ':'_p(it, l, unused)) {
      return false;
    }
    std::ignore = ignore(*parsers::space)(it, l, unused);
    auto message = std::string_view{it, l};
    auto cisco_tag = std::optional<std::string>{};
    if (not message.empty() and message.front() == '%') {
      auto tf = message.begin();
      auto tag = std::string{};
      const auto token = '%' >> +(parsers::printable - ':' - ' ') >> ':'
                         >> ignore(*parsers::space);
      if (token(tf, message.end(), tag)) {
        cisco_tag = std::move(tag);
        message.remove_prefix(static_cast<size_t>(tf - message.begin()));
      }
    }
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      f = l;
      return true;
    }
    x = {};
    if (parsed_pri) {
      x.facility = *parsed_pri / 8;
      x.severity = *parsed_pri % 8;
    }
    x.timestamp = std::move(timestamp);
    x.tag = std::move(cisco_tag);
    x.content = std::string{message};
    f = l;
    return true;
  }
};

template <typename Message>
struct syslog_row {
  syslog_row() = default;

  syslog_row(Message msg, size_t line_no, std::string raw = {})
    : parsed(std::move(msg)),
      starting_line_no(line_no),
      raw_message(std::move(raw)) {
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
  std::string raw_message;

  friend auto inspect(auto& f, syslog_row& x) -> bool {
    return f.object(x).fields(f.field("parsed", x.parsed),
                              f.field("starting_line_no", x.starting_line_no),
                              f.field("line_count", x.line_count),
                              f.field("raw_message", x.raw_message));
  }
};

struct syslog_builder {
public:
  using message_type = message;
  using row_type = syslog_row<message_type>;

  syslog_builder(multi_series_builder::options opts, diagnostic_handler& dh,
                 Option<ast::field_path> raw_message_field = None{})
    : timeout{opts.settings.timeout},
      merge{opts.settings.merge},
      builder{std::move(opts), dh},
      raw_message_field_{std::move(raw_message_field)} {
  }

  auto add_new(row_type&& row) -> void {
    if (last_message) {
      finish_last();
    }
    last_message_time = time::clock::now();
    last_message = std::move(row);
  }

  auto add_line_to_latest(std::string_view line) -> bool {
    if (not last_message) {
      return false;
    }
    auto& latest = *last_message;
    auto current_size = latest.parsed.msg.value_or("").size();
    if (raw_message_field_) {
      current_size = std::max(current_size, latest.raw_message.size());
    }
    if (current_size + line.size() + 1 > max_syslog_message_size) {
      return false;
    }
    if (not latest.parsed.msg) {
      latest.parsed.msg.emplace(line);
    } else {
      latest.parsed.msg->push_back('\n');
      latest.parsed.msg->append(line);
    }
    if (raw_message_field_) {
      latest.raw_message.push_back('\n');
      latest.raw_message.append(line);
    }
    ++latest.line_count;
    return true;
  }

  auto yield_ready() -> series_builder::YieldReadyResult {
    if (last_message and last_message_time
        and time::clock::now() - *last_message_time > timeout) {
      finish_last();
    }
    return builder.yield_ready_as_table_slice();
  }

  auto finalize_as_table_slice() -> std::vector<table_slice> {
    if (last_message) {
      finish_last();
    }
    return builder.finalize_as_table_slice();
  }

  auto finalize() -> std::vector<series> {
    if (last_message) {
      finish_last();
    }
    return builder.finalize();
  }

  auto finish_last() -> void {
    TENZIR_ASSERT(last_message);
    auto r = builder.record();
    auto& row = last_message->parsed;
    r.exact_field("facility").data(row.hdr.facility);
    r.exact_field("severity").data(row.hdr.severity);
    r.exact_field("version").data(row.hdr.version);
    r.exact_field("timestamp").data(std::move(row.hdr.ts));
    r.exact_field("hostname").data(std::move(row.hdr.hostname));
    r.exact_field("app_name").data(std::move(row.hdr.app_name));
    r.exact_field("process_id").data(std::move(row.hdr.process_id));
    r.exact_field("message_id").data(std::move(row.hdr.msg_id));
    if (merge) {
      merge_duplicate_sd_ids(row.data);
    }
    auto sd = r.exact_field("structured_data").record();
    for (auto& elem : row.data) {
      auto elem_rec = sd.field(elem.id).record();
      for (auto& [k, v] : elem.params) {
        elem_rec.field(k).data_unparsed(std::move(v));
      }
    }
    r.exact_field("message").data(std::move(row.msg));
    if (raw_message_field_) {
      r.field(*raw_message_field_).data(std::move(last_message->raw_message));
    }
    last_message = None{};
    last_message_time = None{};
  }

  duration timeout;
  bool merge;
  multi_series_builder builder;
  Option<time> last_message_time = None{};
  Option<row_type> last_message = None{};
  Option<ast::field_path> raw_message_field_ = None{};
};

struct legacy_syslog_builder {
public:
  using message_type = legacy_message;
  using row_type = syslog_row<message_type>;

  legacy_syslog_builder(multi_series_builder::options opts,
                        diagnostic_handler& dh,
                        Option<ast::field_path> raw_message_field = None{},
                        bool emit_structured_data = false)
    : timeout{opts.settings.timeout},
      builder{std::move(opts), dh},
      raw_message_field_{std::move(raw_message_field)},
      emit_structured_data_{emit_structured_data} {
  }

  auto add_new(row_type&& row) -> void {
    if (last_message) {
      finish_last();
    }
    last_message_time = time::clock::now();
    last_message = std::move(row);
  }

  auto yield_ready() -> series_builder::YieldReadyResult {
    if (last_message and last_message_time
        and time::clock::now() - *last_message_time > timeout) {
      finish_last();
    }
    return builder.yield_ready_as_table_slice();
  }

  auto finalize_as_table_slice() -> std::vector<table_slice> {
    if (last_message) {
      finish_last();
    }
    return builder.finalize_as_table_slice();
  }

  auto finalize() -> std::vector<series> {
    if (last_message) {
      finish_last();
    }
    return builder.finalize();
  }

  auto add_line_to_latest(std::string_view line) -> bool {
    if (not last_message) {
      return false;
    }
    auto& latest = *last_message;
    auto current_size = latest.parsed.content.size();
    if (raw_message_field_) {
      current_size = std::max(current_size, latest.raw_message.size());
    }
    if (current_size + line.size() + 1 > max_syslog_message_size) {
      return false;
    }
    latest.parsed.content.push_back('\n');
    latest.parsed.content.append(line);
    if (raw_message_field_) {
      latest.raw_message.push_back('\n');
      latest.raw_message.append(line);
    }
    ++latest.line_count;
    return true;
  }

  auto finish_last() -> void {
    TENZIR_ASSERT(last_message);
    auto& msg = last_message->parsed;
    TENZIR_ASSERT(emit_structured_data_ or msg.data.empty());
    auto r = builder.record();
    r.exact_field("facility").data(msg.facility);
    r.exact_field("severity").data(msg.severity);
    r.exact_field("timestamp").data_unparsed(std::move(msg.timestamp));
    r.exact_field("hostname").data(std::move(msg.host));
    r.exact_field("app_name").data(std::move(msg.tag));
    r.exact_field("process_id").data(std::move(msg.process_id));
    if (emit_structured_data_) {
      merge_duplicate_sd_ids(msg.data);
      auto sd = r.exact_field("structured_data").record();
      for (auto& elem : msg.data) {
        auto elem_rec = sd.field(elem.id).record();
        for (auto& [k, v] : elem.params) {
          elem_rec.field(k).data_unparsed(std::move(v));
        }
      }
    }
    r.exact_field("content").data(msg.content);
    if (raw_message_field_) {
      r.field(*raw_message_field_).data(std::move(last_message->raw_message));
    }
    last_message = None{};
    last_message_time = None{};
  }

  duration timeout;
  multi_series_builder builder;
  Option<time> last_message_time = None{};
  Option<row_type> last_message = None{};
  Option<ast::field_path> raw_message_field_ = None{};
  bool emit_structured_data_;
};

struct unknown_syslog_builder {
public:
  using message_type = std::string;
  using row_type = syslog_row<message_type>;

  unknown_syslog_builder(multi_series_builder::options opts,
                         diagnostic_handler& dh)
    : builder{std::move(opts), dh} {
  }

  auto yield_ready() -> series_builder::YieldReadyResult {
    return builder.yield_ready_as_table_slice();
  }

  auto finalize_as_table_slice() -> std::vector<table_slice> {
    return builder.finalize_as_table_slice();
  }

  auto finalize() -> std::vector<series> {
    return builder.finalize();
  }

  auto add_new(row_type&& row) -> void {
    builder.record().exact_field("syslog_message").data(std::move(row.parsed));
  }

  multi_series_builder builder;
};

enum class builder_tag {
  syslog_builder,
  legacy_syslog_builder,
  legacy_structured_syslog_builder,
  unknown_syslog_builder,
};

inline auto get_legacy_builder_tag(legacy_message const& msg) -> builder_tag {
  return msg.data.empty() ? builder_tag::legacy_syslog_builder
                          : builder_tag::legacy_structured_syslog_builder;
}

inline auto infuse_new_schema(multi_series_builder::options o)
  -> multi_series_builder::options {
  if (try_as<multi_series_builder::policy_default>(o.policy)) {
    o.policy.emplace<multi_series_builder::policy_schema>("syslog.rfc5424");
  }
  return o;
}

inline auto infuse_legacy_schema(multi_series_builder::options o)
  -> multi_series_builder::options {
  if (try_as<multi_series_builder::policy_default>(o.policy)) {
    o.policy.emplace<multi_series_builder::policy_schema>("syslog.rfc3164");
  }
  return o;
}

inline auto infuse_legacy_structured_schema(multi_series_builder::options o)
  -> multi_series_builder::options {
  if (try_as<multi_series_builder::policy_default>(o.policy)) {
    o.policy.emplace<multi_series_builder::policy_schema>(
      "syslog.rfc3164.structured");
  }
  return o;
}

/// RFC 6587 octet-counting length prefix parser.
/// Parses: MSG-LEN SP
/// where MSG-LEN is a positive decimal (NONZERO-DIGIT *DIGIT per RFC 6587).
inline constexpr auto octet_length_parser = parsers::u32 >> ' ';

} // namespace tenzir::plugins::syslog
