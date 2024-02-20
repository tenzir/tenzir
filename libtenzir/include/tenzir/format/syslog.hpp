//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric.hpp"
#include "tenzir/concept/parseable/string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/printable/std/chrono.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/line_range.hpp"
#include "tenzir/format/multi_schema_reader.hpp"
#include "tenzir/format/reader.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/module.hpp"
#include "tenzir/time.hpp"

#include <caf/sum_type.hpp>

#include <optional>
#include <string>
#include <type_traits>
#include <utility>

/// This namespace includes parsers and a reader for the Syslog protocol
/// as defined in [RFC5424](https://tools.ietf.org/html/rfc5424).
namespace tenzir::format::syslog {

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
       auto p = ('-'_p >> &(' '_p))  ->*[] { return attribute{}; }
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  std::string host;
  std::optional<std::string> app_name;
  std::optional<std::string> process_id;
  std::string content;
};

// Timestamp as specified by RFC3164:
// Mmm dd hh:mm:ss
struct legacy_message_timestamp_parser
  : parser_base<legacy_message_timestamp_parser> {
  using attribute = std::string;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
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
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parser_literals;
    const auto word = +(parsers::printable - parsers::space);
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
    // HOST is just whitespace-delimited characters (for now, at least)
    const auto host_parser = word;
    // Then, comes the MESSAGE itself.
    //
    // We're diverging from the RFC to produce potentially a little more
    // user-friendly results.
    //
    // In the RFC, TAG is up to 32 alnum characters, and CONTENT is the rest.
    // So, in a message like "FOO[123]: bar", TAG is "FOO", and CONTENT is
    // "[123]: bar". Because the TAG is terminated by the first non-alnum
    // character, in a message like "FOO: bar", the RFC-behavior is even more
    // odd: TAG is "FOO", and CONTENT is ": bar".
    //
    // Instead, we try to detect an app name ("FOO"), and process id ("123"),
    // and include the content without any of these, and any preceding
    // whitespace. Additionally, we include the MESSAGE in its entirety, for the
    // case that there's really no app name and pid.
    const auto message_parser = *parsers::printable;
    const auto p = ~(priority_parser >> ignore(*parsers::space)) // priority
                   >> timestamp_parser >> wsignore               // timestamp
                   >> host_parser >> wsignore                    // host
                   >> message_parser;                            // message
    std::string message;
    if constexpr (std::is_same_v<Attribute, unused_type>) {
      if (not p(f, l, unused)) {
        return false;
      }
    } else {
      if (not p(f, l, x.timestamp, x.host, message)) {
        return false;
      }
    }
    // Parse MESSAGE into its constituent parts,
    // app_name, process_id, content
    if constexpr (!std::is_same_v<Attribute, unused_type>) {
      const auto app_name_parser = +parsers::alnum;
      const auto process_id_parser
        = (ignore('['_p) >> +parsers::alnum >> ignore(']'_p));
      const auto tag_parser = -app_name_parser >> -process_id_parser
                              >> ignore(':'_p) >> ignore(*parsers::space);
      auto msg_f = message.begin();
      const auto msg_l = message.end();
      std::tuple<std::optional<std::string>, std::optional<std::string>> attr{};
      if (tag_parser(msg_f, msg_l, attr)) {
        std::tie(x.app_name, x.process_id) = std::move(attr);
      }
      x.content.assign(msg_f, msg_l);
    }
    return true;
  }
};

inline type make_syslog_type() {
  return type{
    "syslog.rfc5424",
    record_type{{
      {"facility", uint64_type{}},
      {"severity", uint64_type{}},
      {"version", uint64_type{}},
      {"timestamp", time_type{}},
      {"hostname", string_type{}},
      {"app_name", string_type{}},
      {"process_id", string_type{}},
      {"message_id", string_type{}},
      {"structured_data", record_type{}},
      {"message", string_type{}},
    }},
  };
}

inline type make_legacy_syslog_type() {
  return type{"syslog.rfc3164", record_type{{{"facility", uint64_type{}},
                                             {"severity", uint64_type{}},
                                             {"timestamp", string_type{}},
                                             {"hostname", string_type{}},
                                             {"app_name", string_type{}},
                                             {"process_id", string_type{}},
                                             {"content", string_type{}}}}};
}

inline type make_unknown_type() {
  return type{
    "syslog.unknown",
    record_type{{{"syslog_message", string_type{}}}},
  };
}

/// A reader for Syslog messages.
class reader : public multi_schema_reader {
public:
  using super = multi_schema_reader;

  /// Constructs a Syslog reader.
  /// @param options Additional options.
  /// @param input The stream of Syslog messages.
  reader(const caf::settings& options, std::unique_ptr<std::istream> input
                                       = nullptr);

  reader(const reader& other) = delete;
  reader(reader&& other) = default;
  reader& operator=(const reader& other) = delete;
  reader& operator=(reader&& other) = default;

  void reset(std::unique_ptr<std::istream> in) override;

  ~reader() override = default;

  caf::error module(tenzir::module mod) override;

  tenzir::module module() const override;

  const char* name() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  type syslog_rfc5424_type_;
  type syslog_unkown_type_;
};

} // namespace tenzir::format::syslog
