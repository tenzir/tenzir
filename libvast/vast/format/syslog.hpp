/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/aliases.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/line_range.hpp"
#include "vast/format/reader.hpp"
#include "vast/format/single_layout_reader.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"

#include <caf/optional.hpp>
#include <caf/sum_type.hpp>

#include <string>
#include <type_traits>
#include <utility>

namespace vast::format::syslog {

/// This namespace includes the syslog message defintion of the syslog protocol
/// defined in [RFC 5424](https://tools.ietf.org/html/rfc5424).

// maybe nil parser
template <class Parser>
struct maybe_nil_parser : parser<maybe_nil_parser<Parser>> {
  using value_type = typename std::decay_t<Parser>::attribute;
  using attribute = std::conditional_t<detail::is_container<value_type>,
                                       value_type, caf::optional<value_type>>;

  explicit maybe_nil_parser(Parser parser) : parser_{std::move(parser)} {
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

template <class Parser>
auto maybe_nil(Parser&& parser) {
  return maybe_nil_parser<Parser>{std::forward<Parser>(parser)};
}

// header of syslog message
struct header {
  uint16_t facility;
  uint16_t severity;
  uint16_t version;
  caf::optional<time> ts; // vast time
  std::string hostname;
  std::string app_name;
  std::string process_id;
  std::string msg_id;
};

// The header_parser parses the header of a Syslog message
struct header_parser : parser<header_parser> {
  using attribute = header;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::print, parsers::rep;
    auto is_prival = [](uint16_t in) { return in <= 191; };

    // retrieve facillity and severity from prival
    auto to_facility_and_severity = [&](uint16_t in) {
      if constexpr (!std::is_same_v<Attribute, unused_type>) {
        x.facility = in / 8;
        x.severity = in % 8;
      }
    };
    auto prival = ignore(
      integral_parser<uint16_t, 3>{}.with(is_prival)->*to_facility_and_severity);
    auto pri = '<' >> prival >> '>';
    auto is_version = [](uint16_t in) { return in > 0; };
    auto version = integral_parser<uint16_t, 3>{}.with(is_version);
    auto hostname = maybe_nil(rep(print - ' ', 1, 255));
    auto app_name = maybe_nil(rep(print - ' ', 1, 48));
    auto process_id = maybe_nil(rep(print - ' ', 1, 128));
    auto msg_id = maybe_nil(rep(print - ' ', 1, 32));
    auto timestamp = maybe_nil(parsers::time);
    auto p = pri >> version >> ' ' >> timestamp >> ' ' >> hostname >> ' '
             >> app_name >> ' ' >> process_id >> ' ' >> msg_id;

    if constexpr (std::is_same_v<Attribute, unused_type>)
      return p(f, l, unused);
    else
      return p(f, l, x.version, x.ts, x.hostname, x.app_name, x.process_id,
               x.msg_id);
  }
};

// one parameter (key and value) of one structured data element
using parameter = std::tuple<std::string, std::string>;

// The parameter_parser parses one structured data paramter
struct parameter_parser : parser<parameter_parser> {
  using attribute = parameter;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::print, parsers::rep, parsers::ch;

    // space, =, ", and ] are not allowed in the key of the parameter
    auto key = rep(print - '=' - ' ' - ']' - '"', 1, 32);
    // \ is used to escape specific character
    auto esc = ignore(ch<'\\'>);
    // ], ", \ need to be escaped
    auto escaped = esc >> (ch<']'> | ch<'\\'> | ch<'"'>);
    auto value = escaped | (print - ']' - '"' - '\\');
    auto p = ' ' >> key >> '=' >> '"' >> *value >> '"';

    if constexpr (std::is_same_v<Attribute, unused_type>)
      return p(f, l, unused);
    else
      return p(f, l, x);
  }
};

// all parameters of one structured data element
using parameters = vast::map;

// The parameters_parser parses the parameters of a structured data element
struct parameters_parser : parser<parameters_parser> {
  using attribute = parameters;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto param = parameter_parser{}->*[&](parameter in) {
      if constexpr (!std::is_same_v<Attribute, unused_type>) {
        auto& [key, value] = in;
        x[key] = value;
      }
    };
    auto p = +param;
    return p(f, l, unused);
  }
};

// id and paramters of one structured data element
using structured_data_element = std::tuple<std::string, parameters>;

// The structured_data_element_parser parses one structured data element
struct structured_data__element_parser
  : parser<structured_data__element_parser> {
  using attribute = structured_data_element;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using parsers::print, parsers::rep;

    auto is_sd_name_char = [](char in) {
      return in != '=' && in != ' ' && in != ']' && in != '"';
    };
    auto sd_name = print - ' ';
    auto sd_name_char = sd_name.with(is_sd_name_char);
    auto sd_id = rep(sd_name_char, 1, 32);
    auto params = parameters_parser{};
    auto p = '[' >> sd_id >> params >> ']';
    if constexpr (std::is_same_v<Attribute, unused_type>)
      return p(f, l, unused);
    else
      return p(f, l, x);
  }
};

// all structured data elements
using structured_data = vast::map;

// The structured_data_parser parses the structured_data elements of one Syslog
// message
struct structured_data_parser : parser<structured_data_parser> {
  using attribute = structured_data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parsers;

    auto sd
      = structured_data__element_parser{}->*[&](structured_data_element in) {
          if constexpr (!std::is_same_v<Attribute, unused_type>) {
            auto& [key, value] = in;
            x[key] = value;
          }
        };
    auto p = maybe_nil(+sd);
    return p(f, l, unused);
  }
};

// the message content of the syslog message
using message_content = std::string;

// The message_content_parser parses the message component of a Syslog message
struct message_content_parser : parser<message_content_parser> {
  using attribute = message_content;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parsers;
    using namespace parser_literals;
    auto bom = "\xEF\xBB\xBF"_p;
    auto p = (bom >> +any) | (+any) | eoi;

    return p(f, l, x);
  }
};

// header, data and msg describe one syslog message
struct message {
  header header;
  structured_data data;
  caf::optional<message_content> msg;
};

// the message parses uses the header_parser, structured_datas_parser and the
// message_content_parser to parse one Syslog message
struct message_parser : parser<message_parser> {
  using attribute = message;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    using namespace parsers;
    auto p = header_parser{} >> ' ' >> structured_data_parser{}
             >> -(' ' >> message_content_parser{});
    if constexpr (std::is_same_v<Attribute, unused_type>)
      return p(f, l, unused);
    else
      return p(f, l, x.header, x.data, x.msg);
  }
};

// A reader for Syslog messages.
class reader : public single_layout_reader {
public:
  using super = single_layout_reader;

  /// Constructs a Syslog reader.
  /// @param table_slice_type The ID for table slice type to build.
  /// @param options Additional options.
  /// @param input The stream of Syslog messages.
  reader(caf::atom_value table_slice_type, const caf::settings& options,
         std::unique_ptr<std::istream> input = nullptr);

  reader(const reader& other) = delete;
  reader(reader&& other) = default;
  reader& operator=(const reader& other) = delete;
  reader& operator=(reader&& other) = default;

  void reset(std::unique_ptr<std::istream> in);

  ~reader() = default;

  caf::error schema(vast::schema sch) override;

  vast::schema schema() const override;

  const char* name() const override;

protected:
  caf::error
  read_impl(size_t max_events, size_t max_slice_size, consumer& f) override;

private:
  std::unique_ptr<std::istream> input_;
  std::unique_ptr<detail::line_range> lines_;
  type syslog_msg_type_;
};
}; // namespace vast::format::syslog
