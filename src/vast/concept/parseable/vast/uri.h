#ifndef VAST_CONCEPT_PARSEABLE_VAST_URI_H
#define VAST_CONCEPT_PARSEABLE_VAST_URI_H

#include <algorithm>
#include <cctype>
#include <string>

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/numeric/real.h"
#include "vast/uri.h"
#include "vast/util/string.h"

namespace vast {

struct uri_parser : parser<uri_parser> {
  using attribute = uri;

  static auto make() {
    using namespace parsers;
    // rfc 3986
    auto protocol_ignore_char = ignore(char_parser{':'}) | ignore(char_parser{'/'});
    auto protocol = *(print_parser{} - protocol_ignore_char);
    auto hostname = *(print_parser{} - protocol_ignore_char);
    auto port = u16;
    auto path_ignore_char = ignore(char_parser{'/'}) | ignore(char_parser{'?'}) | ignore(char_parser{'#'}) | ignore(char_parser{' '});
    auto path_segment = *(print_parser{} - path_ignore_char) ->* [](std::string p) { return util::percent_unescape(p); };
    auto option_key = +(print_parser{} - '=') ->* [](std::string o) { return util::percent_unescape(o); };
    auto option_ignore_char = ignore(char_parser{'&'}) | ignore(char_parser{'#'}) | ignore(char_parser{' '});
    auto option_value = +(print_parser{} - option_ignore_char) ->* [](std::string o) { return util::percent_unescape(o); };
    auto option = option_key >> '=' >> option_value;
    auto fragment = *(printable - ' ');
    auto uri = 
         -(protocol >> ':')
      >> -("//" >> hostname)
      >> -(':' >> port)
      >> '/' >> path_segment % '/'
      >> -('?' >> option % '&')
      >> -('#' >>  fragment)
      ;
    return uri;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, uri& u) const {
    static auto p = make();
    using std::get;
    std::tuple<
      optional<std::string>,
      optional<std::string>,
      optional<uint16_t>,
      std::vector<std::string>,
      optional<std::vector<std::tuple<std::string,std::string>>>,
      optional<std::string>
    > h;
    if (p.parse(f, l, h))
    {
      if (get<0>(h)) {
        u.protocol = *get<0>(h);
      }
      if (get<1>(h)) {
        u.hostname = *get<1>(h);
      }
      if (get<2>(h)) {
        u.port = *get<2>(h);
      } else {
        u.port = 0;
      }
      u.path = get<3>(h);
      if (get<4>(h))
      {
        for (auto& option : *get<4>(h)){
          auto key = get<0>(option);
          auto value = get<1>(option);
          u.options[key] = value;
        }
      }
      if (get<5>(h)) {
        u.fragment = *get<5>(h);
      }
      return true;
    }
    return false;
  }

};

template <>
struct parser_registry<uri> {
  using type = uri_parser;
};

} // namespace vast

#endif
