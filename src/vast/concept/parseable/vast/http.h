#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include <string>
#include "vast/access.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/string/char.h"


namespace vast {

class http_parser : public parser<http_parser>
{
public:
  using attribute = std::tuple<std::string,std::string,std::string,std::map<std::string,std::string>>;

  http_parser()
  {
  }

  static auto make()
  {
    auto first_line =  (+(print_parser{} - ' ') >> ' ') >> (+(print_parser{} - ' ') >> ' ') >> (+(print_parser{} - "\r\n") >> "\r\n");
    auto header_field = (+(print_parser{} - ':') >> ':') >> ignore(*char_parser{' '}) >> (+(print_parser{} - "\r\n") >> "\r\n");
    auto header = *(header_field);
    return first_line >> header;
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const
  {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const
  {
    static auto p = make();
    using std::get;
    std::tuple<std::string,std::string,std::string,std::vector<std::tuple<std::vector<char>,std::vector<char>>>> h;
    if (p.parse(f, l, h))
    {
      std::map<std::string,std::string> m;
      for (auto& header_field : get<3>(h))
      {
        auto key_v = get<0>(header_field);
        auto value_v = get<1>(header_field);
        auto key = std::string(key_v.begin(),key_v.end());
        auto value = std::string(value_v.begin(),value_v.end());
        m[key] = value;
      }
      get<0>(a) = get<0>(h);
      get<1>(a) = get<1>(h);
      get<2>(a) = get<2>(h);
      get<3>(a) = m;
      return true;
    }
    return false;
  }

};

} // namespace vast

#endif
