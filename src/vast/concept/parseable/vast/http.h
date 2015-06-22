#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include <string>
#include "vast/access.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/string/char.h"
#include "vast/util/http.h"


namespace vast {

class http_parser : public parser<http_parser>
{
public:
  using attribute = util::http_request;

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
      util::http_request request(get<0>(h),get<1>(h),get<2>(h));
      for (auto& header_field : get<3>(h))
      {
        auto key_v = get<0>(header_field);
        auto value_v = get<1>(header_field);
        auto key = std::string(key_v.begin(),key_v.end());
        auto value = std::string(value_v.begin(),value_v.end());
        request.add_header_field(key,value);
      }
      a = request;
      return true;
    }
    return false;
  }

};

class url_parser : public parser<url_parser>
{
public:
  using attribute = unused_type;

  url_parser()
  {
  }

  static auto make()
  {
    auto path_ignor_char = ignore(char_parser{'/'}) | ignore(char_parser{'?'});
	auto path_char = print_parser{} - path_ignor_char;
	auto path_segments =  '/' >> (*(path_char)) % '/';
	auto option_key = +(print_parser{} - '=');
	auto option_value = +(print_parser{} - '&');
    auto option = option_key >> '=' >> option_value;
    auto options = option % '&';
    return path_segments >> '?' >> options;
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
    std::tuple<std::vector<std::vector<char>>,std::vector<std::tuple<std::vector<char>,std::vector<char>>>> h;
    if (p.parse(f, l, h))
    {
      for (auto& path_segments : get<0>(h)){
        (get<0>(a)).push_back(std::string(path_segments.begin(),path_segments.end()));
      }
      for (auto& option : get<1>(h)){
        std::tuple<std::string,std::string> opt;
        get<0>(opt) = std::string(get<0>(option).begin(),get<0>(option).end());
        get<1>(opt) = std::string(get<1>(option).begin(),get<1>(option).end());
	    (get<1>(a)).push_back(std::move(opt));
      }

      return true;
    }
    return false;
  }

};


} // namespace vast

#endif
