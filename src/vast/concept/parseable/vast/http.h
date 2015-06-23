#ifndef VAST_CONCEPT_PARSEABLE_VAST_TIME_H
#define VAST_CONCEPT_PARSEABLE_VAST_TIME_H

#include <string>
#include "vast/access.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string.h"
#include "vast/concept/parseable/string/char.h"
#include "vast/util/http.h"


namespace vast {



const char HEX2DEC[256] =
{
    /*       0  1  2  3   4  5  6  7   8  9  A  B   C  D  E  F */
    /* 0 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 1 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 2 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 3 */  0, 1, 2, 3,  4, 5, 6, 7,  8, 9,-1,-1, -1,-1,-1,-1,

    /* 4 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 5 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 6 */ -1,10,11,12, 13,14,15,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 7 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* 8 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* 9 */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* A */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* B */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,

    /* C */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* D */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* E */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1,
    /* F */ -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1, -1,-1,-1,-1
};

std::string UriDecode(const std::string & sSrc)
{
    // Note from RFC1630:  "Sequences which start with a percent sign
    // but are not followed by two hexadecimal characters (0-9, A-F) are reserved
    // for future extension"

    const unsigned char * pSrc = (const unsigned char *)sSrc.c_str();
	const int SRC_LEN = sSrc.length();
    const unsigned char * const SRC_END = pSrc + SRC_LEN;
    const unsigned char * const SRC_LAST_DEC = SRC_END - 2;   // last decodable '%'

    char * const pStart = new char[SRC_LEN];
    char * pEnd = pStart;

    while (pSrc < SRC_LAST_DEC)
	{
		if (*pSrc == '%')
        {
            char dec1, dec2;
            if (-1 != (dec1 = HEX2DEC[*(pSrc + 1)])
                && -1 != (dec2 = HEX2DEC[*(pSrc + 2)]))
            {
                *pEnd++ = (dec1 << 4) + dec2;
                pSrc += 3;
                continue;
            }
        }

        *pEnd++ = *pSrc++;
	}

    // the last 2- chars
    while (pSrc < SRC_END)
        *pEnd++ = *pSrc++;

    std::string sResult(pStart, pEnd);
    delete [] pStart;
	return sResult;
}



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
  using attribute = util::http_url;

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
      util::http_url url;
	  for (auto& path_segments : get<0>(h)){
        url.add_path_segment(std::string(path_segments.begin(),path_segments.end()));
      }
      for (auto& option : get<1>(h)){
        std::string key = std::string(get<0>(option).begin(),get<0>(option).end());
        std::string value = std::string(get<1>(option).begin(),get<1>(option).end());
        key = UriDecode(key);
        value = UriDecode(value);
	    url.add_option(key, value);
      }
      a = url;
      return true;
    }
    return false;
  }

};


} // namespace vast

#endif
