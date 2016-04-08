#ifndef VAST_CONCEPT_PRINTABLE_VAST_HTTP_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_HTTP_HPP

#include <string>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/numeric/real.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/detail/print_delimited.hpp"
#include "vast/http.hpp"

namespace vast {

struct http_header_printer : printer<http_header_printer> {
  using attribute = http::header;

  template <typename Iterator>
  bool print(Iterator& out, http::header const& hdr) const {
    using namespace printers;
    return str.print(out, hdr.name) 
        && str.print(out, ": ") 
        && str.print(out, hdr.value);
  }
};

template <>
struct printer_registry<http::header> {
  using type = http_header_printer;
};

struct http_response_printer : printer<http::response> {
  using attribute = http::response;

  template <typename Iterator>
  bool print(Iterator& out, http::response const& res) const {
    using namespace printers;
    auto version = real_printer<double, 1>{};
    return str.print(out, res.protocol) && any.print(out, '/')
           && version.print(out, res.version) && any.print(out, ' ')
           && u32.print(out, res.status_code) && any.print(out, ' ')
           && str.print(out, res.status_text) && str.print(out, "\r\n")
           && detail::print_delimited(res.headers.begin(), res.headers.end(),
                                      out, "\r\n")
           && str.print(out, "\r\n") && str.print(out, "\r\n")  
           && str.print(out, res.body);
  }
};

template <>
struct printer_registry<http::response> {
  using type = http_response_printer;
};

} // namespace vast

#endif
