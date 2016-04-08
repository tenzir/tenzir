#ifndef VAST_CONCEPT_PRINTABLE_VAST_ADDRESS_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_ADDRESS_HPP

#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include <cstring>

#include "vast/address.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

namespace vast {

template <>
struct access::printer<address> : vast::printer<access::printer<address>> {
  using attribute = address;

  template <typename Iterator>
  bool print(Iterator& out, address const& a) const {
    char buf[INET6_ADDRSTRLEN];
    std::memset(buf, 0, sizeof(buf));
    auto result = a.is_v4()
      ? inet_ntop(AF_INET, &a.bytes_[12], buf, INET_ADDRSTRLEN)
      : inet_ntop(AF_INET6, &a.bytes_, buf, INET6_ADDRSTRLEN);
    return result != nullptr && printers::str.print(out, result);
  }
};

template <>
struct printer_registry<address> {
  using type = access::printer<address>;
};

namespace printers {

auto const addr = make_printer<address>{};

} // namespace printers
} // namespace vast

#endif
