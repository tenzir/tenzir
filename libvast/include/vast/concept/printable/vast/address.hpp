//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/address.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstring>

namespace vast {

struct address_printer : printer_base<address_printer> {
  using attribute = address;

  template <class Iterator>
  bool print(Iterator& out, const address& a) const {
    char buf[INET6_ADDRSTRLEN];
    std::memset(buf, 0, sizeof(buf));
    auto bytes = as_bytes(a);
    auto result = a.is_v4()
                    ? inet_ntop(AF_INET, &bytes[12], buf, INET_ADDRSTRLEN)
                    : inet_ntop(AF_INET6, bytes.data(), buf, INET6_ADDRSTRLEN);
    return result != nullptr && printers::str.print(out, result);
  }
};

template <>
struct printer_registry<address> {
  using type = address_printer;
};

namespace printers {

auto const addr = address_printer{};

} // namespace printers
} // namespace vast
