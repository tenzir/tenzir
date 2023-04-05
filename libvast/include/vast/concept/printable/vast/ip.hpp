//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/ip.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <cstring>

namespace vast {

enum class ip_printer_policy {
  any,
  ipv6,
};

template <ip_printer_policy Policy = ip_printer_policy::any>
struct ip_printer : printer_base<ip_printer<Policy>> {
  using attribute = ip;

  template <class Iterator>
  bool print(Iterator& out, const ip& a) const {
    char buf[INET6_ADDRSTRLEN];
    std::memset(buf, 0, sizeof(buf));
    auto bytes = as_bytes(a);
    auto result = [&] {
      if constexpr (Policy == ip_printer_policy::any) {
        if (a.is_v4()) {
          return inet_ntop(AF_INET, &bytes[12], buf, INET_ADDRSTRLEN);
        }
      }
      return inet_ntop(AF_INET6, bytes.data(), buf, INET6_ADDRSTRLEN);
    }();
    return result != nullptr && printers::str.print(out, result);
  }
};

template <>
struct printer_registry<ip> {
  using type = ip_printer<ip_printer_policy::any>;
};

namespace printers {

auto const ip = ip_printer<ip_printer_policy::any>{};
auto const ipv6 = ip_printer<ip_printer_policy::ipv6>{};

} // namespace printers
} // namespace vast
