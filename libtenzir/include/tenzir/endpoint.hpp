//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/port.hpp"

#include <string>

namespace tenzir {

/// A transport-layer endpoint consisting of host and port.
struct Endpoint {
  std::string host;          ///< The hostname or IP address.
  Option<class port> port{}; ///< The transport-layer port.

  friend auto inspect(auto& f, Endpoint& x) -> bool {
    return f.object(x).fields(f.field("host", x.host), f.field("port", x.port));
  }
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::Endpoint> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(::tenzir::Endpoint const& value, FormatContext& ctx) const {
    auto out = ctx.out();
    if (value.port) {
      if (value.host.contains(':')) {
        out = fmt::format_to(out, "[{}]", value.host);
      } else {
        out = fmt::format_to(out, "{}", value.host);
      }
      out = fmt::format_to(out, ":{}", *value.port);
    } else {
      out = fmt::format_to(out, "{}", value.host);
    }
    return out;
  }
};

} // namespace fmt
