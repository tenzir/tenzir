//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/port.hpp"

#include <optional>
#include <string>

namespace tenzir {

/// A transport-layer endpoint consisting of host and port.
struct endpoint {
  std::string host;               ///< The hostname or IP address.
  std::optional<class port> port; ///< The transport-layer port.

  friend auto inspect(auto& f, endpoint& x) -> bool {
    return f.object(x).fields(f.field("host", x.host), f.field("port", x.port));
  }
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::endpoint> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <class FormatContext>
  auto format(const ::tenzir::endpoint& value, FormatContext& ctx) const {
    auto out = ctx.out();
    out = fmt::format_to(out, "{}", value.host);
    if (value.port) {
      out = fmt::format_to(out, ":{}", value.port.value());
    }
    return out;
  }
};

} // namespace fmt
