//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/variant.hpp"

namespace tenzir {

struct star {
  friend auto inspect(auto& f, star& x) -> bool {
    return f.object(x).fields();
  }
};

// Things that also match no-slash:
// `**/x`
// Things that do not match no-slash:
// `x**/y` does not match `xy`
// `x**/` does not match `x`
struct double_star {
  bool slash;

  friend auto inspect(auto& f, double_star& x) -> bool {
    return f.apply(x.slash);
  }
};

using glob_part = variant<std::string, star, double_star>;

using glob = std::vector<glob_part>;

using glob_view = std::span<const glob_part>;

inline auto matches(std::string_view string, const glob_view& glob) -> bool {
  if (glob.empty()) {
    // The empty glob only matches the empty string.
    return string.empty();
  }
  auto& head = glob[0];
  auto tail = glob.subspan(1);
  return match(
    head,
    [&](const std::string& part) {
      // The given part must be a prefix.
      if (not string.starts_with(part)) {
        return false;
      }
      return matches(string.substr(part.size()), tail);
    },
    [&](star) {
      if (matches(string, tail)) {
        // The star is allowed to consume nothing.
        return true;
      }
      // Make it consume something.
      if (string.empty() or string[0] == '/') {
        return false;
      }
      return matches(string.substr(1), glob);
    },
    [&](double_star double_star) {
      // The sequence `**/` is parsed into a `double_star` with `slash == true`.
      // It is allowed to consume nothing (not even a slash), but if it consumes
      // something, then it must also consume a slash at the end.
      if (matches(string, tail)) {
        return true;
      }
      // Make it consume something.
      if (string.empty()) {
        return false;
      }
      if (double_star.slash) {
        auto slash = string.find('/');
        if (slash == std::string::npos) {
          return false;
        }
        auto rest = string.substr(slash + 1);
        // The slash may or may not be the end of the double star.
        return matches(rest, glob) or matches(rest, tail);
      }
      return matches(string.substr(1), glob);
    });
}

inline auto parse_glob(std::string_view string) -> glob {
  auto result = glob{};
  while (true) {
    auto pos = string.find('*');
    if (pos != 0) {
      result.emplace_back(std::string{string.substr(0, pos)});
    }
    if (pos == std::string::npos) {
      return result;
    }
    if (pos + 1 < string.size() and string[pos + 1] == '*') {
      if (pos + 2 < string.size() and string[pos + 2] == '/') {
        result.emplace_back(double_star{true});
        string = string.substr(pos + 3);
      } else {
        result.emplace_back(double_star{false});
        string = string.substr(pos + 2);
      }
    } else {
      result.emplace_back(star{});
      string = string.substr(pos + 1);
    }
  }
}


}
