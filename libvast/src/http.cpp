//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/http.hpp"

#include <algorithm>
#include <cctype>

namespace vast {
namespace http {

const header* message::header(const std::string& name) const {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size())
      return false;
    for (auto i = 0u; i < name.size(); ++i)
      if (::toupper(x.name[i]) != ::toupper(name[i]))
        return false;
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
}

} // namespace http
} // namespace vast
