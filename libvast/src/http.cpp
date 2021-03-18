// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include <algorithm>
#include <cctype>

#include "vast/http.hpp"

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

} // namspace http
} // namspace vast
