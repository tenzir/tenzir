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

struct glob_star {
  friend auto inspect(auto& f, glob_star& x) -> bool {
    return f.object(x).fields();
  }
};

struct glob_star_star {
  bool slash;

  friend auto inspect(auto& f, glob_star_star& x) -> bool {
    return f.apply(x.slash);
  }
};

using glob_part = variant<std::string, glob_star, glob_star_star>;

using glob = std::vector<glob_part>;

using glob_view = std::span<const glob_part>;

auto parse_glob(std::string_view string) -> glob;

auto matches(std::string_view string, glob_view glob) -> bool;

} // namespace tenzir
