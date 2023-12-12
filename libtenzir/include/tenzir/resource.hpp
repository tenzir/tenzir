//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <string>

namespace tenzir {

struct resource {
  std::string url;
  uint64_t size;

  template <class Inspector>
  friend auto inspect(Inspector& f, resource& x) {
    return f.object(x)
      .pretty_name("tenzir.resource")
      .fields(f.field("url", x.url), f.field("size", x.size));
  }
};

} // namespace tenzir
