//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <arrow/util/config.h>

#include <string_view>

#if ARROW_VERSION_MAJOR < 10
#  include <arrow/util/string_view.h>
#endif

namespace vast::arrow_compat {

#if ARROW_VERSION_MAJOR < 10
using string_view = arrow::util::string_view;

inline std::string_view align_type(string_view sv) {
  return std::string_view{sv.data(), sv.size()};
}
#else
using string_view = std::string_view;

inline std::string_view align_type(string_view sv) {
  return sv;
}
#endif

} // namespace vast::arrow_compat
