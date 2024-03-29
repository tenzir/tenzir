//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"
#include "tenzir/detail/fdoutbuf.hpp"

#include <ostream>

namespace tenzir::detail {

/// An output stream which wraps a ::fdoutbuf.
class [[deprecated]] fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  TENZIR_DIAGNOSTIC_PUSH
  TENZIR_DIAGNOSTIC_IGNORE_DEPRECATED
  fdoutbuf buf_;
  TENZIR_DIAGNOSTIC_POP
};

} // namespace tenzir::detail
