//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"
#include "vast/detail/fdoutbuf.hpp"

#include <ostream>

namespace vast::detail {

/// An output stream which wraps a ::fdoutbuf.
class [[deprecated]] fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  VAST_DIAGNOSTIC_PUSH
  VAST_DIAGNOSTIC_IGNORE_DEPRECATED
  fdoutbuf buf_;
  VAST_DIAGNOSTIC_POP
};

} // namespace vast::detail
