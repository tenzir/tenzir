//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/fdostream.hpp"

namespace vast {
namespace detail {

fdostream::fdostream(int fd) : std::ostream{nullptr}, buf_{fd} {
  rdbuf(&buf_);
}

} // namespace detail
} // namespace vast
