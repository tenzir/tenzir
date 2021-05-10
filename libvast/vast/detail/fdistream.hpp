//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/fdinbuf.hpp"

#include <cstddef>
#include <istream>

namespace vast::detail {

/// An input stream which wraps a ::fdinbuf.
class fdistream : public std::istream {
public:
  fdistream(int fd, size_t buffer_size = 8192);

private:
  fdinbuf buf_;
};

} // namespace vast::detail
