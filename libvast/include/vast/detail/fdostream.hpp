//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/fdoutbuf.hpp"

#include <ostream>

namespace vast::detail {

/// An output stream which wraps a ::fdoutbuf.
class fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  fdoutbuf buf_;
};

} // namespace vast::detail
