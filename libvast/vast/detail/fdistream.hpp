/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <cstddef>
#include <istream>

#include "vast/detail/fdinbuf.hpp"

namespace vast::detail {

/// An input stream which wraps a ::fdinbuf.
class fdistream : public std::istream {
public:
  fdistream(int fd, size_t buffer_size = 8192);

private:
  fdinbuf buf_;
};

} // namespace vast::detail

