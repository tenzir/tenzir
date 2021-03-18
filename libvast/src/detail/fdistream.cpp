// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/fdistream.hpp"

namespace vast {
namespace detail {

fdistream::fdistream(int fd, size_t buffer_size)
  : std::istream{nullptr},
    buf_{fd, buffer_size} {
  rdbuf(&buf_);
}

} // namespace detail
} // namespace vast
