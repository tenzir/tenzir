// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/fdostream.hpp"

namespace vast {
namespace detail {

fdostream::fdostream(int fd) : std::ostream{nullptr}, buf_{fd} {
  rdbuf(&buf_);
}

} // namespace detail
} // namespace vast
