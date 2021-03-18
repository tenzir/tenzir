// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <ostream>

#include "vast/detail/fdoutbuf.hpp"

namespace vast::detail {

/// An output stream which wraps a ::fdoutbuf.
class fdostream : public std::ostream {
public:
  fdostream(int fd);

private:
  fdoutbuf buf_;
};

} // namespace vast::detail

