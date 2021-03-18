// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <streambuf>

namespace vast::detail {

/// A streambuffer that proxies writes to an underlying POSIX file descriptor.
class fdoutbuf : public std::streambuf {
public:
  /// Constructs an output streambuffer from a POSIX file descriptor.
  /// @param fd The file descriptor to construct the streambuffer for.
  fdoutbuf(int fd);

protected:
  int_type overflow(int_type c) override;
  std::streamsize xsputn(const char* s, std::streamsize n) override;

private:
  int fd_;
};

} // namespace vast::detail

