//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/actor-framework/actor-framework
// - Commit:     047986079062f7f4ba6b47559d8894efdb401507
// - Path:       libcaf_core/test/streambuf.cpp
// - Author:     Dominik Charousset
// - Created:    April 14th, 2016 4:48 AM
// - License:    BSD 3-Clause

#include "vast/detail/streambuf.hpp"

#include "caf/config.hpp"
#include "vast/test/test.hpp"

TEST(signed_arraybuf) {
  auto data = std::string{"The quick brown fox jumps over the lazy dog"};
  vast::detail::arraybuf<char> ab{data};
  // Let's read some.
  CAF_CHECK_EQUAL(static_cast<size_t>(ab.in_avail()), data.size());
  CAF_CHECK_EQUAL(ab.sgetc(), 'T');
  std::string buf;
  buf.resize(3);
  auto got = ab.sgetn(&buf[0], 3);
  CAF_CHECK_EQUAL(got, 3);
  CAF_CHECK_EQUAL(buf, "The");
  CAF_CHECK_EQUAL(ab.sgetc(), ' ');
  // Exhaust the stream.
  buf.resize(data.size());
  got = ab.sgetn(&buf[0] + 3, static_cast<std::streamsize>(data.size() - 3));
  CAF_CHECK_EQUAL(static_cast<size_t>(got), data.size() - 3);
  CAF_CHECK_EQUAL(data, buf);
  CAF_CHECK_EQUAL(ab.in_avail(), 0);
  // No more.
  auto c = ab.sgetc();
  CAF_CHECK_EQUAL(c, vast::detail::arraybuf<char>::traits_type::eof());
  // Reset the stream and write into it.
  ab.pubsetbuf(&data[0], static_cast<std::streamsize>(data.size()));
  CAF_CHECK_EQUAL(static_cast<size_t>(ab.in_avail()), data.size());
  auto put = ab.sputn("One", 3);
  CAF_CHECK_EQUAL(put, 3);
  CAF_CHECK(data.compare(0, 3, "One") == 0);
}
