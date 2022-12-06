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

#define SUITE streambuf
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

TEST(unsigned_arraybuf) {
  using buf_type = vast::detail::arraybuf<uint8_t>;
  std::vector<uint8_t> data = {0x0a, 0x0b, 0x0c, 0x0d};
  buf_type ab{data};
  decltype(data) buf;
  std::copy(std::istreambuf_iterator<uint8_t>{&ab},
            std::istreambuf_iterator<uint8_t>{}, std::back_inserter(buf));
  CAF_CHECK_EQUAL(data, buf);
  // Relative positioning.
  using pos_type = buf_type::pos_type;
  using int_type = buf_type::int_type;
  CAF_CHECK_EQUAL(ab.pubseekoff(2, std::ios::beg, std::ios::in), pos_type{2});
  CAF_CHECK_EQUAL(ab.sbumpc(), int_type{0x0c});
  CAF_CHECK_EQUAL(ab.sgetc(), int_type{0x0d});
  CAF_CHECK_EQUAL(ab.pubseekoff(0, std::ios::cur, std::ios::in), pos_type{3});
  CAF_CHECK_EQUAL(ab.pubseekoff(-2, std::ios::cur, std::ios::in), pos_type{1});
  CAF_CHECK_EQUAL(ab.sgetc(), int_type{0x0b});
  CAF_CHECK_EQUAL(ab.pubseekoff(-4, std::ios::end, std::ios::in), pos_type{0});
  CAF_CHECK_EQUAL(ab.sgetc(), int_type{0x0a});
  // Absolute positioning.
  CAF_CHECK_EQUAL(ab.pubseekpos(1, std::ios::in), pos_type{1});
  CAF_CHECK_EQUAL(ab.sgetc(), int_type{0x0b});
  CAF_CHECK_EQUAL(ab.pubseekpos(3, std::ios::in), pos_type{3});
  CAF_CHECK_EQUAL(ab.sbumpc(), int_type{0x0d});
  CAF_CHECK_EQUAL(ab.in_avail(), std::streamsize{0});
}
