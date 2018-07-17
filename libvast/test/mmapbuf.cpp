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

#include <fstream>
#include <string>

#include "vast/detail/mmapbuf.hpp"
#include "vast/detail/system.hpp"

#define SUITE streambuf
#include "test.hpp"
#include "fixtures/filesystem.hpp"

using namespace std::string_literals;
using namespace vast;

FIXTURE_SCOPE(fixture_tests, fixtures::filesystem)

TEST(memory-mapped streambuffer) {
  MESSAGE("create a dummy file");
  auto filename = directory / "dummy.txt";
  std::ofstream ofs{filename.str()};
  auto data = "foobarbazqux"s;
  ofs << data;
  ofs.close();
  MESSAGE("performing streambuffer tests");
  detail::mmapbuf sb{filename.str()};
  CHECK_EQUAL(sb.size(), data.size());
  CHECK_EQUAL(sb.in_avail(), static_cast<std::streamsize>(sb.size()));
  std::string buf;
  buf.resize(3);
  auto n = sb.sgetn(&buf[0], 3);
  CHECK_EQUAL(n, 3);
  CHECK_EQUAL(buf, "foo");
  CHECK_EQUAL(sb.in_avail(), 9);
  buf.resize(data.size());
  n = sb.sgetn(&buf[3], 100);
  CHECK_EQUAL(n, 9);
  CHECK_EQUAL(buf, data);
  CHECK_EQUAL(sb.in_avail(), 0);
  // Seek back to beginning.
  sb.pubseekpos(0, std::ios::in);
  CHECK_EQUAL(sb.in_avail(), static_cast<std::streamsize>(sb.size()));
  data = "corge ";
  n = sb.sputn(data.data(), data.size());
  CHECK_EQUAL(n, static_cast<std::streamsize>(data.size()));
  CHECK_EQUAL(std::string(sb.data(), sb.size()), "corge bazqux");
  CHECK(sb.resize(data.size() - 1));
  // Figure out current position.
  size_t cur = sb.pubseekoff(0, std::ios::cur, std::ios::out);
  CHECK_EQUAL(cur, sb.size()); // we're at the end!
}

namespace {

void aligned_resize_test_impl(const vast::path& filename, size_t size) {
  detail::mmapbuf sb{filename.str(), size};
  REQUIRE(sb.data() != nullptr);
  CHECK_EQUAL(sb.size(), size);
  sb.sputn("Here be content", 15);
  // Aligned resizing.
  REQUIRE(sb.resize(size * 2));
  CHECK_EQUAL(sb.size(), size * 2);
  CHECK_EQUAL(std::string(sb.data() + 3, 9), "e be cont");
  // Seek in the middle and perform a random write.
  sb.pubseekpos(size, std::ios::out);
  sb.sputc('x');
  // Unaligned resizing.
  REQUIRE(sb.resize(size / 2));
  CHECK_EQUAL(sb.size(), size / 2);
  REQUIRE(sb.resize(sb.size() * 8));
  CHECK_EQUAL(sb.size(), size * 4);
  CHECK_EQUAL(std::string(sb.data() + 3, 9), "e be cont");
  sb.pubseekpos(size * 3, std::ios::out);
  sb.sputc('x');
}

} // namespace <anonymous>

TEST(memory-mapped streambuffer aligned resize) {
  auto filename = directory / "aligned";
  aligned_resize_test_impl(filename, detail::page_size());
}

TEST(memory-mapped streambuffer aligned resize large) {
  auto filename = directory / "aligned_large";
  auto page_size = detail::page_size();
  auto hundred_mb = size_t{100 * (1 << 20)};
  auto size = hundred_mb - (hundred_mb % page_size);
  aligned_resize_test_impl(filename, size);
}

FIXTURE_SCOPE_END()
