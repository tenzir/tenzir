#include <fstream>
#include <string>

#include "vast/detail/mmapbuf.hpp"

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
  sb.sbumpc();
  sb.sbumpc();
  CHECK_EQUAL(sb.in_avail(), 10);
}

FIXTURE_SCOPE_END()
