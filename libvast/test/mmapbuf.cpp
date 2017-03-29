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
  data = "corge ";
  n = sb.sputn(data.data(), data.size());
  CHECK_EQUAL(n, static_cast<std::streamsize>(data.size()));
  CHECK_EQUAL(std::string(sb.data(), sb.size()), "corge bazqux");
  CHECK(sb.truncate(data.size() - 1));
  // Figure out current position.
  size_t cur = sb.pubseekoff(0, std::ios::cur, std::ios::out);
  CHECK_EQUAL(cur, sb.size()); // we're at the end!
}

FIXTURE_SCOPE_END()
