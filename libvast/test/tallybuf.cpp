#include "vast/detail/tallybuf.hpp"

#define SUITE streambuf
#include "test.hpp"

using namespace std::string_literals;
using namespace vast;
using namespace vast::detail;

TEST(tallying streambuffer) {
  char buf[128];
  std::stringbuf ss{"foobarbaz"};
  tallybuf<std::stringbuf> sb{ss};
  MESSAGE("get area");
  sb.sgetn(buf, 2);
  sb.sgetn(buf, 4);
  CHECK_EQUAL(sb.got(), 2u + 4u);
  MESSAGE("put area");
  sb.sputn(buf, 3);
  sb.sputn(buf, 2);
  CHECK_EQUAL(sb.put(), 3u + 2u);
}
