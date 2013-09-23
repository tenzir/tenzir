#include "test.h"
#include "vast/util/print.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(print_integral)
{
  std::string str;
  auto out = std::back_inserter(str);
  uint8_t u8 = 1;
  render(out, u8);
  uint16_t u16 = 2;
  render(out, u16);
  uint32_t u32 = 3;
  render(out, u32);
  uint64_t u64 = 4;
  render(out, u64);
  // FIXME: why does this cause a compile error?
  // --> call to 'print' is ambiguous
  //size_t s = 5;
  //render(out, s);
  BOOST_CHECK_EQUAL(str, "1234");
}
