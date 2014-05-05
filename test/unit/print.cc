#include "test.h"
#include "vast/print.h"

using namespace vast;

BOOST_AUTO_TEST_CASE(print_integral)
{
  std::string str;
  auto out = std::back_inserter(str);
  uint8_t u8 = 1;
  print(u8, out);
  uint16_t u16 = 2;
  print(u16, out);
  uint32_t u32 = 3;
  print(u32, out);
  uint64_t u64 = 4;
  print(u64, out);
  size_t s = 5;
  print(s, out);
  BOOST_CHECK_EQUAL(str, "12345");
}
