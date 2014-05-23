#include "framework/unit.h"
#include "vast/print.h"

SUITE("print")

using namespace vast;

TEST("integral")
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

  CHECK(str == "12345");
}
