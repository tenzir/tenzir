#include "framework/unit.h"
#include "vast/print.h"

SUITE("print")

using namespace vast;

namespace n {

struct foo
{
  int i = 42;
};

template <typename Iterator>
trial<void> print(foo const& x, Iterator&& out)
{
  using vast::print;
  return print(x.i, out);
}

} // namespace n

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

TEST("floating point")
{
  std::string str;
  auto out = std::back_inserter(str);

  double d = 0.0;
  print(d, out);
  CHECK(str == "0.0000000000");

  str.clear();
  d = 1.0;
  print(d, out);
  CHECK(str == "1.0000000000");

  str.clear();
  d = 0.005;
  print(d, out);
  CHECK(str == "0.0050000000");

  str.clear();
  d = 123.456;
  print(d, out);
  CHECK(str == "123.4560000000");

  str.clear();
  d = -123.456;
  print(d, out);
  CHECK(str == "-123.4560000000");

  str.clear();
  d = 123456.1234567890123;
  print(d, out);
  CHECK(str == "123456.1234567890");

  str.clear();
  d = 123456.1234567890123;
  print(d, out, 6);
  CHECK(str == "123456.123457");

  str.clear();
  d = 123456.8888;
  print(d, out, 0);
  CHECK(str == "123457");

  str.clear();
  d = 123456.1234567890123;
  print(d, out, 1);
  CHECK(str == "123456.1");

  str.clear();
  d = 123456.00123;
  print(d, out, 6);
  CHECK(str == "123456.001230");
}

TEST("custom")
{
  std::string str;
  auto out = std::back_inserter(str);

  n::foo x;
  print(x, out);

  CHECK(str == "+42");
}

TEST("container")
{
  std::string str;
  auto out = std::back_inserter(str);

  std::vector<int> v = {1, 2, 3};
  print(v, out);
  CHECK(str == "+1, +2, +3");

  str.clear();
  std::vector<unsigned> u = {1, 2, 3};
  print(u, out);
  CHECK(str == "1, 2, 3");

  str.clear();
  std::vector<n::foo> f = {n::foo{}, n::foo{}, n::foo{}};
  print(f, out);
  CHECK(str == "+42, +42, +42");
}
