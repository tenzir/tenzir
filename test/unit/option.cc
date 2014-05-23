#include "framework/unit.h"

#include "vast/optional.h"
#include "vast/io/serialization.h"

SUITE("core")

using namespace vast;

TEST("option serialization")
{
  optional<std::string> o1 = std::string{"foo"};
  decltype(o1) o2;
  std::vector<uint8_t> buf;
  io::archive(buf, o1);
  io::unarchive(buf, o2);
  REQUIRE(o1);
  REQUIRE(o2);
  CHECK(*o2 == "foo");
  CHECK(*o1 == *o2);
}

TEST("std::vector<option> serialization")
{
  std::vector<optional<int>> v1, v2;
  v1.emplace_back(42);
  v1.emplace_back(84);
  std::vector<uint8_t> buf;
  io::archive(buf, v1);
  io::unarchive(buf, v2);
  REQUIRE(v2.size() == 2);
  CHECK(*v2[0] == 42);
  CHECK(*v2[1] == 84);
}
