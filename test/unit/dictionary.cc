#include "framework/unit.h"

#include "vast/string.h"
#include "vast/util/dictionary.h"

using namespace vast;

SUITE("util")

TEST("map dictionary")
{
  util::map_dictionary<string, size_t> dict;
  auto i0 = dict.insert("foo");
  auto i1 = dict.insert("bar");
  auto i2 = dict.insert("baz");
  auto i3 = dict.insert("foo");
  CHECK(i0 != nullptr);
  CHECK(i1 != nullptr);
  CHECK(i2 != nullptr);
  CHECK(i3 == nullptr);
  CHECK(*i0 == 0);
  CHECK(*i1 == 1);
  CHECK(*i2 == 2);

  i0 = dict["foo"];
  i1 = dict["bar"];
  i2 = dict["baz"];
  i3 = dict["qux"];
  CHECK(i0 != nullptr);
  CHECK(i1 != nullptr);
  CHECK(i2 != nullptr);
  CHECK(i3 == nullptr);
  CHECK(*i0 == 0);
  CHECK(*i1 == 1);
  CHECK(*i2 == 2);

  auto s0 = dict[0];
  auto s1 = dict[1];
  auto s2 = dict[2];
  auto s3 = dict[3];
  CHECK(s0 != nullptr);
  CHECK(s1 != nullptr);
  CHECK(s2 != nullptr);
  CHECK(s3 == nullptr);
  CHECK(*s0 == "foo");
  CHECK(*s1 == "bar");
  CHECK(*s2 == "baz");
}
