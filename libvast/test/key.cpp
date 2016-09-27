#include "vast/concept/parseable/vast/key.hpp"
#include "vast/key.hpp"

#define SUITE parseable
#include "test.hpp"

using namespace vast;

TEST(key) {
  key k;
  CHECK(parsers::key("foo.bar_baz.qux", k));
  CHECK(k == key{"foo", "bar_baz", "qux"});
}
