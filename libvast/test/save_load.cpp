#include "vast/concept/serializable/state.hpp"
#include "vast/save.hpp"
#include "vast/load.hpp"

#define SUITE serialization
#include "test.hpp"

using namespace std::string_literals;
using namespace vast;

namespace {

// A type that models the Serializable concept.
struct foo {
  int i = 0;
};

template <class Processor>
void serialize(Processor& proc, foo& x) {
  proc & x.i;
}

// A type that models the State concept.
class bar {
  friend vast::access;

public:
  void set(int i) {
    f_.i = i;
  }

  int get() const {
    return f_.i;
  }

private:
  foo f_;
};

} // namespace <anonymous>

namespace vast {

template <>
struct access::state<bar> {
  template <class T, class F>
  static void call(T&& x, F f) {
    f(x.f_);
  }
};

} // namespace vast

TEST(variadic) {
  std::string buf;
  auto m = save<compression::lz4>(buf, 42, 4.2, 1337u, "foo"s);
  CHECK(!m.error());
  int i;
  double d;
  unsigned u;
  std::string s;
  m = load<compression::lz4>(buf, i, d, u, s);
  CHECK(!m.error());
  CHECK_EQUAL(i, 42);
  CHECK_EQUAL(d, 4.2);
  CHECK_EQUAL(u, 1337u);
  CHECK_EQUAL(s, "foo");
}

TEST(custom type modeling serializable) {
  std::vector<char> buf;
  foo x;
  x.i = 42;
  auto m = save(buf, x);
  foo y;
  m = load(buf, y);
  CHECK_EQUAL(x.i, y.i);
}

TEST(custom type modeling state) {
  std::vector<char> buf;
  bar x;
  x.set(42);
  auto m = save(buf, x);
  bar y;
  m = load(buf, y);
  CHECK_EQUAL(x.get(), y.get());
}
