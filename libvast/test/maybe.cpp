#include "vast/load.hpp"
#include "vast/maybe.hpp"
#include "vast/save.hpp"

#define SUITE maybe
#include "test.hpp"

using namespace vast;

namespace {

struct qwertz {
  qwertz(int i, int j) : i_(i), j_(j) {
    // nop
  }

  int i_;
  int j_;
};

bool operator==(const qwertz& lhs, const qwertz& rhs) {
  return lhs.i_ == rhs.i_ && lhs.j_ == rhs.j_;
}

enum class test_errc : uint8_t {
  first_error = 1,
  second_error
};

error make_error(test_errc x) {
  return {static_cast<uint8_t>(x), caf::atom("test")};
}

} // namespace <anonymous>

TEST(empty) {
  maybe<int> i;
  maybe<int> j;
  CHECK(i == j);
  CHECK(!(i != j));
}

TEST(empty - distinct_types) {
  maybe<int> i;
  maybe<double> j;
  CHECK(i == j);
  CHECK(!(i != j));
}

TEST(unequal) {
  maybe<int> i = 5;
  maybe<int> j = 6;
  CHECK(!(i == j));
  CHECK(i != j);
}

TEST(custom type - none) {
  maybe<qwertz> i;
  CHECK(i == nil);
}

TEST(custom type - valid) {
  qwertz obj{1, 2};
  maybe<qwertz> j = obj;
  CHECK(j != nil);
  CHECK(obj == j);
  CHECK(j == obj );
  CHECK(obj == *j);
  CHECK(*j == obj);
}

TEST(error cases) {
  auto f = []() -> maybe<int> { return test_errc::second_error; };
  auto val = f();
  REQUIRE(! val);
  CHECK(val.error() == test_errc::second_error);
  val = 42;
  REQUIRE(val);
  CHECK_EQUAL(*val, 42);
  val = test_errc::first_error;
  REQUIRE(! val);
  CHECK(val.error() == test_errc::first_error);
}

TEST(void specialization) {
  // Default-constructed instances represent no failure.
  maybe<void> m;
  CHECK(m);
  CHECK(m.valid());
  CHECK(m.empty());
  CHECK(! m.error());
  // Assign erroneous state.
  m = test_errc::second_error;
  CHECK(! m);
  CHECK(! m.valid());
  CHECK(! m.empty());
  CHECK(m.error());
  CHECK(m.error() == test_errc::second_error);
  // Implicit construction.
  auto f = []() -> maybe<void> { return test_errc::second_error; };
  auto val = f();
  REQUIRE(! val);
  CHECK(val.error() == test_errc::second_error);
}

TEST(serialization) {
  maybe<int> m1, m2;
  m1 = 42;
  std::vector<char> buf;
  save(buf, m1);
  load(buf, m2);
  REQUIRE(m1);
  REQUIRE(m2);
  CHECK(*m2 == 42);
  CHECK(*m1 == *m2);
}
