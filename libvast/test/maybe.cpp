#define SUITE maybe
#include "test.hpp"

#include "vast/maybe.hpp"

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

CAF_TEST(empty) {
  maybe<int> i;
  maybe<int> j;
  CHECK(i == j);
  CHECK(!(i != j));
}

CAF_TEST(empty - distinct_types) {
  maybe<int> i;
  maybe<double> j;
  CHECK(i == j);
  CHECK(!(i != j));
}

CAF_TEST(unequal) {
  maybe<int> i = 5;
  maybe<int> j = 6;
  CHECK(!(i == j));
  CHECK(i != j);
}

CAF_TEST(custom type - none) {
  maybe<qwertz> i;
  CHECK(i == nil);
}

CAF_TEST(custom type - valid) {
  qwertz obj{1, 2};
  maybe<qwertz> j = obj;
  CHECK(j != nil);
  CHECK(obj == j);
  CHECK(j == obj );
  CHECK(obj == *j);
  CHECK(*j == obj);
}

CAF_TEST(error cases) {
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

CAF_TEST(void specialization) {
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
  std::cout << to_string(val.error()) << std::endl;
  CHECK(val.error() == test_errc::second_error);
}

