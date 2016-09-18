#include "vast/load.hpp"
#include "vast/save.hpp"

#include "vast/detail/variant.hpp"

#define SUITE variant
#include "test.hpp"

using namespace vast;
using namespace vast::detail;

struct stateful {
  template <typename T>
  void operator()(T&) {
    ++state;
  }

  int state = 0;
};

struct doppler {
  template <typename T>
  void operator()(T& x) const {
    x += x;
  }
};

struct binary {
  template <typename T>
  bool operator()(T const&, T const&) const {
    return true;
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const {
    return false;
  }
};

struct ternary {
  template <typename T, typename U>
  double operator()(bool c, T const& t, U const& f) const {
    return c ? t : f;
  }

  template <typename T, typename U, typename V>
  double operator()(T const&, U const&, V const&) const {
    return 42;
  }
};

using triple = variant<int, double, std::string>;

namespace {

struct fixture {
  triple t0{42};
  triple t1{4.2};
  triple t2{"42"};
};

} // namespace <anonymous>

FIXTURE_SCOPE(variant_tests, fixture)

TEST(operator==) {
  using pair = variant<double, int>;

  pair p0{42};
  pair p1{42.0};
  pair p2{1337};
  pair p3{4.2};

  CHECK(p0 != p1);
  CHECK(p0 != p2);
  CHECK(p0 != p3);
  CHECK(p1 != p3);

  p1 = 4.2;
  CHECK(p1 == p3);

  CHECK(!(p1 < p3 || p1 > p3));
  CHECK(p1 < p2);
  CHECK(p2 > p1);
  CHECK(p0 < p2);

  // The total ordering works component-wise: for the pair variant, all double
  // types are less-than int types.
  CHECK(p1 < p0);
  CHECK(p1 < p2);
  CHECK(p3 < p2);
}

TEST(positional introspection) {
  CHECK(t0.index() == 0);
  CHECK(t1.index() == 1);
  CHECK(t2.index() == 2);
}

TEST(type - based access) {
  REQUIRE(get_if<int>(t0));
  CHECK(get<int>(t0) == 42);

  REQUIRE(get_if<double>(t1));
  CHECK(get<double>(t1) == 4.2);

  REQUIRE(get_if<std::string>(t2));
  CHECK(get<std::string>(t2) == "42");
}

TEST(assignment) {
  get<int>(t0) = 1337;
  get<double>(t1) = 1.337;
  std::string leet{"1337"};
  get<std::string>(t2) = std::move(leet);
  CHECK(get<int>(t0) == 1337);
  CHECK(get<double>(t1) == 1.337);
  CHECK(get<std::string>(t2) == "1337");
}

TEST(unary visitation) {
  stateful v;
  visit(v, t1);          // lvalue
  visit(stateful{}, t1); // rvalue
  visit(doppler{}, t1);
  CHECK_EQUAL(get<double>(t1), 8.4);
}

TEST(binary visitation) {
  CHECK(!visit(binary{}, t0, t1));
  CHECK(!visit(binary{}, t1, t0));
  CHECK(!visit(binary{}, t0, t2));
  CHECK(visit(binary{}, t0, triple{84}));
}

TEST(ternary visitation) {
  using trio = variant<bool, double, int>;
  CHECK(visit(ternary{}, trio{true}, trio{4.2}, trio{42}) == 4.2);
  CHECK(visit(ternary{}, trio{false}, trio{4.2}, trio{1337}) == 1337.0);
}

TEST(generic lambda visitation) {
  using pair = variant<double, int>;
  auto fourty_two = pair{42};
  auto r = visit([](auto x) -> int { return x + 42; }, fourty_two);
  CHECK_EQUAL(r, 84);
}

TEST(delayed visitation) {
  std::vector<variant<double, int>> doubles;
  doubles.emplace_back(1337);
  doubles.emplace_back(4.2);
  doubles.emplace_back(42);
  stateful s;
  std::for_each(doubles.begin(), doubles.end(), visit(s));
  CHECK_EQUAL(s.state, 3);
  std::for_each(doubles.begin(), doubles.end(), visit(doppler{}));
  CHECK_EQUAL(get<int>(doubles[2]), 84);
}

TEST(variant serialization) {
  std::vector<char> buf;
  variant<bool, int> v, u;
  v = 42;
  save(buf, v);
  load(buf, u);
  REQUIRE(get_if<int>(u));
  CHECK_EQUAL(get<int>(u), 42);
}

FIXTURE_SCOPE_END()
