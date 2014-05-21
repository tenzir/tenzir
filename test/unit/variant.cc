#include "test.h"
#include "vast/util/variant.h"

using namespace vast;

struct stateful
{
  template <typename T>
  void operator()(T&)
  {
    ++state;
  }

  int state = 0;
};

struct doppler
{
  template <typename T>
  void operator()(T& x) const
  {
    x += x;
  }
};

struct binary
{
  template <typename T>
  bool operator()(T const&, T const&) const
  {
    return true;
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const
  {
    return false;
  }
};

struct ternary
{
  template <typename T, typename U>
  double operator()(bool c, T const& t, U const& f) const
  {
    return c ? t : f;
  }

  template <typename T, typename U, typename V>
  double operator()(T const&, U const&, V const&) const
  {
    return 42;
  }
};

BOOST_AUTO_TEST_CASE(variant_test)
{
  using triple = util::variant<int, double, std::string>;

  triple t0{42};
  triple t1{4.2};
  triple t2{"42"};

  // Positional type introspection
  BOOST_CHECK_EQUAL(t0.which(), 0);
  BOOST_CHECK_EQUAL(t1.which(), 1);
  BOOST_CHECK_EQUAL(t2.which(), 2);

  // Access
  BOOST_REQUIRE(util::get<int>(t0));
  BOOST_REQUIRE(util::get<double>(t1));
  BOOST_REQUIRE(util::get<std::string>(t2));
  BOOST_CHECK_EQUAL(*util::get<int>(t0), 42);
  BOOST_CHECK_EQUAL(*util::get<double>(t1), 4.2);
  BOOST_CHECK_EQUAL(*util::get<std::string>(t2), "42");

  // Assignment
  *util::get<int>(t0) = 1337;
  *util::get<double>(t1) = 1.337;
  std::string leet{"1337"};
  *util::get<std::string>(t2) = std::move(leet);
  BOOST_CHECK_EQUAL(*util::get<int>(t0), 1337);
  BOOST_CHECK_EQUAL(*util::get<double>(t1), 1.337);
  BOOST_CHECK_EQUAL(*util::get<std::string>(t2), "1337");

  // Unary visitation
  stateful v;
  apply_visitor(v, t1);           // lvalue
  apply_visitor(stateful{}, t1);  // rvalue
  apply_visitor(doppler{}, t1);
  BOOST_CHECK_EQUAL(*util::get<double>(t1), 1.337 * 2);

  // Binary visitation.
  BOOST_CHECK(! apply_visitor(binary{}, t0, t1));
  BOOST_CHECK(! apply_visitor(binary{}, t1, t0));
  BOOST_CHECK(! apply_visitor(binary{}, t0, t2));
  BOOST_CHECK(apply_visitor(binary{}, t0, triple{84}));

  // Ternary visitation.
  using trio = util::variant<bool, double, int>;
  BOOST_CHECK(apply_visitor(ternary{}, trio{true}, trio{4.2}, trio{42}) == 4.2);
  BOOST_CHECK(apply_visitor(ternary{}, trio{false}, trio{4.2}, trio{1337}) == 1337.0);

  // Generic lambda visitation.
  using pair = util::variant<double, int>;
  auto fourty_two = pair{42};
  auto r = apply_visitor([](auto x) -> int { return x + 42; }, fourty_two);
  BOOST_CHECK(r == 42 + 42);
}

BOOST_AUTO_TEST_CASE(delayed_visitation)
{
  std::vector<util::variant<double, int>> doubles;

  doubles.emplace_back(1337);
  doubles.emplace_back(4.2);
  doubles.emplace_back(42);

  stateful s;
  std::for_each(doubles.begin(), doubles.end(), util::apply_visitor(s));
  BOOST_CHECK(s.state == 3);

  std::for_each(doubles.begin(), doubles.end(), util::apply_visitor(doppler{}));
  BOOST_CHECK(*util::get<int>(doubles[2]) == 84);
}

BOOST_AUTO_TEST_CASE(factory_construction)
{
  using pair = util::variant<double, int>;

  BOOST_CHECK(util::get<double>(pair::make(0)));
  BOOST_CHECK(util::get<int>(pair::make(1)));
}
