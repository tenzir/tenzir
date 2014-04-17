#include "test.h"
#include "vast/util/variant.h"

using namespace vast;

struct stateful
{
  using result_type = void;

  template <typename T>
  void operator()(T&)
  {
    ++state;
  }

  int state = 0;
};

struct doppler
{
  using result_type = void;

  template <typename T>
  void operator()(T& x) const
  {
    x += x;
  }
};

struct binary
{
  using result_type = bool;

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

  // Visitation
  apply_visitor(stateful{}, t1);
  apply_visitor(doppler{}, t1);
  BOOST_CHECK_EQUAL(*util::get<double>(t1), 1.337 * 2);

  BOOST_CHECK(! apply_visitor_binary(binary{}, t0, t1));
  BOOST_CHECK(! apply_visitor_binary(binary{}, t1, t0));
  BOOST_CHECK(! apply_visitor_binary(binary{}, t0, t2));
  BOOST_CHECK(apply_visitor_binary(binary{}, t0, triple{84}));
}
