#include "framework/unit.h"

#include "vast/util/variant.h"

SUITE("variant")

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

using triple = util::variant<int, double, std::string>;

namespace {

triple t0{42};
triple t1{4.2};
triple t2{"42"};

} // namespace <anonymous>

TEST("factory construction")
{
  using pair = util::variant<double, int>;

  CHECK(util::get<double>(pair::make(0)));
  CHECK(util::get<int>(pair::make(1)));
}

TEST("operator==")
{
  using pair = util::variant<double, int>;

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
}

TEST("positional introspection")
{
  CHECK(t0.which() == 0);
  CHECK(t1.which() == 1);
  CHECK(t2.which() == 2);
}

TEST("type-based access")
{
  REQUIRE(util::is<int>(t0));
  CHECK(*util::get<int>(t0) == 42);

  REQUIRE(util::is<double>(t1));
  CHECK(*util::get<double>(t1) == 4.2);

  REQUIRE(util::is<std::string>(t2));
  CHECK(*util::get<std::string>(t2) == "42");
}

TEST("assignment")
{
  *util::get<int>(t0) = 1337;
  *util::get<double>(t1) = 1.337;
  std::string leet{"1337"};
  *util::get<std::string>(t2) = std::move(leet);
  CHECK(*util::get<int>(t0) == 1337);
  CHECK(*util::get<double>(t1) == 1.337);
  CHECK(*util::get<std::string>(t2) == "1337");
}

TEST("unary visitation")
{
  stateful v;
  apply_visitor(v, t1);           // lvalue
  apply_visitor(stateful{}, t1);  // rvalue
  apply_visitor(doppler{}, t1);
  CHECK(*util::get<double>(t1) == 1.337 * 2);
}

TEST("binary visitation")
{
  CHECK(! apply_visitor(binary{}, t0, t1));
  CHECK(! apply_visitor(binary{}, t1, t0));
  CHECK(! apply_visitor(binary{}, t0, t2));
  CHECK(apply_visitor(binary{}, t0, triple{84}));
}

TEST("ternary visitation")
{
  using trio = util::variant<bool, double, int>;
  CHECK(apply_visitor(ternary{}, trio{true}, trio{4.2}, trio{42}) == 4.2);
  CHECK(apply_visitor(ternary{}, trio{false}, trio{4.2}, trio{1337}) == 1337.0);
}

TEST("generic lambda visitation")
{
  using pair = util::variant<double, int>;
  auto fourty_two = pair{42};
  auto r = apply_visitor([](auto x) -> int { return x + 42; }, fourty_two);
  CHECK(r == 84);
}

TEST("delayed visitation")
{
  std::vector<util::variant<double, int>> doubles;

  doubles.emplace_back(1337);
  doubles.emplace_back(4.2);
  doubles.emplace_back(42);

  stateful s;
  std::for_each(doubles.begin(), doubles.end(), util::apply_visitor(s));
  CHECK(s.state == 3);

  std::for_each(doubles.begin(), doubles.end(), util::apply_visitor(doppler{}));
  CHECK(*util::get<int>(doubles[2]) == 84);
}

namespace {

// A type containing a variant and modeling the Variant concept.
class plain
{
public:
  plain() = default;

  template <typename T>
  plain(T&& x)
    : value_(std::forward<T>(x))
  {
  }

  using value = util::variant<int, bool>;

protected:
  value value_;

  friend value const& expose(plain const& w)
  {
    return w.value_;
  }
};

struct typed : plain
{
  enum class type : value::tag_type
  {
    foo,
    bar
  };
};

} // namespace <anonymous>

TEST("variant concept")
{
  plain p;
  typed t;

  CHECK(std::is_same<decltype(util::which(p)), plain::value::tag_type>::value);
  CHECK(std::is_same<decltype(util::which(t)), typed::type>::value);

  CHECK(util::which(p) == 0);
  CHECK(util::which(t) == typed::type::foo);

  REQUIRE(util::is<int>(p));
  CHECK(*util::get<int>(p) == 0);

  auto r = util::visit([](auto x) -> bool { return !! x; }, p);
  CHECK(! r);
}
