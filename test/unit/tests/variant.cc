#include "framework/unit.h"

#include "vast/concept/serializable/util/variant.h"
#include "vast/io/serialization.h"
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

  CHECK(get<double>(pair::make(0)));
  CHECK(get<int>(pair::make(1)));
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

  CHECK(! (p1 < p3 || p1 > p3));
  CHECK(p1 < p2);
  CHECK(p2 > p1);
  CHECK(p0 < p2);

  // The total ordering works component-wise: for the pair variant, all double
  // types are less-than int types.
  CHECK(p1 < p0);
  CHECK(p1 < p2);
  CHECK(p3 < p2);
}

TEST("positional introspection")
{
  CHECK(t0.which() == 0);
  CHECK(t1.which() == 1);
  CHECK(t2.which() == 2);
}

TEST("type-based access")
{
  REQUIRE(is<int>(t0));
  CHECK(*get<int>(t0) == 42);

  REQUIRE(is<double>(t1));
  CHECK(*get<double>(t1) == 4.2);

  REQUIRE(is<std::string>(t2));
  CHECK(*get<std::string>(t2) == "42");
}

TEST("assignment")
{
  *get<int>(t0) = 1337;
  *get<double>(t1) = 1.337;
  std::string leet{"1337"};
  *get<std::string>(t2) = std::move(leet);
  CHECK(*get<int>(t0) == 1337);
  CHECK(*get<double>(t1) == 1.337);
  CHECK(*get<std::string>(t2) == "1337");
}

TEST("unary visitation")
{
  stateful v;
  visit(v, t1);           // lvalue
  visit(stateful{}, t1);  // rvalue
  visit(doppler{}, t1);
  CHECK(*get<double>(t1) == 1.337 * 2);
}

TEST("binary visitation")
{
  CHECK(! visit(binary{}, t0, t1));
  CHECK(! visit(binary{}, t1, t0));
  CHECK(! visit(binary{}, t0, t2));
  CHECK(visit(binary{}, t0, triple{84}));
}

TEST("ternary visitation")
{
  using trio = util::variant<bool, double, int>;
  CHECK(visit(ternary{}, trio{true}, trio{4.2}, trio{42}) == 4.2);
  CHECK(visit(ternary{}, trio{false}, trio{4.2}, trio{1337}) == 1337.0);
}

TEST("generic lambda visitation")
{
  using pair = util::variant<double, int>;
  auto fourty_two = pair{42};
  auto r = visit([](auto x) -> int { return x + 42; }, fourty_two);
  CHECK(r == 84);
}

TEST("delayed visitation")
{
  std::vector<util::variant<double, int>> doubles;

  doubles.emplace_back(1337);
  doubles.emplace_back(4.2);
  doubles.emplace_back(42);

  stateful s;
  std::for_each(doubles.begin(), doubles.end(), visit(s));
  CHECK(s.state == 3);

  std::for_each(doubles.begin(), doubles.end(), visit(doppler{}));
  CHECK(*get<int>(doubles[2]) == 84);
}

namespace {

struct reference_returner
{
  template <typename T>
  double const& operator()(T const&) const
  {
    static constexpr double nada = 0.0;
    return nada;
  }

  double const& operator()(double const& d) const
  {
    return d;
  }
};

} // namespace <anonymous>

TEST("visitor with reference as return value")
{
  util::variant<double, int> v = 4.2;
  reference_returner r;
  CHECK(std::is_same<decltype(r(42)), double const&>::value);
  CHECK(! std::is_same<decltype(r(42)), double>::value);
  FAIL(std::is_same<decltype(visit(r, v)), double const&>::value);
  FAIL(! std::is_same<decltype(visit(r, v)), double>::value);
}

namespace {

// Discriminator unions must begin at 0 and increment sequentially.
enum class hell : int
{
  devil = 0,
  diablo = 1
};

} // namespace <anonymous>


TEST("variant custom tag")
{
  using custom_variant = util::basic_variant<hell, int, std::string>;
  custom_variant v(42);
  CHECK(v.which() == hell::devil);
}

TEST("variant serialization")
{
  std::vector<uint8_t> buf;
  CHECK(io::archive(buf, util::variant<bool, int>{42}));

  util::variant<bool, int> v;
  CHECK(io::unarchive(buf, v));
  REQUIRE(is<int>(v));
  CHECK(*get<int>(v) == 42);
}

namespace {

// A type containing a variant and modeling the Variant concept.
class concept
{
public:
  concept() = default;

  template <typename T>
  concept(T&& x)
    : value_(std::forward<T>(x))
  {
  }

  using value = util::variant<int, bool>;

protected:
  value value_;

  friend value const& expose(concept const& w)
  {
    return w.value_;
  }
};

} // namespace <anonymous>

TEST("variant concept")
{
  concept c;

  CHECK(which(c) == 0);
  REQUIRE(is<int>(c));
  CHECK(*get<int>(c) == 0);

  auto r = visit([](auto x) -> bool { return !! x; }, c);
  CHECK(! r);
}
