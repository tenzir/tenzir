//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/convertible/data.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/ip.hpp"
#include "vast/concept/parseable/vast/pattern.hpp"
#include "vast/concept/parseable/vast/subnet.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/data.hpp"
#include "vast/detail/flat_map.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

#include <iterator>

using namespace vast;
using namespace vast::test;

template <class From, class To = From>
struct X {
  constexpr inline static bool use_deep_to_string_formatter = true;

  To value;

  template <class Inspector>
  friend auto inspect(Inspector& fun, X& x) {
    return fun.apply(x.value);
  }

  inline static const record_type& schema() noexcept {
    if constexpr (has_schema<From>) {
      static const auto result = record_type{
        {"value", From::schema()},
      };
      return result;
    } else {
      static const auto result = record_type{
        {"value", type::infer(From{})},
      };
      return result;
    }
  }
};

template <class Type>
auto test_basic = [](auto v) {
  auto val = Type{v};
  auto x = X<Type>{};
  auto r = record{{"value", val}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, val);
};

#define BASIC(type, v)                                                         \
  TEST(basic - type) { /* NOLINT */                                            \
    test_basic<type>(v);                                                       \
  }

BASIC(bool, true)
BASIC(int64_t, 42)
BASIC(uint64_t, 56u)
BASIC(double, 0.42)
BASIC(duration, std::chrono::minutes{55})
BASIC(vast::time, unbox(to<vast::time>("2012-08-12+23:55-0130")))
BASIC(std::string, "test")
BASIC(pattern, unbox(to<pattern>("/pat/")))
BASIC(ip, unbox(to<ip>("44.0.0.1")))
BASIC(subnet, unbox(to<subnet>("44.0.0.1/20")))
#undef BASIC

template <class From, class To>
auto test_narrow = [](auto v) {
  auto x = X<From, To>{};
  auto r = record{{"value", From{v}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, To(v));
};

#define NARROW(from_, to_, v)                                                  \
  TEST(narrow - from_ to to_) { /* NOLINT */                                   \
    test_narrow<from_, to_>(v);                                                \
  }

NARROW(int64_t, int8_t, 42)
NARROW(int64_t, int16_t, 42)
NARROW(int64_t, int32_t, 42)
NARROW(int64_t, int64_t, 42)
NARROW(uint64_t, uint8_t, 56u)
NARROW(uint64_t, uint16_t, 56u)
NARROW(uint64_t, uint32_t, 56u)
NARROW(double, float, 0.42)
#undef NARROW

template <class From, class To>
auto test_oob = [](auto v) {
  auto val = v;
  auto x = X<From, To>{};
  auto r = record{{"value", From{val}}};
  REQUIRE_EQUAL(convert(r, x), ec::convert_error);
};

#define OUT_OF_BOUNDS(from_, to_, v)                                           \
  TEST(oob - from_ to to_ `v`) { /* NOLINT */                                  \
    test_oob<from_, to_>(v);                                                   \
  }

OUT_OF_BOUNDS(int64_t, int8_t, 1 << 7)
OUT_OF_BOUNDS(int64_t, int8_t, -(1 << 7) - 1)
OUT_OF_BOUNDS(int64_t, int16_t, 1 << 15)
OUT_OF_BOUNDS(int64_t, int16_t, -(1 << 15) - 1)
OUT_OF_BOUNDS(int64_t, int32_t, 1ll << 31)
OUT_OF_BOUNDS(int64_t, int32_t, -(1ll << 31) - 1)
OUT_OF_BOUNDS(uint64_t, uint8_t, 1u << 8)
OUT_OF_BOUNDS(uint64_t, uint16_t, 1u << 16)
OUT_OF_BOUNDS(uint64_t, uint32_t, 1ull << 32)
#undef OUT_OF_BOUNDS

TEST(data overload) {
  auto val = int64_t{42};
  auto x = X<int64_t, int>{};
  auto d = data{record{{"value", val}}};
  CHECK_EQUAL(convert(d, x), ec::no_error);
  d = val;
  CHECK_EQUAL(convert(d, x), ec::convert_error);
}

TEST(integer conversion) {
  auto r = record{{"value", int64_t{42}}};
  auto x = X<int64_t>{};
  x.value = 1337;
  r = record{{"foo", int64_t{42}}};
  CHECK_EQUAL(convert(r, x), ec::no_error);
  x.value = 1337;
  r = record{{"value", uint64_t{666}}};
  CHECK_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, 666);
  x.value = 1337;
  r = record{{"value", caf::none}};
  CHECK_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, 1337);
}

struct MultiMember {
  int64_t x;
  bool y;
  duration z;

  template <class Inspector>
  friend auto inspect(Inspector& f, MultiMember& a) {
    return vast::detail::apply_all(f, a.x, a.y, a.z);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"x", int64_type{}},
      {"y", bool_type{}},
      {"z", duration_type{}},
    };
    return result;
  };
};

TEST(multiple members) {
  using namespace std::chrono_literals;
  auto x = MultiMember{};
  auto r = record{{"x", int64_t{42}}, {"y", bool{true}}, {"z", duration{42ns}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.x, 42);
  CHECK_EQUAL(x.y, true);
  CHECK_EQUAL(x.z, 42ns);
}

struct Nest {
  X<int64_t> inner;

  template <class Inspector>
  friend auto inspect(Inspector& f, Nest& b) {
    return f.apply(b.inner);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"inner", X<int64_t>::schema()},
    };
    return result;
  }
};

TEST(nested struct) {
  auto x = Nest{};
  auto r = record{{"inner", record{{"value", int64_t{23}}}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.inner.value, 23);
}

struct Complex {
  std::string a;
  struct b_t {
    int64_t c;
    std::vector<uint64_t> d;

    friend auto inspect(auto& f, b_t& x) {
      return vast::detail::apply_all(f, x.c, x.d);
    }
  } b;
  struct e_t {
    int64_t f;
    std::optional<uint64_t> g;

    friend auto inspect(auto& f, e_t& x) {
      return vast::detail::apply_all(f, x.f, x.g);
    }
  } e;
  bool h;

  friend auto inspect(auto& f, Complex& x) {
    return vast::detail::apply_all(f, x.a, x.b, x.e, x.h);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"a", string_type{}},
      {"b",
       record_type{
         {"c", int64_type{}},
         {"d", list_type{uint64_type{}}},
       }},
      {"e",
       record_type{
         {"f", int64_type{}},
         {"g", uint64_type{}},
       }},
      {"h", bool_type{}},
    };
    return result;
  }
};

TEST(nested struct - single schema) {
  auto x = Complex{};
  auto r = record{{"a", "c3po"},
                  {"b", record{{"c", int64_t{23}}, {"d", list{1u, 2u, 3u}}}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.a, "c3po");
  CHECK_EQUAL(x.b.c, int64_t{23});
  CHECK_EQUAL(x.b.d[0], uint64_t{1u});
  CHECK_EQUAL(x.b.d[1], uint64_t{2u});
  CHECK_EQUAL(x.b.d[2], uint64_t{3u});
}

struct Enum {
  enum { foo, bar, baz } value;

  template <class Inspector>
  friend auto inspect(Inspector& f, Enum& x) {
    return f.apply(x.value);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"value", enumeration_type{{"foo"}, {"bar"}, {"baz"}}},
    };
    return result;
  }
};

TEST(complex - enum) {
  auto x = Enum{};
  auto r = record{{"value", "baz"}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, Enum::baz);
}

TEST(parser - duration) {
  using namespace std::chrono_literals;
  auto x = duration{};
  const auto* r = "10 minutes";
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x, duration{10min});
}

TEST(parser - list<subnet>) {
  auto x = std::vector<subnet>{};
  auto schema = list_type{subnet_type{}};
  auto r = list{"10.0.0.0/8", "172.16.0.0/16"};
  REQUIRE_EQUAL(convert(r, x, schema), ec::no_error);
  auto ref = std::vector{unbox(to<subnet>("10.0.0.0/8")),
                         unbox(to<subnet>("172.16.0.0/16"))};
  CHECK_EQUAL(x, ref);
}

struct EC {
  enum class X { foo, bar, baz };
  X value;

  template <class Inspector>
  friend auto inspect(Inspector& f, EC& x) {
    return f.apply(x.value);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"value", enumeration_type{{{"foo"}, {"bar"}, {"baz"}}}},
    };
    return result;
  }
};

TEST(complex - enum class) {
  auto x = EC{};
  auto r = record{{"value", "baz"}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, EC::X::baz);
}

struct StdOpt {
  std::optional<int64_t> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, StdOpt& c) {
    return f.apply(c.value);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"value", int64_type{}},
    };
    return result;
  }
};

struct CafOpt {
  caf::optional<int64_t> value;

  template <class Inspector>
  friend auto inspect(Inspector& f, CafOpt& c) {
    return f.apply(c.value);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"value", int64_type{}},
    };
    return result;
  }
};

TEST(std::optional member variable) {
  auto x = StdOpt{int64_t{42}};
  auto r = record{{"value", caf::none}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, int64_t{42});
  r = record{{"value", int64_t{22}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(*x.value, 22);
}

TEST(caf::optional member variable) {
  auto x = CafOpt{int64_t{42}};
  auto r = record{{"value", caf::none}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(x.value, int64_t{42});
  r = record{{"value", int64_t{22}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  CHECK_EQUAL(*x.value, 22);
}

struct Derived : X<int64_t> {};

TEST(inherited member variable) {
  auto d = Derived{};
  auto r = record{{"value", int64_t{42}}};
  REQUIRE_EQUAL(convert(r, d), ec::no_error);
  CHECK_EQUAL(d.value, 42);
}

struct Vec {
  std::vector<uint64_t> xs;

  template <class Inspector>
  friend auto inspect(Inspector& f, Vec& e) {
    return f.apply(e.xs);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"xs", list_type{uint64_type{}}},
    };
    return result;
  }
};

TEST(list to vector of unsigned) {
  auto x = Vec{};
  auto r
    = record{{"xs", list{1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u, 9u, 10u, 11u, 12u,
                         1u, 2u, 3u, 4u, 5u, 6u, 7u, 8u, 9u, 10u, 42u, 1337u}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  REQUIRE_EQUAL(x.xs.size(), 24u);
  CHECK_EQUAL(x.xs[1], 2ull);
  CHECK_EQUAL(x.xs[22], 42ull);
  CHECK_EQUAL(x.xs[23], 1337ull);
}

struct VecS {
  std::vector<X<int64_t>> xs;

  template <class Inspector>
  friend auto inspect(Inspector& fun, VecS& f) {
    return fun.apply(f.xs);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"xs", list_type{X<int64_t>::schema()}},
    };
    return result;
  }
};

TEST(list to vector of struct) {
  auto x = VecS{};
  auto r = record{{"xs", list{record{{"value", int64_t{-42}}},
                              record{{"value", int64_t{1337}}}}}};
  REQUIRE_EQUAL(convert(r, x), ec::no_error);
  REQUIRE_EQUAL(x.xs.size(), 2u);
  CHECK_EQUAL(x.xs[0].value, -42);
  CHECK_EQUAL(x.xs[1].value, 1337);
}

struct iList {
  std::vector<uint64_t> value;

  friend iList mappend(iList lhs, iList rhs) {
    lhs.value.insert(lhs.value.end(),
                     std::make_move_iterator(rhs.value.begin()),
                     std::make_move_iterator(rhs.value.end()));
    return lhs;
  }

  template <class Inspector>
  friend auto inspect(Inspector& fun, iList& x) {
    return fun.apply(x.value);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"value", list_type{uint64_type{}}},
    };
    return result;
  }
};

struct OptVec {
  constexpr inline static bool use_deep_to_string_formatter = true;

  caf::optional<std::vector<std::string>> ovs = {};
  caf::optional<uint64_t> ou = 0;

  template <class Inspector>
  friend auto inspect(Inspector& f, OptVec& x) {
    return vast::detail::apply_all(f, x.ovs, x.ou);
  }

  inline static const record_type& schema() noexcept {
    static const auto result = record_type{
      {"ovs", list_type{string_type{}}},
      {"ou", uint64_type{}},
    };
    return result;
  }
};

TEST(conversion to float) {
  float fdest = 0;
  double ddest = 0;
  CHECK_EQUAL(convert(int64_t{42}, fdest, double_type{}), caf::none);
  CHECK_EQUAL(convert(int64_t{42}, ddest, double_type{}), caf::none);
  CHECK_EQUAL(convert(42, fdest, double_type{}), caf::none);
  CHECK_EQUAL(convert(-42, ddest, double_type{}), caf::none);
  CHECK_EQUAL(convert(42u, fdest, double_type{}), caf::none);
  CHECK_EQUAL(convert(42ull, ddest, double_type{}), caf::none);
  CHECK_EQUAL(convert(42.0, fdest, double_type{}), caf::none);
  CHECK_EQUAL(convert(42.0, ddest, double_type{}), caf::none);
}
