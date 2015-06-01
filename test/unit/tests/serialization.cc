#include "vast/announce.h"
#include "vast/concept/serializable/hierarchy.h"
#include "vast/concept/serializable/state.h"
#include "vast/concept/serializable/vector_event.h"
#include "vast/concept/serializable/std/list.h"
#include "vast/concept/serializable/std/unordered_map.h"
#include "vast/concept/serializable/std/vector.h"
#include "vast/concept/serializable/util/optional.h"
#include "vast/concept/serializable/io.h"
#include "vast/util/byte_swap.h"
#include "vast/util/optional.h"

#define SUITE serialization
#include "test.h"
#include "fixtures/events.h"

using namespace vast;
using namespace vast::util;

TEST(byte swapping)
{
  uint8_t  x08 = 0x11;
  uint16_t x16 = 0x1122;
  uint32_t x32 = 0x11223344;
  uint64_t x64 = 0x1122334455667788;
  // TODO: extend to all arithmetic types, e.g., float and double.

  auto y08 = byte_swap<little_endian, big_endian>(x08);
  auto y16 = byte_swap<little_endian, big_endian>(x16);
  auto y32 = byte_swap<little_endian, big_endian>(x32);
  auto y64 = byte_swap<little_endian, big_endian>(x64);
  CHECK(y08 == 0x11);
  CHECK(y16 == 0x2211);
  CHECK(y32 == 0x44332211);
  CHECK(y64 == 0x8877665544332211);

  y08 = byte_swap<big_endian, little_endian>(y08);
  y16 = byte_swap<big_endian, little_endian>(y16);
  y32 = byte_swap<big_endian, little_endian>(y32);
  y64 = byte_swap<big_endian, little_endian>(y64);
  CHECK(y08 == x08);
  CHECK(y16 == x16);
  CHECK(y32 == x32);
  CHECK(y64 == x64);

  // NOP
  y08 = byte_swap<big_endian, big_endian>(y08);
  y16 = byte_swap<big_endian, big_endian>(y16);
  y32 = byte_swap<big_endian, big_endian>(y32);
  y64 = byte_swap<big_endian, big_endian>(y64);
  CHECK(y08 == x08);
  CHECK(y16 == x16);
  CHECK(y32 == x32);
  CHECK(y64 == x64);

  // NOP
  y08 = byte_swap<little_endian, little_endian>(y08);
  y16 = byte_swap<little_endian, little_endian>(y16);
  y32 = byte_swap<little_endian, little_endian>(y32);
  y64 = byte_swap<little_endian, little_endian>(y64);
  CHECK(y08 == x08);
  CHECK(y16 == x16);
  CHECK(y32 == x32);
  CHECK(y64 == x64);
}

TEST(containers)
{
  std::vector<double> v0{4.2, 8.4, 16.8}, v1;
  std::list<int> l0{4, 2}, l1;
  std::unordered_map<int, int> u0{{4, 2}, {8, 4}}, u1;

  std::vector<uint8_t> buf;
  save(buf, v0, l0, u0);
  load(buf, v1, l1, u1);

  CHECK(v0 == v1);
  CHECK(l0 == l1);
  CHECK(u0 == u1);
}

TEST(optional<T>)
{
  util::optional<std::string> o1 = std::string{"foo"};
  decltype(o1) o2;
  std::vector<uint8_t> buf;
  save(buf, o1);
  load(buf, o2);
  REQUIRE(o1);
  REQUIRE(o2);
  CHECK(*o2 == "foo");
  CHECK(*o1 == *o2);
}

// A serializable class.
class serializable
{
  friend vast::access;

public:
  int i() const
  {
    return i_;
  }

  void i(int x)
  {
    i_ = x;
  }

private:
  int i_ = 0;
};

namespace vast {

// By specializing the state concept, the class becomes automatically
// serializable.
template <>
struct access::state<serializable>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.i_);
  }
};

} // namespace vast

TEST(compress/decompress)
{
  std::vector<io::compression> methods{io::null, io::lz4};
#ifdef VAST_HAVE_SNAPPY
  methods.push_back(io::snappy);
#endif // VAST_HAVE_SNAPPY
  for (auto method : methods)
  {
    // Generate some data.
    std::vector<int> input(1u << 10);
    REQUIRE((input.size() % 2) == 0);
    for (size_t i = 0; i < input.size() / 2; ++i)
      input[i] = i % 128;
    for (size_t i = input.size() / 2; i < input.size(); ++i)
      input[i] = i % 2;
    // Serialize & compress.
    serializable x;
    x.i(42);
    std::string buf;
    auto t = compress(buf, method, input, x);
    CHECK(t);
    serializable y;
    decltype(input) output;
    t = decompress(buf, method, output, y);
    CHECK(t);
    CHECK(input.size() == output.size());
    CHECK(input == output);
    CHECK(y.i() == 42);
  }
}

//
// Polymorphic serialization
//

struct base
{
  virtual ~base() = default;
  friend bool operator==(base, base) { return true; }
  int i = 0;
};

template <typename Serializer>
void serialize(Serializer& sink, base const& b)
{
  sink << b.i;
}

template <typename Deserializer>
void deserialize(Deserializer& source, base& b)
{
  source >> b.i;
}

struct derived1 : base
{
  friend bool operator==(derived1, derived1) { return true; }
  int j = 1;
};

namespace vast {

template <>
struct access::state<derived1>
{
  template <class F>
  static void read(derived1 const& x, F f)
  {
    f(static_cast<base const&>(x), x.j);
  }

  template <class F>
  static void write(derived1& x, F f)
  {
    f(static_cast<base&>(x), x.j);
  }
};

} // namespace vast

struct derived2 : base
{
  friend bool operator==(derived2, derived2) { return true; }
  int k = 1;
};

template <typename Serializer>
void serialize(Serializer& sink, derived2 const& d)
{
  sink << static_cast<base const&>(d) << d.k;
}

template <typename Deserializer>
void deserialize(Deserializer& source, derived2& d)
{
  source >> static_cast<base&>(d) >> d.k;
}

TEST(polymorphic serialization)
{
  announce_hierarchy<base, derived1, derived2>("derived1", "derived2");
  auto uti = caf::uniform_typeid<derived1>();
  REQUIRE(uti != nullptr);
  CHECK(uti->name() == std::string{"derived1"});
  std::vector<uint8_t> buf;
  {
    derived1 d1;
    d1.i = 42;
    d1.j = 1337;
    auto out = io::make_container_output_stream(buf);
    binary_serializer bs{out};
    polymorphic_serialize(bs, &d1);
  }
  {
    base* b;
    auto in = io::make_container_input_stream(buf);
    binary_deserializer bd{in};
    polymorphic_deserialize(bd, b);
    REQUIRE(b);
    CHECK(b->i == 42);
    auto d1 = dynamic_cast<derived1*>(b);
    REQUIRE(d1 != nullptr);
    CHECK(d1->j == 1337);
  }
}

FIXTURE_SCOPE(events_scope, fixtures::simple_events)

// The serialization of events goes through custom (de)serialization routines
// to avoid redudant type serialization.
TEST(vector<event> serialization)
{
  std::string str;
  {
    auto out = io::make_container_output_stream(str);
    binary_serializer bs{out};
    bs << events;
  }

  std::vector<event> deserialized;
  auto in = io::make_container_input_stream(str);
  binary_deserializer ds{in};
  ds >> deserialized;

  CHECK(events == deserialized);
}

FIXTURE_SCOPE_END()
