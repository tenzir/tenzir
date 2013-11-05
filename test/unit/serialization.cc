#include "test.h"
#include "event_fixture.h"

#include "vast/object.h"
#include "vast/serialization.h"
#include "vast/io/serialization.h"

using namespace vast;

BOOST_FIXTURE_TEST_SUITE(event_serialization_tests, event_fixture)

BOOST_AUTO_TEST_CASE(byte_swapping)
{
  using util::byte_swap;

  uint8_t  x08 = 0x11;
  uint16_t x16 = 0x1122;
  uint32_t x32 = 0x11223344;
  uint64_t x64 = 0x1122334455667788;
  // TODO: extend to all arithmetic types, e.g., float and double.

  auto y08 = byte_swap<little_endian, big_endian>(x08);
  auto y16 = byte_swap<little_endian, big_endian>(x16);
  auto y32 = byte_swap<little_endian, big_endian>(x32);
  auto y64 = byte_swap<little_endian, big_endian>(x64);
  BOOST_CHECK_EQUAL(y08, 0x11);
  BOOST_CHECK_EQUAL(y16, 0x2211);
  BOOST_CHECK_EQUAL(y32, 0x44332211);
  BOOST_CHECK_EQUAL(y64, 0x8877665544332211);

  y08 = byte_swap<big_endian, little_endian>(y08);
  y16 = byte_swap<big_endian, little_endian>(y16);
  y32 = byte_swap<big_endian, little_endian>(y32);
  y64 = byte_swap<big_endian, little_endian>(y64);
  BOOST_CHECK_EQUAL(y08, x08);
  BOOST_CHECK_EQUAL(y16, x16);
  BOOST_CHECK_EQUAL(y32, x32);
  BOOST_CHECK_EQUAL(y64, x64);

  // NOP
  y08 = byte_swap<big_endian, big_endian>(y08);
  y16 = byte_swap<big_endian, big_endian>(y16);
  y32 = byte_swap<big_endian, big_endian>(y32);
  y64 = byte_swap<big_endian, big_endian>(y64);
  BOOST_CHECK_EQUAL(y08, x08);
  BOOST_CHECK_EQUAL(y16, x16);
  BOOST_CHECK_EQUAL(y32, x32);
  BOOST_CHECK_EQUAL(y64, x64);

  // NOP
  y08 = byte_swap<little_endian, little_endian>(y08);
  y16 = byte_swap<little_endian, little_endian>(y16);
  y32 = byte_swap<little_endian, little_endian>(y32);
  y64 = byte_swap<little_endian, little_endian>(y64);
  BOOST_CHECK_EQUAL(y08, x08);
  BOOST_CHECK_EQUAL(y16, x16);
  BOOST_CHECK_EQUAL(y32, x32);
  BOOST_CHECK_EQUAL(y64, x64);
}

BOOST_AUTO_TEST_CASE(containers)
{
  std::vector<double> v0{4.2, 8.4, 16.8}, v1;
  std::list<int> l0{4, 2}, l1;
  std::unordered_map<int, int> u0{{4, 2}, {8, 4}}, u1;

  std::vector<uint8_t> buf;
  io::archive(buf, v0, l0, u0);
  io::unarchive(buf, v1, l1, u1);

  BOOST_CHECK(v0 == v1);
  BOOST_CHECK(l0 == l1);
  BOOST_CHECK(u0 == u1);
}

// A serializable class.
class serializable
{
public:
  int i() const
  {
    return i_;
  }

private:
  friend vast::access;

  void serialize(serializer& sink) const
  {
    sink << i_ - 10;
  }

  void deserialize(deserializer& source)
  {
    source >> i_;
    i_ += 10;
  }

  int i_ = 42;
};

BOOST_AUTO_TEST_CASE(io_serialization_interface)
{
  std::vector<io::compression> methods{io::null, io::lz4};
#ifdef VAST_HAVE_SNAPPY
  methods.push_back(io::snappy);
#endif // VAST_HAVE_SNAPPY
  for (auto method : methods)
  {
    std::vector<int> input(1u << 10), output;
    BOOST_REQUIRE(input.size() % 2 == 0);
    for (size_t i = 0; i < input.size() / 2; ++i)
      input[i] = i % 128;
    for (size_t i = input.size() / 2; i < input.size(); ++i)
      input[i] = i % 2;

    std::vector<uint8_t> buf;
    serializable x;
    io::compress(method, buf, input, serializable());
    io::decompress(method, buf, output, x);

    BOOST_REQUIRE_EQUAL(input.size(), output.size());
    for (size_t i = 0; i < input.size(); ++i)
      BOOST_CHECK_EQUAL(output[i], input[i]);
    BOOST_CHECK_EQUAL(x.i(), 42);
  }
}

BOOST_AUTO_TEST_CASE(object_serialization)
{
  object o, p;
  o = object::adopt(new record{42, 84, 1337});

  std::vector<uint8_t> buf;
  io::archive(buf, o);
  io::unarchive(buf, p);

  BOOST_CHECK(o == p);
}

struct base
{
  virtual ~base() = default;
  virtual uint32_t f() const = 0;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
};

struct derived : base
{
  friend bool operator==(derived const& x, derived const& y)
  {
    return x.i == y.i;
  }

  virtual uint32_t f() const final
  {
    return i;
  }

  virtual void serialize(serializer& sink) const final
  {
    sink << i;
  }

  virtual void deserialize(deserializer& source) final
  {
    source >> i;
  }

  uint32_t i = 0;
};

BOOST_AUTO_TEST_CASE(polymorphic_object_serialization)
{
  derived d;
  d.i = 42;

  // Polymorphic types must be announced as their concrete type is not known at
  // compile time.
  BOOST_REQUIRE((announce<derived>()));

  // Due to the lacking introspection capabilities in C++, the serialization
  // framework requires explicit registration of each derived
  // class to provide type-safe access.
  BOOST_REQUIRE((make_convertible<derived, base>()));

  std::vector<uint8_t> buf;
  {
    // We serialize the object through a polymorphic reference to the base
    // class, which simply invokes the correct virtual serialize() method of
    // the derived class.
    auto out = io::make_container_output_stream(buf);
    binary_serializer sink(out);
    base& b = d;
    BOOST_REQUIRE(write_object(sink, b));
  }
  {
    // Actually, this is the same as serializing through a pointer directory,
    // because serializing a pointer assumes reference semantics and hence
    // writes an instance out as object.
    decltype(buf) buf2;
    base* bp = &d;
    io::archive(buf2, bp);
    BOOST_CHECK_EQUAL_COLLECTIONS(buf.begin(), buf.end(),
                                  buf2.begin(), buf2.end());
  }
  {
    // It should always be possible to deserialize an instance of the exact
    // derived type. This technically does not require any virtual functions.
    auto in = io::make_array_input_stream(buf);
    binary_deserializer source(in);
    derived e;
    BOOST_REQUIRE(read_object(source, e));
    BOOST_CHECK_EQUAL(e.i, 42);
  }
  {
    // Similarly, it should always be possible to retrieve an opaque object and
    // get the derived type via a type-checked invocation of get<T>.
    object o;
    io::unarchive(buf, o);
    BOOST_CHECK(o.convertible_to<derived>());
    BOOST_CHECK_EQUAL(get<derived>(o).f(), 42);
    // Since we've announced the type as being convertible to its base class,
    // we can also safely obtain a reference to the base.
    BOOST_CHECK(o.convertible_to<base>());
    BOOST_CHECK_EQUAL(get<base>(o).f(), 42);
    // Moreover, since we've assurred convertability we can extract the raw
    // instance from the object. Thereafter, we own it and have to delete it.
    base* b = o.release_as<base>();
    BOOST_CHECK_EQUAL(b->f(), 42);
    BOOST_REQUIRE(b != nullptr);
    delete b;
  }
  {
    // Finally, since all serializations of pointers are assumed to pertain to
    // objects with reference semantics, we can just serialize the pointer
    // directly.
    base* b = nullptr;
    io::unarchive(buf, b);
    BOOST_REQUIRE(b != nullptr);
    BOOST_CHECK_EQUAL(b->f(), 42);
    delete b;
  }
}

BOOST_AUTO_TEST_SUITE_END()
