#include "framework/unit.h"

#include "vast/value.h"
#include "vast/optional.h"
#include "vast/serialization/all.h"
#include "vast/io/serialization.h"

SUITE("serialization")

using namespace vast;

TEST("byte swapping")
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

TEST("containers")
{
  std::vector<double> v0{4.2, 8.4, 16.8}, v1;
  std::list<int> l0{4, 2}, l1;
  std::unordered_map<int, int> u0{{4, 2}, {8, 4}}, u1;

  std::vector<uint8_t> buf;
  io::archive(buf, v0, l0, u0);
  io::unarchive(buf, v1, l1, u1);

  CHECK(v0 == v1);
  CHECK(l0 == l1);
  CHECK(u0 == u1);
}

TEST("optional<T>")
{
  optional<std::string> o1 = std::string{"foo"};
  decltype(o1) o2;
  std::vector<uint8_t> buf;
  io::archive(buf, o1);
  io::unarchive(buf, o2);
  REQUIRE(o1);
  REQUIRE(o2);
  CHECK(*o2 == "foo");
  CHECK(*o1 == *o2);
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

TEST("vast::io API")
{
  std::vector<io::compression> methods{io::null, io::lz4};
#ifdef VAST_HAVE_SNAPPY
  methods.push_back(io::snappy);
#endif // VAST_HAVE_SNAPPY
  for (auto method : methods)
  {
    std::vector<int> input(1u << 10), output;
    REQUIRE((input.size() % 2) == 0);
    for (size_t i = 0; i < input.size() / 2; ++i)
      input[i] = i % 128;
    for (size_t i = input.size() / 2; i < input.size(); ++i)
      input[i] = i % 2;

    std::vector<uint8_t> buf;
    serializable x;
    io::compress(method, buf, input, serializable());
    io::decompress(method, buf, output, x);

    REQUIRE(input.size() == output.size());
    for (size_t i = 0; i < input.size(); ++i)
      CHECK(output[i] == input[i]);
    CHECK(x.i() == 42);
  }
}

TEST("objects")
{
  object o, p;
  o = object::adopt(new record{42, 84, 1337});

  std::vector<uint8_t> buf;
  io::archive(buf, o);
  io::unarchive(buf, p);

  CHECK(o == p);
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

TEST("polymorphic objects")
{
  derived d;
  d.i = 42;

  // Polymorphic types must be announced as their concrete type is not known at
  // compile time.
  REQUIRE((announce<derived>()));

  // Due to the lacking introspection capabilities in C++, the serialization
  // framework requires explicit registration of each derived
  // class to provide type-safe access.
  REQUIRE((make_convertible<derived, base>()));

  std::vector<uint8_t> buf;
  {
    // We serialize the object through a polymorphic reference to the base
    // class, which simply invokes the correct virtual serialize() method of
    // the derived class.
    auto out = io::make_container_output_stream(buf);
    binary_serializer sink(out);
    base& b = d;
    REQUIRE(write_object(sink, b));
  }
  {
    // Actually, this is the same as serializing through a pointer directory,
    // because serializing a pointer assumes reference semantics and hence
    // writes an instance out as object.
    decltype(buf) buf2;
    base* bp = &d;
    io::archive(buf2, bp);
    CHECK(buf == buf2);
  }
  {
    // It should always be possible to deserialize an instance of the exact
    // derived type. This technically does not require any virtual functions.
    auto in = io::make_array_input_stream(buf);
    binary_deserializer source(in);
    derived e;
    REQUIRE(read_object(source, e));
    CHECK(e.i == 42);
  }
  {
    // Similarly, it should always be possible to retrieve an opaque object and
    // get the derived type via a type-checked invocation of get<T>.
    object o;
    io::unarchive(buf, o);
    CHECK(o.convertible_to<derived>());
    CHECK(get<derived>(o).f() == 42);
    // Since we've announced the type as being convertible to its base class,
    // we can also safely obtain a reference to the base.
    CHECK(o.convertible_to<base>());
    CHECK(get<base>(o).f() == 42);
    // Moreover, since we've assurred convertability we can extract the raw
    // instance from the object. Thereafter, we own it and have to delete it.
    base* b = o.release_as<base>();
    CHECK(b->f() == 42);
    REQUIRE(b != nullptr);
    delete b;
  }
  {
    // Finally, since all serializations of pointers are assumed to pertain to
    // objects with reference semantics, we can just serialize the pointer
    // directly.
    base* b = nullptr;
    io::unarchive(buf, b);
    REQUIRE(b != nullptr);
    CHECK(b->f() == 42);
    delete b;
  }
}
