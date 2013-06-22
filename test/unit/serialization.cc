#include "test.h"
#include "event_fixture.h"

#include "vast/chunk.h"
#include "vast/serialization.h"
#include "vast/io/array_stream.h"
#include "vast/io/container_stream.h"

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

// A serializable object.
class serializable
{
  friend struct access;
public:
  int i() const
  {
    return i_;
  }

private:
  void serialize(serializer& sink)
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
    std::vector<int> input(1u << 10);
    BOOST_REQUIRE(input.size() % 2 == 0);
    for (size_t i = 0; i < input.size() / 2; ++i)
      input[i] = i % 128;
    for (size_t i = input.size() / 2; i < input.size(); ++i)
      input[i] = i % 2;

    std::vector<uint8_t> tmp;
    {
      auto out = io::make_container_output_stream(tmp);
      std::unique_ptr<io::compressed_output_stream> comp_out(
          io::compressed_output_stream::create(method, out));
      binary_serializer sink(*comp_out);
      sink << input;
      sink << serializable();
    }

    std::vector<int> output;
    io::array_input_stream in(tmp.data(), tmp.size());
    std::unique_ptr<io::compressed_input_stream> comp_in(
        io::compressed_input_stream::create(method, in));
    binary_deserializer source(*comp_in);
    source >> output;
    BOOST_REQUIRE_EQUAL(input.size(), output.size());
    for (size_t i = 0; i < input.size(); ++i)
    {
      if (output[i] != input[i])
        std::cerr << i << " !!" << std::endl;
      BOOST_CHECK_EQUAL(output[i], input[i]);
    }
    serializable x;
    source >> x;
    BOOST_CHECK_EQUAL(x.i(), 42);
  }
}

BOOST_AUTO_TEST_CASE(chunk_serialization)
{
  chunk<event> chk;
  {
    auto putter = std::move(chunk<event>::putter(&chk)); // test = and move.
    for (size_t i = 0; i < 1e3; ++i)
      putter << events[i % events.size()];
    BOOST_CHECK(chk.size() == 1e3);
  }

  auto getter = std::move(chunk<event>::getter(&chk)); // test = and move.
  for (size_t i = 0; i < 1e3; ++i)
  {
    event deserialized;
    getter >> deserialized;
    BOOST_CHECK(deserialized == events[i % events.size()]);
  }

  size_t n = 0;
  auto h = chunk<event>::getter(&chk);
  h.get([&](event e) { BOOST_CHECK(e == events[n++ % events.size()]); });

  auto copy(chk);
  BOOST_CHECK(chk == copy);
}

BOOST_AUTO_TEST_SUITE_END()
