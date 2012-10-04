#include "test.h"
#include "vast/bitmap.h"
#include "vast/to_string.h"

using namespace vast;

template <typename Bitstream>
std::string stringify(Bitstream const& bs)
{
  return to_string(bs.bits());
}

BOOST_AUTO_TEST_CASE(vector_storage)
{
  typedef null_bitstream bitstream_type;
  detail::vector_storage<uint8_t, bitstream_type> s;
  BOOST_CHECK(s.emplace(0, {}));
  BOOST_CHECK(s.emplace(1, bitstream_type(10, true)));
  BOOST_CHECK(s.emplace(2, {}));
  BOOST_CHECK(s.emplace(3, bitstream_type(5, false)));
  BOOST_CHECK(s.emplace(4, {}));
  BOOST_CHECK_EQUAL(s.cardinality(), 5);
  auto b = s.bounds(2);
  BOOST_CHECK(b.first && b.second);
  BOOST_CHECK_EQUAL(b.first->size(), 10);
  BOOST_CHECK_EQUAL(b.second->size(), 5);
  auto c = s.bounds(0);
  BOOST_CHECK(! c.first && c.second);
  auto d = s.bounds(4);
  BOOST_CHECK(d.first && ! d.second);

  detail::vector_storage<uint8_t, bitstream_type> t;
  BOOST_CHECK(t.emplace(2, bitstream_type(10, true)));
  BOOST_CHECK(t.emplace(4, bitstream_type(5, false)));
  BOOST_CHECK_EQUAL(t.cardinality(), 2);
  auto e = t.bounds(3);
  BOOST_CHECK(e.first && e.second);
  BOOST_CHECK_EQUAL(e.first->size(), 10);
  BOOST_CHECK_EQUAL(e.second->size(), 5);
  auto f = t.bounds(0);
  BOOST_CHECK(! f.first && f.second);
  auto g = t.bounds(8);
  BOOST_CHECK(g.first && ! g.second);
}

BOOST_AUTO_TEST_CASE(basic_bitmap)
{
  bitmap<int> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  BOOST_CHECK_EQUAL(stringify(*bm[21]), "01000");
  BOOST_CHECK_EQUAL(stringify(*bm[30]), "10000");
  BOOST_CHECK_EQUAL(stringify(*bm[42]), "00101");
  BOOST_CHECK_EQUAL(stringify(*bm[84]), "00010");

  auto zeros = bm.all(false);
  BOOST_CHECK_EQUAL(zeros.size(), bm.size());
  BOOST_CHECK_EQUAL(zeros[0], false);
  BOOST_CHECK_EQUAL(zeros[zeros.size() - 1], false);
}

BOOST_AUTO_TEST_CASE(range_encoded_bitmap)
{
  bitmap<int, null_bitstream, range_encoder> bm;
  bm.push_back(42);
  bm.push_back(84);
  bm.push_back(42);
  bm.push_back(21);
  bm.push_back(30);

  BOOST_CHECK_EQUAL(stringify(*bm[21]), "01000");
  BOOST_CHECK_EQUAL(stringify(*bm[30]), "11000");
  BOOST_CHECK_EQUAL(stringify(*bm[42]), "11101");
  BOOST_CHECK_EQUAL(stringify(*bm[84]), "11111");
}

BOOST_AUTO_TEST_CASE(binary_encoded_bitmap)
{
  bitmap<int8_t, null_bitstream, binary_encoder> bm;
  bm.push_back(0);
  bm.push_back(1);
  bm.push_back(1);
  bm.push_back(2);
  bm.push_back(3);
  bm.push_back(2);
  bm.push_back(2);

  BOOST_CHECK_EQUAL(stringify(*bm[0]), "0000001");
  BOOST_CHECK_EQUAL(stringify(*bm[1]), "0000110");
  BOOST_CHECK_EQUAL(stringify(*bm[2]), "1101000");
  BOOST_CHECK_EQUAL(stringify(*bm[3]), "0010000");

  // Binary encoding always returns an answer after the first element has been
  // inserted into the bitmap.
  BOOST_CHECK_EQUAL(stringify(*bm[4]), "0000000");
  BOOST_CHECK_EQUAL(stringify(*bm[5]), "0000000");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_integral)
{
  bitmap<int, null_bitstream, equality_encoder, precision_binner> bm({}, 2);
  bm.push_back(183);
  bm.push_back(215);
  bm.push_back(350);
  bm.push_back(253);
  bm.push_back(101);
  
  BOOST_CHECK_EQUAL(stringify(*bm[100]), "10001");
  BOOST_CHECK_EQUAL(stringify(*bm[200]), "01010");
  BOOST_CHECK_EQUAL(stringify(*bm[300]), "00100");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_negative)
{
  bitmap<double, null_bitstream, equality_encoder, precision_binner> bm({}, -3);

  // These end up in different bins...
  bm.push_back(42.001);
  bm.push_back(42.002);

  // ...whereas these in the same.
  bm.push_back(43.0014);
  bm.push_back(43.0013);

  bm.push_back(43.0005); // This one is rounded up to the previous bin...
  bm.push_back(43.0015); // ...and this one to the next.
  
  BOOST_CHECK_EQUAL(stringify(*bm[42.001]), "000001");
  BOOST_CHECK_EQUAL(stringify(*bm[42.002]), "000010");
  BOOST_CHECK_EQUAL(stringify(*bm[43.001]), "011100");
  BOOST_CHECK_EQUAL(stringify(*bm[43.002]), "100000");
}

BOOST_AUTO_TEST_CASE(bitmap_precision_binning_double_positive)
{
  bitmap<double, null_bitstream, equality_encoder, precision_binner> bm({}, 1);

  // These end up in different bins...
  bm.push_back(42.123);
  bm.push_back(53.9);

  // ...whereas these in the same.
  bm.push_back(41.02014);
  bm.push_back(44.91234543);

  bm.push_back(39.5); // This one just makes it into the 40 bin.
  bm.push_back(49.5); // ...and this in the 50.
  
  BOOST_CHECK_EQUAL(stringify(*bm[40.0]), "011101");
  BOOST_CHECK_EQUAL(stringify(*bm[50.0]), "100010");
}
