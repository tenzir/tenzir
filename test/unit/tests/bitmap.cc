#include "framework/unit.h"
#include "vast/bitmap.h"
#include "vast/convert.h"
#include "vast/io/serialization.h"

using namespace vast;

// Prints doubles as IEEE 754 and with our custom offset binary encoding.
void print(double d)
{
  std::cout << d << "\t = ";
  auto o = detail::order(d, 4);
  auto xu = reinterpret_cast<uint64_t*>(&d);
  auto x = *xu;
  auto yu = reinterpret_cast<uint64_t*>(&o);
  auto y = *yu;

  for (size_t i = 0; i < 64; ++i)
  {
    if (i == 1 || i == 12)
      std::cout << ' ';
    std::cout << ((x >> (64 - i - 1)) & 1);
  }

  std::cout << '\t';

  for (size_t i = 0; i < 64; ++i)
  {
    if (i == 1 || i == 12)
      std::cout << ' ';
    std::cout << ((y >> (64 - i - 1)) & 1);
  }

  std::cout << std::endl;
}

SUITE("bitmap")

TEST("bitwise total ordering")
{
  CHECK(detail::order(0u) == 0);
  CHECK(detail::order(4u) == 4);

  int32_t i = -4;
  CHECK(detail::order(i) == 2147483644);
  i = 4;
  CHECK(detail::order(i) == 2147483652);

  //print(-1111.2);
  //print(-10.0);
  //print(-2.4);
  //print(-2.2);
  //print(-2.0);
  //print(-1.0);
  //print(-0.1);
  //print(-0.001);
  //print(-0.0);
  //print(0.0);
  //print(0.001);
  //print(0.1);
  //print(1.0);
  //print(2.0);
  //print(2.2);
  //print(2.4);
  //print(10.0);;
  //print(1111.2);
}

TEST("basic bitmap")
{
  bitmap<int, null_bitstream> bm, bm2;
  REQUIRE(bm.push_back(42));
  REQUIRE(bm.push_back(84));
  REQUIRE(bm.push_back(42));
  REQUIRE(bm.push_back(21));
  REQUIRE(bm.push_back(30));

  CHECK(to_string(*bm[21]) == "00010");

  CHECK(to_string(*bm[30]) == "00001");
  CHECK(to_string(*bm[42]) == "10100");
  CHECK(to_string(*bm[84]) == "01000");
  CHECK(to_string(*bm[39]) == "00000");

  CHECK(to_string(*bm.lookup(not_equal, 21)) == "11101");
  CHECK(to_string(*bm.lookup(not_equal, 30)) == "11110");
  CHECK(to_string(*bm.lookup(not_equal, 42)) == "01011");
  CHECK(to_string(*bm.lookup(not_equal, 84)) == "10111");
  CHECK(bm.lookup(not_equal, 13));
  CHECK(to_string(*bm.lookup(not_equal, 13)) == "11111");

  CHECK(bm.append(5, false));
  CHECK(bm.size() == 10);

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  // The default bitmap storage is unordered, so the the following commented
  // check may fail due to different underlying hash tables. However, the
  // bitmaps should still be equal.
  //CHECK(to_string(bm) == to_string(bm2));
  CHECK(bm == bm2);
  CHECK(bm.size() == bm2.size());
  CHECK(to_string(*bm[21]) == to_string(*bm2[21]));
  CHECK(to_string(*bm[30]) == to_string(*bm2[30]));
  CHECK(to_string(*bm[42]) == to_string(*bm2[42]));
  CHECK(to_string(*bm[84]) == to_string(*bm2[84]));
}

TEST("range coding")
{
  range_bitslice_coder<uint8_t, null_bitstream> r;

  REQUIRE(r.encode(0));
  REQUIRE(r.encode(6));
  REQUIRE(r.encode(9));
  REQUIRE(r.encode(10));
  REQUIRE(r.encode(77));
  REQUIRE(r.encode(99));
  REQUIRE(r.encode(100));
  REQUIRE(r.encode(255));
  REQUIRE(r.encode(254));

  //r.each(
  //    [&](size_t, uint8_t x, null_bitstream const& bs)
  //    {
  //      std::cout << (uint64_t)x << "\t" << bs << std::endl;
  //    });

  CHECK(to_string(*r.decode(0,   less)) ==          "000000000");
  CHECK(to_string(*r.decode(8,   less)) ==          "110000000");
  CHECK(to_string(*r.decode(9,   less)) ==          "110000000");
  CHECK(to_string(*r.decode(10,  less)) ==          "111000000");
  CHECK(to_string(*r.decode(100, less)) ==          "111111000");
  CHECK(to_string(*r.decode(254, less)) ==          "111111100");
  CHECK(to_string(*r.decode(255, less)) ==          "111111101");
  CHECK(to_string(*r.decode(0,   less_equal)) ==    "100000000");
  CHECK(to_string(*r.decode(8,   less_equal)) ==    "110000000");
  CHECK(to_string(*r.decode(9,   less_equal)) ==    "111000000");
  CHECK(to_string(*r.decode(10,  less_equal)) ==    "111100000");
  CHECK(to_string(*r.decode(100, less_equal)) ==    "111111100");
  CHECK(to_string(*r.decode(254, less_equal)) ==    "111111101");
  CHECK(to_string(*r.decode(255, less_equal)) ==    "111111111");
  CHECK(to_string(*r.decode(0,   greater)) ==       "011111111");
  CHECK(to_string(*r.decode(8,   greater)) ==       "001111111");
  CHECK(to_string(*r.decode(9,   greater)) ==       "000111111");
  CHECK(to_string(*r.decode(10,  greater)) ==       "000011111");
  CHECK(to_string(*r.decode(100, greater)) ==       "000000011");
  CHECK(to_string(*r.decode(254, greater)) ==       "000000010");
  CHECK(to_string(*r.decode(255, greater)) ==       "000000000");
  CHECK(to_string(*r.decode(0,   greater_equal)) == "111111111");
  CHECK(to_string(*r.decode(8,   greater_equal)) == "001111111");
  CHECK(to_string(*r.decode(9,   greater_equal)) == "001111111");
  CHECK(to_string(*r.decode(10,  greater_equal)) == "000111111");
  CHECK(to_string(*r.decode(100, greater_equal)) == "000000111");
  CHECK(to_string(*r.decode(254, greater_equal)) == "000000011");
  CHECK(to_string(*r.decode(255, greater_equal)) == "000000010");
  CHECK(to_string(*r.decode(0,   equal)) ==         "100000000");
  CHECK(to_string(*r.decode(6,   equal)) ==         "010000000");
  CHECK(to_string(*r.decode(8,   equal)) ==         "000000000");
  CHECK(to_string(*r.decode(9,   equal)) ==         "001000000");
  CHECK(to_string(*r.decode(10,  equal)) ==         "000100000");
  CHECK(to_string(*r.decode(77,  equal)) ==         "000010000");
  CHECK(to_string(*r.decode(100, equal)) ==         "000000100");
  CHECK(to_string(*r.decode(254, equal)) ==         "000000001");
  CHECK(to_string(*r.decode(255, equal)) ==         "000000010");
  CHECK(to_string(*r.decode(0,   not_equal)) ==     "011111111");
  CHECK(to_string(*r.decode(6,   not_equal)) ==     "101111111");
  CHECK(to_string(*r.decode(8,   not_equal)) ==     "111111111");
  CHECK(to_string(*r.decode(9,   not_equal)) ==     "110111111");
  CHECK(to_string(*r.decode(10,  not_equal)) ==     "111011111");
  CHECK(to_string(*r.decode(100, not_equal)) ==     "111111011");
  CHECK(to_string(*r.decode(254, not_equal)) ==     "111111110");
  CHECK(to_string(*r.decode(255, not_equal)) ==     "111111101");

  r = {};

  for (size_t i = 0; i < 256; ++i)
    CHECK(r.encode(i));

  CHECK(r.size() == 256);

  std::string str(256, '0');
  for (size_t i = 0; i < 256; ++i)
  {
    str[i] = '1';
    CHECK(to_string(*r.decode(i, less_equal)) == str);
  }
}

TEST("range encoded bitmap (null)")
{
  bitmap<int8_t, null_bitstream, range_bitslice_coder> bm, bm2;
  REQUIRE(bm.push_back(42));
  REQUIRE(bm.push_back(84));
  REQUIRE(bm.push_back(42));
  REQUIRE(bm.push_back(21));
  REQUIRE(bm.push_back(30));

  CHECK(to_string(*bm.lookup(not_equal, 13)) == "11111");
  CHECK(to_string(*bm.lookup(not_equal, 42)) == "01011");
  CHECK(to_string(*bm.lookup(equal, 21)) == "00010");
  CHECK(to_string(*bm.lookup(equal, 30)) == "00001");
  CHECK(to_string(*bm.lookup(equal, 42)) == "10100");
  CHECK(to_string(*bm.lookup(equal, 84)) == "01000");
  CHECK(to_string(*bm.lookup(less_equal, 21)) == "00010");
  CHECK(to_string(*bm.lookup(less_equal, 30)) == "00011");
  CHECK(to_string(*bm.lookup(less_equal, 42)) == "10111");
  CHECK(to_string(*bm.lookup(less_equal, 84)) == "11111");
  CHECK(to_string(*bm.lookup(less_equal, 25)) == "00010");
  CHECK(to_string(*bm.lookup(less_equal, 80)) == "10111");
  CHECK(to_string(*bm.lookup(not_equal, 30)) == "11110");
  CHECK(to_string(*bm.lookup(greater, 42)) == "01000");
  CHECK(to_string(*bm.lookup(greater, 13)) == "11111");
  CHECK(to_string(*bm.lookup(greater, 84)) == "00000");
  CHECK(to_string(*bm.lookup(less, 42)) == "00011");
  CHECK(to_string(*bm.lookup(less, 84)) == "10111");
  CHECK(to_string(*bm.lookup(greater_equal, 84)) == "01000");
  CHECK(to_string(*bm.lookup(greater_equal, -42)) == "11111");
  CHECK(to_string(*bm.lookup(greater_equal, 22)) == "11101");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  CHECK(bm == bm2);
  CHECK(to_string(bm) == to_string(bm2));
  CHECK(to_string(*bm2.lookup(greater, 84)) == "00000");
  CHECK(to_string(*bm2.lookup(less, 84)) == "10111");
  CHECK(to_string(*bm2.lookup(greater_equal, -42)) == "11111");
}

TEST("range encoded bitmap (EWAH)")
{
  bitmap<uint16_t, ewah_bitstream, range_bitslice_coder> bm;
  bm.push_back(80);
  bm.push_back(443);
  bm.push_back(53);
  bm.push_back(8);
  bm.push_back(31337);
  bm.push_back(80);
  bm.push_back(8080);

  ewah_bitstream all_zeros;
  all_zeros.append(7, false);
  ewah_bitstream all_ones;
  all_ones.append(7, true);

  ewah_bitstream greater_eight;
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(0);
  greater_eight.push_back(1);
  greater_eight.push_back(1);
  greater_eight.push_back(1);

  ewah_bitstream greater_eighty;
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);
  greater_eighty.push_back(0);
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);
  greater_eighty.push_back(0);
  greater_eighty.push_back(1);

  CHECK(*bm.lookup(greater, 1) == all_ones);
  CHECK(*bm.lookup(greater, 2) == all_ones);
  CHECK(*bm.lookup(greater, 3) == all_ones);
  CHECK(*bm.lookup(greater, 4) == all_ones);
  CHECK(*bm.lookup(greater, 5) == all_ones);
  CHECK(*bm.lookup(greater, 6) == all_ones);
  CHECK(*bm.lookup(greater, 7) == all_ones);
  CHECK(*bm.lookup(greater, 8) == greater_eight);
  CHECK(*bm.lookup(greater, 9) == greater_eight);
  CHECK(*bm.lookup(greater, 10) == greater_eight);
  CHECK(*bm.lookup(greater, 11) == greater_eight);
  CHECK(*bm.lookup(greater, 12) == greater_eight);
  CHECK(*bm.lookup(greater, 13) == greater_eight);
  CHECK(*bm.lookup(greater, 80) == greater_eighty);
  CHECK(*bm.lookup(greater, 80) == greater_eighty);
  CHECK(*bm.lookup(greater, 31337) == all_zeros);
  CHECK(*bm.lookup(greater, 31338) == all_zeros);
}

TEST("binary encoded bitmap")
{
  bitmap<int8_t, null_bitstream, binary_bitslice_coder> bm, bm2;
  REQUIRE(bm.push_back(0));
  REQUIRE(bm.push_back(1));
  REQUIRE(bm.push_back(1));
  REQUIRE(bm.push_back(2));
  REQUIRE(bm.push_back(3));
  REQUIRE(bm.push_back(2));
  REQUIRE(bm.push_back(2));

  CHECK(to_string(*bm[0]) ==   "1000000");
  CHECK(to_string(*bm[1]) ==   "0110000");
  CHECK(to_string(*bm[2]) ==   "0001011");
  CHECK(to_string(*bm[3]) ==   "0000100");
  CHECK(to_string(*bm[-42]) == "0000000");
  CHECK(to_string(*bm[4]) ==   "0000000");

  CHECK(to_string(*bm.lookup(not_equal, -42)) == "1111111");
  CHECK(to_string(*bm.lookup(not_equal, 0)) ==   "0111111");
  CHECK(to_string(*bm.lookup(not_equal, 1)) ==   "1001111");
  CHECK(to_string(*bm.lookup(not_equal, 2)) ==   "1110100");
  CHECK(to_string(*bm.lookup(not_equal, 3)) ==   "1111011");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  CHECK(bm == bm2);
  CHECK(to_string(bm) == to_string(bm2));
  CHECK(to_string(*bm2[0]) == "1000000");
  CHECK(to_string(*bm2[1]) == "0110000");
  CHECK(to_string(*bm2[2]) == "0001011");
}

TEST("precision binning (integral)")
{
  bitmap<int, null_bitstream, equality_coder, precision_binner> bm{2};
  REQUIRE(bm.push_back(183));
  REQUIRE(bm.push_back(215));
  REQUIRE(bm.push_back(350));
  REQUIRE(bm.push_back(253));
  REQUIRE(bm.push_back(101));
  
  CHECK(to_string(*bm[100]) == "10001");
  CHECK(to_string(*bm[200]) == "01010");
  CHECK(to_string(*bm[300]) == "00100");
}

TEST("precision binning (double, negative)")
{
  bitmap<double, null_bitstream, equality_coder, precision_binner> bm{-3}, bm2;

  // These end up in different bins...
  REQUIRE(bm.push_back(42.001));
  REQUIRE(bm.push_back(42.002));

  // ...whereas these in the same.
  REQUIRE(bm.push_back(43.0014));
  REQUIRE(bm.push_back(43.0013));

  REQUIRE(bm.push_back(43.0005)); // This one is rounded up to the previous bin...
  REQUIRE(bm.push_back(43.0015)); // ...and this one to the next.
  
  CHECK(to_string(*bm[42.001]) == "100000");
  CHECK(to_string(*bm[42.002]) == "010000");
  CHECK(to_string(*bm[43.001]) == "001110");
  CHECK(to_string(*bm[43.002]) == "000001");

  std::vector<uint8_t> buf;
  io::archive(buf, bm);
  io::unarchive(buf, bm2);
  CHECK(to_string(*bm2[43.001]) == "001110");
  CHECK(to_string(*bm2[43.002]) == "000001");

  // Check if the precision got serialized properly and that adding a new
  // element lands in the right bin.
  REQUIRE(bm2.push_back(43.0022));
  CHECK(to_string(*bm2[43.002]) == "0000011");
}

TEST("precision binning (double, positive)")
{
  bitmap<double, null_bitstream, equality_coder, precision_binner> bm{1};

  // These end up in different bins...
  REQUIRE(bm.push_back(42.123));
  REQUIRE(bm.push_back(53.9));

  // ...whereas these in the same.
  REQUIRE(bm.push_back(41.02014));
  REQUIRE(bm.push_back(44.91234543));

  REQUIRE(bm.push_back(39.5)); // This one just makes it into the 40 bin.
  REQUIRE(bm.push_back(49.5)); // ...and this in the 50.
  
  CHECK(to_string(*bm[40.0]) == "101110");
  CHECK(to_string(*bm[50.0]) == "010001");
}
