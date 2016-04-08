#include "vast/bitvector.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitvector.hpp"

#define SUITE bitvector
#include "test.hpp"

using namespace vast;

TEST(to_string) {
  bitvector a;
  bitvector b{10};
  bitvector c{78, true};

  CHECK(to_string(a) == "");
  CHECK(to_string(b) == "0000000000");
  CHECK(to_string(c), std::string(78, '1'));
}

TEST(basic operations) {
  bitvector x;
  x.push_back(true);
  x.push_back(false);
  x.push_back(true);

  CHECK(x[0]);
  CHECK(!x[1]);
  CHECK(x[2]);

  CHECK(x.size() == 3);
  CHECK(x.blocks() == 1);

  x.append(0xf00f, 16);
  CHECK(x[3]);
  CHECK(x[18]);
  x.append(0xf0, 8);

  CHECK(x.blocks() == 1);
  CHECK(x.size() == 3 + 16 + 8);

  x.append(0);
  x.append(0xff, 8);
  CHECK(x.blocks() == 2);
  CHECK(x.size() == 3 + 16 + 8 + bitvector::block_width + 8);
}

TEST(block operations) {
  auto ones = bitvector::all_one;

  for (bitvector::block_type i = 0; i < bitvector::block_width - 1; ++i)
    CHECK(bitvector::next_bit(ones, i) == i + 1);
  CHECK(bitvector::next_bit(ones, bitvector::block_width - 1)
        == bitvector::npos);
  CHECK(bitvector::next_bit(ones, bitvector::block_width) == bitvector::npos);

  CHECK(bitvector::prev_bit(ones, bitvector::block_width) == bitvector::npos);
  for (bitvector::block_type i = bitvector::block_width - 1; i >= 1; --i)
    CHECK(bitvector::prev_bit(ones, i) == i - 1);
  CHECK(bitvector::prev_bit(ones, 0) == bitvector::npos);

  CHECK(bitvector::lowest_bit(ones) == 0);
  CHECK(bitvector::lowest_bit(ones & (ones - 1)) == 1);
  CHECK(bitvector::lowest_bit(ones & (ones - 3)) == 2);
}

TEST(bitwise operations) {
  bitvector a{6};
  CHECK(a.size() == 6);
  CHECK(a.blocks() == 1);

  a.toggle(3);
  CHECK(to_string(a) == "001000");
  CHECK(to_string(a << 1) == "010000");
  CHECK(to_string(a << 2) == "100000");
  CHECK(to_string(a << 3) == "000000");
  CHECK(to_string(a >> 1) == "000100");
  CHECK(to_string(a >> 2) == "000010");
  CHECK(to_string(a >> 3) == "000001");
  CHECK(to_string(a >> 4) == "000000");

  bitvector b{a};
  b[5] = b[1] = 1;
  CHECK(to_string(b) == "101010");
  CHECK(to_string(~b) == "010101");

  CHECK(to_string(a | ~b) == "011101");
  CHECK(to_string((~a << 2) & b) == to_string(a));

  CHECK(b.count() == 3);
}

TEST(backward search) {
  bitvector x;
  x.append(0xffff);
  x.append(0x30abffff7000ffff);

  auto i = x.find_last();
  CHECK(i == 125);
  i = x.find_prev(i);
  CHECK(i == 124);
  i = x.find_prev(i);
  CHECK(i == 119);
  CHECK(x.find_prev(63) == 15);

  bitvector y;
  y.append(0xf0ffffffffffff0f);
  CHECK(y.find_last() == 63);
  CHECK(y.find_prev(59) == 55);
}

TEST(iteration) {
  bitvector x;
  x.append(0x30abffff7000ffff);

  std::string str;
  std::transform(bitvector::const_bit_iterator::begin(x),
                 bitvector::const_bit_iterator::end(x), std::back_inserter(str),
                 [](auto bit) { return bit ? '1' : '0'; });

  std::string lsb_to_msb;
  CHECK(bitvector_printer<policy::lsb_to_msb>{}(lsb_to_msb, x));
  CHECK(lsb_to_msb == str);

  std::string rts;
  std::transform(bitvector::const_bit_iterator::rbegin(x),
                 bitvector::const_bit_iterator::rend(x),
                 std::back_inserter(rts),
                 [](auto bit) { return bit ? '1' : '0'; });

  std::reverse(str.begin(), str.end());
  CHECK(str == rts);

  std::string ones;
  std::transform(
    bitvector::const_ones_iterator::begin(x),
    bitvector::const_ones_iterator::end(x), std::back_inserter(ones),
    [](bitvector::const_reference bit) { return bit ? '1' : '0'; });

  CHECK(ones == "111111111111111111111111111111111111111111");

  auto i = bitvector::const_ones_iterator::rbegin(x);
  CHECK(i.base().position() == 61);
  ++i;
  CHECK(i.base().position() == 60);
  ++i;
  CHECK(i.base().position() == 55);
  while (i != bitvector::const_ones_iterator::rend(x))
    ++i;
  CHECK(i.base().position() == 0);

  auto j = bitvector::ones_iterator::rbegin(x);
  CHECK(j.base().position() == 61);
  *j.base() = false;
  ++j;
  *j.base() = false;
  j = bitvector::ones_iterator::rbegin(x);
  CHECK(j.base().position() == 55);
}

TEST(selective flipping) {
  using block_type = bitvector::block_type;
  auto blk = block_type{0xffffffffffffffff};
  CHECK(bitvector::flip(blk, 0) == block_type{0x0000000000000000});
  CHECK(bitvector::flip(blk, 1) == block_type{0x0000000000000001});
  CHECK(bitvector::flip(blk, 4) == block_type{0x000000000000000f});
  CHECK(bitvector::flip(blk, bitvector::block_width / 2)
        == block_type{0x00000000ffffffff});
  CHECK(bitvector::flip(blk, bitvector::block_width - 1)
        == block_type{0x7fffffffffffffff});

  bitvector v;
  v.append(0xffffffffffffffff);
  v.append(0xffffffffffffffff);
  v.flip(96);
  bitvector expected;
  expected.append(0xffffffffffffffff);
  expected.append(0x00000000ffffffff);
  CHECK(v == expected);
}

TEST(bitvector appending) {
  bitvector v1;
  v1.append(0xffffffffffffffff);
  v1.resize(200, false);
  v1.flip(150);

  bitvector v2;
  v2.append(0xffffffffffffffff);
  v2.append(0x00000000ffffffff);
  v2.resize(200, false);

  auto size_before = v1.size();
  v1.append(v2);
  CHECK(v1.size() == size_before + v2.size());
  CHECK(!v1[149]);
  CHECK(v1[150]);
  CHECK(v1[200]);
  CHECK(v1[263]);
  CHECK(v1[264]);
  CHECK(v1[295]);
  CHECK(!v1[296]);

  v1.resize(128);
  v2.resize(128);
  v1.append(v2);
  CHECK(v1.size() == 256);
}
