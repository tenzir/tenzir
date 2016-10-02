#include "vast/bitvector.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/bitvector.hpp"
#include "vast/load.hpp"
#include "vast/save.hpp"

#define SUITE bitvector
#include "test.hpp"

using namespace vast;

TEST(default construction) {
  bitvector<uint8_t> x;
  CHECK(x.empty());
  CHECK_EQUAL(x.size(), 0u);
}

TEST(copy construction) {
  bitvector<uint8_t> x{0, 1, 1, 0, 0, 0, 1};
  bitvector<uint8_t> y{x};
  REQUIRE_EQUAL(x, y);
  y.push_back(1);
  CHECK_NOT_EQUAL(x, y);
  y.pop_back();
  CHECK_EQUAL(x, y);
}


TEST(size construction) {
  bitvector<uint8_t> x(42);
  CHECK_EQUAL(x.size(), 42u);
  CHECK(!x[41]);
  bitvector<uint8_t> y(42, true);
  CHECK_EQUAL(y.size(), 42u);
  CHECK(y[3]);
  CHECK(y[29]);
  CHECK(y[41]);
}

TEST(initializer_list construction) {
  bitvector<uint8_t> x{0, 0, 0, 1, 0, 1}; // implicitly tests assign(f, l).
  REQUIRE_EQUAL(x.size(), 6u);
  CHECK(!x[0]);
  CHECK(!x[1]);
  CHECK(!x[2]);
  CHECK(x[3]);
  CHECK(!x[4]);
  CHECK(x[5]);
}

TEST(iterator) {
  bitvector<uint8_t> x(25, true);
  CHECK(std::all_of(x.begin(), x.end(), [](auto bit) { return bit; }));
  // Ensure that we do N iterations for bitvector of size N.
  auto n = 0u;
  auto f = x.cbegin();
  auto l = x.cend();
  for (; f != l; ++f, ++n)
    ;
  REQUIRE_EQUAL(n, x.size());
  x[4] = false;
  x[23] = false;
  // Use iterators to convert to string.
  std::string str;
  auto bit_to_char = [](auto bit) { return bit ? '1' : '0'; };
  std::transform(x.begin(), x.end(), std::back_inserter(str), bit_to_char);
  CHECK_EQUAL(str, "1111011111111111111111101");
  // Reverse
  std::string rts;
  std::transform(x.rbegin(), x.rend(), std::back_inserter(rts), bit_to_char);
  std::reverse(str.begin(), str.end());
  CHECK_EQUAL(str, rts);
}

TEST(modifiers) {
  bitvector<uint8_t> x;
  CHECK(x.empty());
  CHECK_EQUAL(x.size(), 0u);
  MESSAGE("push_back");
  x.push_back(true);
  x.push_back(false);
  x.push_back(true);
  REQUIRE_EQUAL(x.size(), 3u);
  CHECK(x[0]);
  CHECK(!x[1]);
  CHECK(x[2]);
  x.push_back(false);
  x.push_back(true);
  x.push_back(false);
  x.push_back(true);
  x.push_back(true);
  REQUIRE_EQUAL(x.size(), 8u);
  CHECK(x[7]);
  x.push_back(false); // overflow into next word
  REQUIRE_EQUAL(x.size(), 9u);
  CHECK(!x[8]);
  x.pop_back(); // previous word again
  CHECK_EQUAL(x.size(), 8u);
  CHECK(!x.empty());
  x.clear();
  CHECK(x.empty());
}

TEST(resize) {
  bitvector<uint8_t> x;
  x.resize(20);
  CHECK_EQUAL(to_string(x), "00000000000000000000");
  x[10] = true;
  CHECK_EQUAL(to_string(x), "00000000001000000000");
  x.resize(11);
  CHECK_EQUAL(to_string(x), "00000000001");
  x.resize(10);
  CHECK_EQUAL(to_string(x), "0000000000");
  x.resize(13, true);
  CHECK_EQUAL(to_string(x), "0000000000111");
  x.resize(15, false);
  CHECK_EQUAL(to_string(x), "000000000011100");
  x.resize(32, true);
  CHECK_EQUAL(to_string(x), "00000000001110011111111111111111");
  x.resize(16, false);
  x.resize(128, false);
  auto str = "0000000000111001" + std::string(112, '0');
  CHECK_EQUAL(to_string(x), str);
  x.resize(256, true);
  str += std::string(128, '1');
  CHECK_EQUAL(to_string(x), str);
}

TEST(flip) {
  bitvector<uint8_t> x(23);
  x.flip();
  CHECK_EQUAL(to_string(x), "11111111111111111111111");
  x[10] = false;
  x[21] = false;
  CHECK_EQUAL(to_string(x), "11111111110111111111101");
  x.flip();
  CHECK_EQUAL(to_string(x), "00000000001000000000010");
}

TEST(relational operators) {
  bitvector<uint16_t> x, y;
  CHECK_EQUAL(x, y);
  x.push_back(true);
  CHECK_NOT_EQUAL(x, y);
  y.push_back(true);
  CHECK_EQUAL(x, y);
  x.pop_back();
  CHECK_NOT_EQUAL(x, y);
  y.pop_back();
  CHECK_EQUAL(x, y);
  x.resize(100, true);
  y.resize(100, true);
  CHECK_EQUAL(x, y);
  x[99] = false;
  CHECK_NOT_EQUAL(x, y);
  x.resize(99);
  y.resize(99);
  CHECK_EQUAL(x, y);
}

TEST(counting) {
  bitvector<uint64_t> x(1024, true);
  CHECK_EQUAL(x.count(), 1024u);
  x.push_back(0);
  x.push_back(0);
  x.push_back(0);
  x.push_back(0);
  x.push_back(1);
  CHECK_EQUAL(x.count(), 1025u);
  x.resize(2048, false);
  x.resize(4096, true);
  CHECK_EQUAL(x.count(), 1025u + 2048);
}

TEST(append_block) {
  bitvector<uint8_t> x;
  x.append_block(0b01111011);
  CHECK_EQUAL(to_string(x), "11011110");
  x.append_block(0b00111101, 6);
  CHECK_EQUAL(to_string(x), "11011110101111");
  x.append_block(0b11000010, 3);
  CHECK_EQUAL(to_string(x), "11011110101111010");
  x.append_block(0b10101010);
  CHECK_EQUAL(to_string(x), "1101111010111101001010101");
  x.append_block(0b10101010, 7);
  CHECK_EQUAL(to_string(x), "11011110101111010010101010101010");
}

TEST(append_blocks) {
  MESSAGE("block-wise copy");
  bitvector<uint8_t> x;
  std::vector<uint8_t> blocks = {1, 2, 4};
  x.append_blocks(blocks.begin(), blocks.end());
  CHECK_EQUAL(to_string(x), "100000000100000000100000");
  MESSAGE("shifting copy");
  bitvector<uint8_t> y;
  y.push_back(true);
  y.push_back(false);
  y.push_back(false);
  y.append_blocks(blocks.begin(), blocks.end());
  CHECK_EQUAL(to_string(y), "100100000000100000000100000");
}

TEST(append bits) {
  // Effectively tests resize().
  bitvector<uint8_t> x;
  x.append_bits(10, true);
  x.append_bits(5, false);
  x.append_bits(5, true);
  CHECK_EQUAL(to_string(x), "11111111110000011111");
}

TEST(serializable) {
  bitvector<uint64_t> x, y;
  x.resize(1024, false);
  x[1000] = true;
  std::vector<char> buf;
  save(buf, x);
  load(buf, y);
  REQUIRE_EQUAL(x, y);
  CHECK(y[1000]);
}

TEST(printable) {
  bitvector<uint32_t> a;
  CHECK_EQUAL(to_string(a), "");
  bitvector<uint32_t> b(10);
  b[2] = true;
  CHECK_EQUAL(to_string(b), "0010000000");
  bitvector<uint32_t> c(78, true);
  CHECK_EQUAL(to_string(c), std::string(78, '1'));
  MESSAGE("MSB to LSB");
  auto p = bitvector_printer<bitvector<uint32_t>, policy::msb_to_lsb>{};
  std::string str;
  CHECK(p(str, b));
  CHECK_EQUAL(str, "0000000100");
}
