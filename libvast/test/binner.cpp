#include "vast/binner.hpp"

#define SUITE bitmap
#include "test.hpp"

using namespace vast;

TEST(precision - binner 1) {
  using b = precision_binner<1>;
  CHECK(b::integral_max == 10);
  CHECK(b::fractional_max == 1);
  CHECK(b::bin(-4.2) == -4.0);
  CHECK(b::bin(4.2) == 4.0);
  CHECK(b::bin(-123.456) == -10.0);
  CHECK(b::bin(123.456) == 10.0);
}

TEST(precision - binner 5 and 2) {
  using b = precision_binner<5, 2>;
  CHECK(b::bin(-7.8) == -7.8);
  CHECK(b::bin(42.123) == 42.12);
  CHECK(b::bin(42.125) == 42.13);
  CHECK(b::bin(123456.0) == 100000.0);
}

TEST(precision - binner 2 and 3) {
  using b = precision_binner<2, 3>;
  CHECK(b::integral_max == 100);
  CHECK(b::fractional_max == 1000);
  CHECK(b::digits10 == 2 + 3);
  CHECK(b::digits2 == 17);

  CHECK(b::bin(42.001) == 42.001);
  CHECK(b::bin(42.002) == 42.002);
  CHECK(b::bin(43.0014) == 43.001);
  CHECK(b::bin(43.0013) == 43.001);
  CHECK(b::bin(43.0005) == 43.001);
  CHECK(b::bin(43.0015) == 43.002);
}

TEST(decimal binner 1) {
  using b = decimal_binner<1>;
  CHECK(b::bucket_size == 10);
  CHECK(b::digits2 == 4);
  CHECK(b::bin(42.123) == 4);
  CHECK(b::bin(53.9) == 5);
  CHECK(b::bin(41.02014) == 4);
  CHECK(b::bin(44.91234543) == 4);
  CHECK(b::bin(39.5) == 4);
  CHECK(b::bin(49.5) == 5);
}

TEST(decimal binner 2) {
  using b = decimal_binner<2>;
  CHECK(b::bucket_size == 100);
}
