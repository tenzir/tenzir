#include "vast/chunk.hpp"

#include "test.hpp"

using namespace vast;

TEST(chunk construction) {
  auto x = chunk::make(100);
  CHECK_EQUAL(x->size(), 100u);
}

TEST(chunk slicing) {
  char buf[100];
  auto i = 42;
  auto deleter = [&](char*, size_t) { i = 0; };
  auto x = chunk::make(sizeof(buf), buf, deleter);
  auto y = x->slice(50);
  auto z = y->slice(40, 5);
  CHECK_EQUAL(y->size(), 50u);
  CHECK_EQUAL(z->size(), 5u);
  x = y = nullptr;
  CHECK_EQUAL(z->size(), 5u);
  CHECK_EQUAL(i, 42);
  z = nullptr;
  CHECK_EQUAL(i, 0);
}
