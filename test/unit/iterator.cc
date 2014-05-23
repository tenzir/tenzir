#include "framework/unit.h"

#include "vast/util/iterator.h"

SUITE("util")

using namespace vast;

// A simple iterator over an array.
template <typename T, size_t N>
struct iterator : util::iterator_facade<
                  iterator<T, N>,
                  std::random_access_iterator_tag,
                  T>
{
public:
  iterator() = default;

  iterator(T (&array)[N])
    : array_{array},
      i_{0}
  {
  }

private:
  friend util::iterator_access;

  void increment()
  {
    ++i_;
  }

  void decrement()
  {
    --i_;
  }

  void advance(size_t n)
  {
    i_ += n;
  }

  size_t distance_to(iterator const& other) const
  {
    return other.i_ - i_;
  }

  T& dereference() const
  {
    return *(array_ + i_);
  }

  bool equals(iterator const& other) const
  {
    return i_ == other.i_;
  }

  T* array_;
  size_t i_ = N;
};

TEST("basic custom iterator")
{
  int a[5] = { 1, 2, 3, 4, 5 };
  iterator<int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    CHECK(*begin++ == ++i);

  begin -= 3;
  CHECK(*begin == 3);
  *begin = 42;
  CHECK(*begin == 42);

  CHECK(*--begin == 2);
  CHECK((end - begin) == 4);
  CHECK((begin + 4) == end);

  CHECK(! (begin == end));
  CHECK(begin != end);
  CHECK(begin < end);
  CHECK(! (end < begin));
  CHECK(begin <= end);
}

TEST("basic custom const-iterator")
{
  int a[5] = { 1, 2, 3, 4, 5 };
  iterator<const int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    CHECK(*begin++ == ++i);
}
