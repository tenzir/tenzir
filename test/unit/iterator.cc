#include "test.h"
#include "vast/util/iterator.h"

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



BOOST_AUTO_TEST_CASE(simple_custom_iterator)
{
  int a[5] = { 1, 2, 3, 4, 5 };
  iterator<int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    BOOST_CHECK_EQUAL(*begin++, ++i);

  begin -= 3;
  BOOST_CHECK_EQUAL(*begin, 3);
  *begin = 42;
  BOOST_CHECK_EQUAL(*begin, 42);

  BOOST_CHECK_EQUAL(*--begin, 2);
  BOOST_CHECK_EQUAL(end - begin, 4);
  BOOST_CHECK(begin + 4 == end);

  BOOST_CHECK(! (begin == end));
  BOOST_CHECK(begin != end);
  BOOST_CHECK(begin < end);
  BOOST_CHECK(! (end < begin));
  BOOST_CHECK(begin <= end);
}

BOOST_AUTO_TEST_CASE(simple_custom_const_iterator)
{
  int a[5] = { 1, 2, 3, 4, 5 };
  iterator<const int, 5> begin{a}, end;

  int i = 0;
  while (begin != end)
    BOOST_CHECK_EQUAL(*begin++, ++i);
}
