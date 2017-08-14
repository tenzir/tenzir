#include <sstream>

#include <date.h>

#define SUITE date
#include "test.hpp"

// These are just very rough functionality tests, no need duplicate Howard's
// test suite in detail.

using namespace std::chrono;
using namespace date;
using namespace vast;

// Quick hack until we have added a to_string overload for streamable types.
template <class T>
auto to_string(T&& x)
-> decltype(std::declval<std::stringstream&>() << x, std::string()) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}

TEST(date) {
  auto d = aug / 12 / 2012;
  CHECK_EQUAL(to_string(d), "2012-08-12");
}

TEST(time of day) {
  auto t = make_time(hours{11}, minutes{55}, seconds{4}, pm);
  CHECK_EQUAL(to_string(t), "11:55:04pm");
  t.make24();
  CHECK_EQUAL(to_string(t), "23:55:04");
  // These don't wrap around. Up to the user to ensure reasonable values.
  auto later = make_time(t.to_duration() + hours(2));
  CHECK_EQUAL(later.hours(), hours(25));
  CHECK_EQUAL(later.minutes(), minutes(55));
  CHECK_EQUAL(later.seconds(), seconds(4));
}
