#include <atomic>
#include <mutex>
#include <sstream>
#include <iomanip>

#include "vast/config.hpp"
#include "vast/time.hpp"
#include "vast/concept/convertible/vast/time.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/util/assert.hpp"

namespace vast {
namespace time {

constexpr char const* point::format;

namespace {
std::mutex time_zone_mutex;
std::atomic<bool> time_zone_set{false};
} // namespace <anonymous>

point now() {
  return point::clock::now();
}

duration duration::zero() {
  return duration_type::zero();
}

duration duration::min() {
  return duration_type::min();
}

duration duration::max() {
  return duration_type::max();
}

duration::rep duration::count() const {
  return duration_.count();
}

duration::rep duration::minutes() const {
  return std::chrono::duration_cast<std::chrono::minutes>(duration_).count();
}

duration::rep duration::seconds() const {
  return std::chrono::duration_cast<std::chrono::seconds>(duration_).count();
}

double duration::double_seconds() const {
  return std::chrono::duration_cast<std::chrono::duration<double>>(duration_)
    .count();
}

duration::rep duration::milliseconds() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(duration_)
    .count();
}

duration::rep duration::microseconds() const {
  return std::chrono::duration_cast<std::chrono::microseconds>(duration_)
    .count();
}

duration::rep duration::nanoseconds() const {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(duration_)
    .count();
}

duration duration::operator+() const {
  return *this;
}

duration duration::operator-() const {
  return -duration_;
}

duration& duration::operator++() {
  ++duration_;
  return *this;
}

duration duration::operator++(int) {
  return duration_++;
}

duration& duration::operator--() {
  --duration_;
  return *this;
}

duration duration::operator--(int) {
  return duration_--;
}

duration& duration::operator+=(duration const& rhs) {
  duration_ += rhs.duration_;
  return *this;
}

duration& duration::operator-=(duration const& rhs) {
  duration_ -= rhs.duration_;
  return *this;
}

duration& duration::operator*=(rep const& rhs) {
  duration_ *= rhs;
  return *this;
}

duration& duration::operator/=(rep const& rhs) {
  duration_ /= rhs;
  return *this;
}

duration operator+(duration const& x, duration const& y) {
  return x.duration_ + y.duration_;
}

duration operator-(duration const& x, duration const& y) {
  return x.duration_ - y.duration_;
}

bool operator==(duration const& x, duration const& y) {
  return x.duration_ == y.duration_;
}

bool operator<(duration const& x, duration const& y) {
  return x.duration_ < y.duration_;
}

point point::from_tm(std::tm const& tm) {
  // Because std::mktime by default uses localtime, we have to make sure to set
  // the timezone before the first call to it.
  if (!time_zone_set) {
    std::lock_guard<std::mutex> lock{time_zone_mutex};
    if (::setenv("TZ", "GMT", 1))
      throw std::runtime_error("could not set timzone variable");
    time_zone_set = true;
  }
  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    return {};
  return std::chrono::system_clock::from_time_t(t);
}

point point::utc(int year, int month, int day, int hour, int min, int sec) {
  auto t = make_tm();
  if (sec) {
    if (sec < 0 || sec > 59)
      throw std::out_of_range("point: second");
    t.tm_sec = sec;
  }
  if (min) {
    if (min < 0 || min > 59)
      throw std::out_of_range("time_at: minute");
    t.tm_min = min;
  }
  if (hour) {
    if (hour < 0 || hour > 59)
      throw std::out_of_range("time_at: hour");
    t.tm_hour = hour;
  }
  if (day) {
    if (day < 1 || day > 31)
      throw std::out_of_range("time_at: day");
    t.tm_mday = day;
  }
  if (month) {
    if (month < 1 || month > 12)
      throw std::out_of_range("time_at: month");
    t.tm_mon = month - 1;
  }
  if (year) {
    if (year < 1970)
      throw std::out_of_range("time_at: year");
    t.tm_year = year - 1900;
  }
  propagate(t);
  return std::chrono::system_clock::from_time_t(to_time_t(t));
}

point::point(duration d) : time_point_(d.duration_) {
}

point point::delta(int secs, int mins, int hours, int days, int months,
                   int years) {
  auto tm = to<std::tm>(*this);
  if (!tm)
    return {};
  if (secs)
    tm->tm_sec += secs;
  if (mins)
    tm->tm_min += mins;
  if (hours)
    tm->tm_hour += hours;
  if (days)
    tm->tm_mday += days;
  // We assume that when someone says "three month from today," it means the
  // same day just the month number advanced by three.
  if (months)
    tm->tm_mday += days_from(tm->tm_year, tm->tm_mon, months);
  if (years)
    tm->tm_mday += days_from(tm->tm_year, tm->tm_mon, years * 12);
  propagate(*tm);
  return from_tm(*tm);
}

duration point::time_since_epoch() const {
  return duration{time_point_.time_since_epoch()};
}

point& point::operator+=(duration const& rhs) {
  time_point_ += rhs.duration_;
  return *this;
}

point& point::operator-=(duration const& rhs) {
  time_point_ -= rhs.duration_;
  return *this;
}

point operator+(point const& x, duration const& y) {
  return x.time_point_ + y.duration_;
}

point operator-(point const& x, duration const& y) {
  return x.time_point_ - y.duration_;
}

point operator+(duration const& x, point const& y) {
  return x.duration_ + y.time_point_;
}

duration operator-(point const& x, point const& y) {
  return x.time_since_epoch() - y.time_since_epoch();
}

bool operator==(point const& x, point const& y) {
  return x.time_point_ == y.time_point_;
}

bool operator<(point const& x, point const& y) {
  return x.time_point_ < y.time_point_;
}

namespace {

int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

} // namespace <anonymous>

bool is_leap_year(int year) {
  // http://stackoverflow.com/a/11595914/1170277
  return (year & 3) == 0 && ((year % 25) != 0 || (year & 15) == 0);
}

int days_in_month(int year, int month) {
  VAST_ASSERT(month >= 0);
  VAST_ASSERT(month < 12);
  int days = days_per_month[month];
  // A February of a leap year has an extra day.
  if (month == 1 && is_leap_year(year))
    ++days;
  return days;
}

int days_from(int year, int month, int n) {
  VAST_ASSERT(month >= 0);
  VAST_ASSERT(month < 12);
  auto days = 0;
  if (n > 0) {
    auto current = month;
    for (auto i = 0; i < n; ++i) {
      days += days_in_month(year, current++);
      if (current == 12) {
        current %= 12;
        ++year;
      }
    }
  } else if (n < 0) {
    auto prev = month;
    for (auto i = n; i < 0; ++i) {
      prev -= prev == 0 ? -11 : 1;
      days -= days_in_month(year, prev);
      if (prev == 11)
        --year;
    }
  }
  return days;
}

time_t to_time_t(std::tm const& tm) {
  // Because std::mktime by default uses localtime, we have to make sure to set
  // the timezone before the first call to it.
  if (!time_zone_set) {
    std::lock_guard<std::mutex> lock(time_zone_mutex);
    if (::setenv("TZ", "GMT", 1))
      throw std::runtime_error("could not set timzone variable");
    time_zone_set.store(true);
  }
  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    throw std::runtime_error("point(): invalid std::tm");
  return t;
}

std::tm make_tm() {
  std::tm t;
  t.tm_sec = 0;
  t.tm_min = 0;
  t.tm_hour = 0;
  t.tm_mday = 1;
  t.tm_mon = 0;
  t.tm_year = 70;
  t.tm_wday = 0;
  t.tm_yday = 0;
  t.tm_isdst = 0;
  return t;
}

void propagate(std::tm& t) {
  VAST_ASSERT(t.tm_mon >= 0);
  VAST_ASSERT(t.tm_year >= 0);
  if (t.tm_sec >= 60) {
    t.tm_min += t.tm_sec / 60;
    t.tm_sec %= 60;
  }
  if (t.tm_min >= 60) {
    t.tm_hour += t.tm_min / 60;
    t.tm_min %= 60;
  }
  if (t.tm_hour >= 24) {
    t.tm_mday += t.tm_hour / 24;
    t.tm_hour %= 24;
  }
  if (t.tm_mday > 0) {
    auto this_month_days = days_in_month(t.tm_year, t.tm_mon);
    if (t.tm_mday > this_month_days) {
      auto days = t.tm_mday;
      t.tm_mday = 0;
      auto next_month_days = this_month_days;
      while (days > 0) {
        days -= next_month_days;
        t.tm_year += ++t.tm_mon / 12;
        t.tm_mon %= 12;
        next_month_days = days_in_month(t.tm_year, t.tm_mon);
        if (days <= next_month_days) {
          t.tm_mday += days;
          break;
        }
      }
    }
  } else {
    auto days = t.tm_mday;
    while (days <= 0) {
      t.tm_mon -= t.tm_mon == 0 ? -11 : 1;
      if (t.tm_mon == 11)
        --t.tm_year;
      auto prev_month_days = days_in_month(t.tm_year, t.tm_mon);
      if (prev_month_days + days >= 1) {
        t.tm_mday = prev_month_days + days;
        break;
      }
      days += prev_month_days;
    }
  }
}

#ifdef VAST_CLANG
std::tm to_tm(std::string const& str, char const* fmt, char const* locale)
#else
std::tm to_tm(std::string const& str, char const* fmt, char const*)
#endif
{
  auto epoch = make_tm();
  std::istringstream ss(str);
#ifdef VAST_CLANG
  if (locale)
    ss.imbue(std::locale(locale));
  ss >> std::get_time(&epoch, fmt);
#else
  // GCC does not implement std::locale correctly :-/.
  strptime(str.data(), fmt, &epoch);
#endif
  return epoch;
}

} // namespace time
} // namespace vast
