#include "vast/time.h"

#include <mutex>
#include <sstream>
#include <iomanip>
#include "vast/config.h"
#include "vast/logger.h"
#include "vast/serialization/time.h"
#include "vast/util/json.h"

namespace vast {
namespace time {

constexpr char const* point::format;

namespace {
std::mutex time_zone_mutex;
bool time_zone_set = false;
} // namespace <anonymous>

point now()
{
  return point::clock::now();
}

duration duration::operator+() const
{
  return *this;
}

duration duration::operator-() const
{
	return -duration_;
}

duration& duration::operator++()
{
  ++duration_;
  return *this;
}

duration duration::operator++(int)
{
	return duration_++;
}

duration& duration::operator--()
{
  --duration_;
  return *this;
}

duration duration::operator--(int)
{
  return duration_--;
}

duration& duration::operator+=(duration const& rhs)
{
  duration_ += rhs.duration_;
  return *this;
}

duration& duration::operator-=(duration const& rhs)
{
  duration_ -= rhs.duration_;
  return *this;
}

duration& duration::operator*=(rep const& rhs)
{
  duration_ *= rhs;
  return *this;
}

duration& duration::operator/=(rep const& rhs)
{
  duration_ /= rhs;
  return *this;
}

duration operator+(duration const& x, duration const& y)
{
  return x.duration_ + y.duration_;
}

duration operator-(duration const& x, duration const& y)
{
  return x.duration_ - y.duration_;
}

bool operator==(duration const& x, duration const& y)
{
  return x.duration_ == y.duration_;
}

bool operator<(duration const& x, duration const& y)
{
  return x.duration_ < y.duration_;
}

duration::rep duration::count() const
{
  return duration_.count();
}

void duration::serialize(serializer& sink) const
{
  sink << duration_;
}

void duration::deserialize(deserializer& source)
{
  source >> duration_;
}

trial<void> convert(duration dur, double& d)
{
  auto dd = std::chrono::duration_cast<duration::double_seconds>(dur.duration_);
  d = dd.count();
  return nothing;
}

trial<void> convert(duration dur, duration::duration_type& dt)
{
  dt = dur.duration_;
  return nothing;
}

trial<void> convert(duration dur, util::json& j)
{
  j = dur.count();
  return nothing;
}

point point::utc(int year, int month, int day, int hour, int min, int sec)
{
  auto t = make_tm();
  if (sec)
  {
    if (sec < 0 || sec > 59)
      throw std::out_of_range("point: second");
    t.tm_sec = sec;
  }
  if (min)
  {
    if (min < 0 || min > 59)
      throw std::out_of_range("time_at: minute");
    t.tm_min = min;
  }
  if (hour)
  {
    if (hour < 0 || hour > 59)
      throw std::out_of_range("time_at: hour");
    t.tm_hour = hour;
  }
  if (day)
  {
    if (day < 1 || day > 31)
      throw std::out_of_range("time_at: day");
    t.tm_mday = day;
  }
  if (month)
  {
    if (month < 1 || month > 12)
      throw std::out_of_range("time_at: month");
    t.tm_mon = month - 1;
  }
  if (year)
  {
    if (year < 1970)
      throw std::out_of_range("time_at: year");
    t.tm_year = year - 1900;
  }
  propagate(t);
  return point::clock::from_time_t(to_time_t(t));
}

point::point(duration d)
  : time_point_(d.duration_)
{
}

point& point::operator+=(duration const& rhs)
{
  time_point_ += rhs.duration_;
  return *this;
}

point& point::operator-=(duration const& rhs)
{
  time_point_ -= rhs.duration_;
  return *this;
}

point operator+(point const& x, duration const& y)
{
  return x.time_point_ + y.duration_;
}

point operator-(point const& x, duration const& y)
{
  return x.time_point_ - y.duration_;
}

point operator+(duration const& x, point const& y)
{
  return x.duration_ + y.time_point_;
}

bool operator==(point const& x, point const& y)
{
  return x.time_point_ == y.time_point_;
}

bool operator<(point const& x, point const& y)
{
  return x.time_point_ < y.time_point_;
}

point::point(std::tm const& tm)
{
  // Because std::mktime by default uses localtime, we have to make sure to set
  // the timezone before the first call to it.
  if (! time_zone_set)
  {
    std::lock_guard<std::mutex> lock(time_zone_mutex);
    if (::setenv("TZ", "GMT", 1))
      throw std::runtime_error("could not set timzone variable");
  }
  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    throw std::runtime_error("point(): invalid std::tm");
  time_point_ = clock::from_time_t(t);
}

point point::delta(int secs,
                   int mins,
                   int hours,
                   int days,
                   int months,
                   int years)
{
  auto tm = to<std::tm>(*this);
  if (! tm)
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
  return point(*tm);
}

duration point::since_epoch() const
{
  return duration(time_point_.time_since_epoch());
}

void point::serialize(serializer& sink) const
{
  sink << time_point_;
}

void point::deserialize(deserializer& source)
{
  source >> time_point_;
}

trial<void> convert(point p, double &d)
{
  d = *to<double>(duration{p.time_point_.time_since_epoch()});
  return nothing;
}

trial<void> convert(point p, std::tm& tm)
{
  auto d = std::chrono::duration_cast<point::clock::duration>(
      p.time_point_.time_since_epoch());
  auto tt = point::clock::to_time_t(point::clock::time_point(d));
  return ::gmtime_r(&tt, &tm) != nullptr
    ? nothing 
    : error{"failed to convert point"};
}

trial<void> convert(point p, util::json& j)
{
  j = p.since_epoch().count();
  return nothing;
}

trial<void> convert(point p, std::string& str, char const* fmt)
{
  auto tm = to<std::tm>(p);
  std::ostringstream ss;
#ifdef VAST_CLANG
  ss << std::put_time(&*tm, fmt);
  str = ss.str();
#else
  char buf[256];
  strftime(buf, sizeof(buf), fmt, &*tm);
  str = buf;
#endif
  return nothing;
}

namespace {

int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

} // namespace <anonymous>

bool is_leap_year(int year)
{
  // http://stackoverflow.com/a/11595914/1170277
  return (year & 3) == 0 && ((year % 25) != 0 || (year & 15) == 0);
}

int days_in_month(int year, int month)
{
  assert(month >= 0);
  assert(month < 12);
  int days = days_per_month[month];
  // A February of a leap year has an extra day.
  if (month == 1 && is_leap_year(year))
    ++days;
  return days;
}

int days_from(int year, int month, int n)
{
  assert(month >= 0);
  assert(month < 12);
  auto days = 0;
  if (n > 0)
  {
    auto current = month;
    for (auto i = 0; i < n; ++i)
    {
      days += days_in_month(year, current++);
      if (current == 12)
      {
        current %= 12;
        ++year;
      }
    }
  }
  else if (n < 0)
  {
    auto prev = month;
    for (auto i = n; i < 0; ++i)
    {
      prev -= prev == 0 ? -11 : 1;
      days -= days_in_month(year, prev);
      if (prev == 11)
        --year;
    }
  }
  return days;
}

time_t to_time_t(std::tm const& tm)
{
  // Because std::mktime by default uses localtime, we have to make sure to set
  // the timezone before the first call to it.
  if (! time_zone_set)
  {
    std::lock_guard<std::mutex> lock(time_zone_mutex);
    if (::setenv("TZ", "GMT", 1))
      throw std::runtime_error("could not set timzone variable");
  }
  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    throw std::runtime_error("point(): invalid std::tm");
  return t;
}

std::tm make_tm()
{
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

void propagate(std::tm &t)
{
  assert(t.tm_mon >= 0);
  assert(t.tm_year >= 0);
  if (t.tm_sec >= 60)
  {
    t.tm_min += t.tm_sec / 60;
    t.tm_sec %= 60;
  }
  if (t.tm_min >= 60)
  {
    t.tm_hour += t.tm_min / 60;
    t.tm_min %= 60;
  }
  if (t.tm_hour >= 24)
  {
    t.tm_mday += t.tm_hour / 24;
    t.tm_hour %= 24;
  }
  if (t.tm_mday > 0)
  {
    auto this_month_days = days_in_month(t.tm_year, t.tm_mon);
    if (t.tm_mday > this_month_days)
    {
      auto days = t.tm_mday;
      t.tm_mday = 0;
      auto next_month_days = this_month_days;
      while (days > 0)
      {
        days -= next_month_days;
        t.tm_year += ++t.tm_mon / 12;
        t.tm_mon %= 12;
        next_month_days = days_in_month(t.tm_year, t.tm_mon);
        if (days <= next_month_days)
        {
          t.tm_mday += days;
          break;
        }
      }
    }
  }
  else
  {
    auto days = t.tm_mday;
    while (days <= 0)
    {
      t.tm_mon -= t.tm_mon == 0 ? -11 : 1;
      if (t.tm_mon == 11)
        --t.tm_year;
      auto prev_month_days = days_in_month(t.tm_year, t.tm_mon);
      if (prev_month_days + days >= 1)
      {
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
