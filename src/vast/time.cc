#include "vast/time.h"

#include <mutex>
#include <sstream>
#include <iomanip>
#include "vast/config.h"
#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/io/serialization.h"

namespace vast {

namespace {
std::mutex time_zone_mutex;
bool time_zone_set = false;
} // namespace <anonymous>

time_point now()
{
  return time_point::clock::now();
}

time_range time_range::operator+() const
{
  return *this;
}

time_range time_range::operator-() const
{
	return -duration_;
}

time_range& time_range::operator++()
{
  ++duration_;
  return *this;
}

time_range time_range::operator++(int)
{
	return duration_++;
}

time_range& time_range::operator--()
{
  --duration_;
  return *this;
}

time_range time_range::operator--(int)
{
  return duration_--;
}

time_range& time_range::operator+=(time_range const& rhs)
{
  duration_ += rhs.duration_;
  return *this;
}

time_range& time_range::operator-=(time_range const& rhs)
{
  duration_ -= rhs.duration_;
  return *this;
}

time_range& time_range::operator*=(rep const& rhs)
{
  duration_ *= rhs;
  return *this;
}

time_range& time_range::operator/=(rep const& rhs)
{
  duration_ /= rhs;
  return *this;
}

time_range operator+(time_range const& x, time_range const& y)
{
  return x.duration_ + y.duration_;
}

time_range operator-(time_range const& x, time_range const& y)
{
  return x.duration_ - y.duration_;
}

bool operator==(time_range const& x, time_range const& y)
{
  return x.duration_ == y.duration_;
}

bool operator<(time_range const& x, time_range const& y)
{
  return x.duration_ < y.duration_;
}

double time_range::to_double() const
{
  return std::chrono::duration_cast<double_seconds>(duration_).count();
}

time_range::duration_type time_range::to_duration() const
{
  return duration_;
}

time_range::rep time_range::count() const
{
  return duration_.count();
}

void time_range::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);
  sink << duration_;
}

void time_range::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  source >> duration_;
  VAST_LEAVE(VAST_THIS);
}

std::string to_string(time_range r)
{
  return std::to_string(r.to_double()) + "s";
}

std::ostream& operator<<(std::ostream& out, time_range r)
{
  out << to_string(r);
  return out;
}

time_point::time_point(std::string const& str, char const* fmt, char const* locale)
{
  auto epoch = detail::make_tm();
  std::istringstream ss(str);
#ifdef VAST_CLANG
  if (locale)
    ss.imbue(std::locale(locale));
  ss >> std::get_time(&epoch, fmt);
#else
  // GCC does not implement std::locale correctly :-/.
  strptime(str.data(), fmt, &epoch);
#endif
  time_point_ = clock::from_time_t(detail::to_time_t(epoch));
}

time_point::time_point(int year,
                       int month,
                       int day,
                       int hour,
                       int min,
                       int sec)
{
  auto t = detail::make_tm();
  if (sec)
  {
    if (sec < 0 || sec > 59)
      throw error::out_of_range("time_point: second");

    t.tm_sec = sec;
  }
  if (min)
  {
    if (min < 0 || min > 59)
      throw error::out_of_range("time_at: minute");

    t.tm_min = min;
  }
  if (hour)
  {
    if (hour < 0 || hour > 59)
      throw error::out_of_range("time_at: hour");

    t.tm_hour = hour;
  }
  if (day)
  {
    if (day < 1 || day > 31)
      throw error::out_of_range("time_at: day");

    t.tm_mday = day;
  }
  if (month)
  {
    if (month < 1 || month > 12)
      throw error::out_of_range("time_at: month");

    t.tm_mon = month - 1;
  }
  if (year)
  {
    if (year < 1970)
      throw error::out_of_range("time_at: year");

    t.tm_year = year - 1900;
  }
  detail::propagate(t);
  time_point_ = clock::from_time_t(detail::to_time_t(t));
}

time_point::time_point(time_range range)
  : time_point_(range.duration_)
{
}

time_point& time_point::operator+=(time_range const& rhs)
{
  time_point_ += rhs.duration_;
  return *this;
}

time_point& time_point::operator-=(time_range const& rhs)
{
  time_point_ -= rhs.duration_;
  return *this;
}

time_point operator+(time_point const& x, time_range const& y)
{
  return x.time_point_ + y.duration_;
}

time_point operator-(time_point const& x, time_range const& y)
{
  return x.time_point_ - y.duration_;
}

time_point operator+(time_range const& x, time_point const& y)
{
  return x.duration_ + y.time_point_;
}

bool operator==(time_point const& x, time_point const& y)
{
  return x.time_point_ == y.time_point_;
}

bool operator<(time_point const& x, time_point const& y)
{
  return x.time_point_ < y.time_point_;
}

time_point::time_point(std::tm const& tm)
{
  // Because std::mktime by default uses localtime, we have to make sure to set
  // the timezone before the first call to it.
  if (! time_zone_set)
  {
    std::lock_guard<std::mutex> lock(time_zone_mutex);
    if (::setenv("TZ", "GMT", 1))
      throw exception("could not set timzone variable");
  }

  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    throw exception("time_point(): invalid std::tm");

  time_point_ = clock::from_time_t(t);
}

time_point time_point::delta(int secs,
                             int mins,
                             int hours,
                             int days,
                             int months,
                             int years)
{
  auto tm = to_tm();

  if (secs)
    tm.tm_sec += secs;
  if (mins)
    tm.tm_min += mins;
  if (hours)
    tm.tm_hour += hours;
  if (days)
    tm.tm_mday += days;

  // We assume that when someone says "three month from today," it means the
  // same day just the month number advanced by three.
  if (months)
    tm.tm_mday += detail::days_from(tm.tm_year, tm.tm_mon, months);
  if (years)
    tm.tm_mday += detail::days_from(tm.tm_year, tm.tm_mon, years * 12);

  detail::propagate(tm);

  return time_point(tm);
}

time_range time_point::since_epoch() const
{
  return time_range(time_point_.time_since_epoch());
}

double time_point::to_double() const
{
  return time_range(time_point_.time_since_epoch()).to_double();
}

std::tm time_point::to_tm() const
{
  auto d = std::chrono::duration_cast<clock::duration>(
      time_point_.time_since_epoch());

  auto tt = clock::to_time_t(clock::time_point(d));
  std::tm t;
  ::gmtime_r(&tt, &t);
  return t;
}

void time_point::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);;
  sink << time_point_;
}

void time_point::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  source >> time_point_;
  VAST_LEAVE(VAST_THIS);
}

std::string to_string(time_point p)
{
  auto tm = p.to_tm();
  std::ostringstream ss;
#ifdef VAST_CLANG
  ss << std::put_time(&tm, time_point::format);
  return ss.str();
#else
  char buf[256];
  strftime(buf, sizeof(buf), time_point::format, &tm);
  return buf;
#endif
}

std::ostream& operator<<(std::ostream& out, time_point t)
{
  out << to_string(t);
  return out;
}


namespace detail {

static int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

bool is_leap_year(int year)
{
  // http://stackoverflow.com/a/11595914/1170277
  return (year & 3) == 0 && ((year % 25) != 0 || (year & 15) == 0);
}

int days_in_month(int year, int month)
{
  assert(month >= 0);
  assert(month < 12);

  int days = month_days[month];

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
      throw exception("could not set timzone variable");
  }

  auto t = std::mktime(const_cast<std::tm*>(&tm));
  if (t == -1)
    throw exception("time_point(): invalid std::tm");

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
    auto this_month_days = detail::days_in_month(t.tm_year, t.tm_mon);
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

        next_month_days = detail::days_in_month(t.tm_year, t.tm_mon);
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

      auto prev_month_days = detail::days_in_month(t.tm_year, t.tm_mon);
      if (prev_month_days + days >= 1)
      {
        t.tm_mday = prev_month_days + days;
        break;
      }

      days += prev_month_days;
    }
  }
}

} // namespace detail

} // namespace vast
