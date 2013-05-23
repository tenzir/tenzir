#ifndef VAST_TIME_H
#define VAST_TIME_H

#include <chrono>
#include <string>
#include "vast/io/fwd.h"
#include "vast/util/operators.h"

namespace vast {

// Forward declaration
class time_point;

/// Constructs a time point with the current time.
time_point now();

/// A time duration.
class time_range : util::totally_ordered<time_range>
{
  friend class time_point;
  typedef std::chrono::duration<double, std::ratio<1>> double_seconds;

public:
  typedef int64_t rep;
  typedef std::chrono::duration<rep, std::nano> duration_type;

  /// Constructs a nanosecond time range.
  /// @param ns The number of nanoseconds.
  /// @return A time range of *ns* nanoseconds.
  template <typename T>
  static time_range nanoseconds(T ns)
  {
    return std::chrono::nanoseconds(ns);
  }

  /// Constructs a microsecond time range.
  /// @param us The number of microseconds.
  /// @return A time range of *us* microseconds.
  template <typename T>
  static time_range microseconds(T us)
  {
    return std::chrono::microseconds(us);
  }

  /// Constructs a millisecond time range.
  /// @param ms The number of milliseconds.
  /// @return A time range of *ms* milliseconds.
  template <typename T>
  static time_range milliseconds(T ms)
  {
    return std::chrono::milliseconds(ms);
  }

  /// Constructs a second time range.
  /// @param s The number of seconds.
  /// @return A time range of *s* seconds.
  template <typename T>
  static time_range seconds(T s)
  {
    return std::chrono::seconds(s);
  }

  /// Constructs a second time range.
  /// @param f The number of fractional seconds.
  /// @return A time range of *f* fractional seconds.
  static time_range fractional(double f)
  {
    return double_seconds(f);
  }

  /// Constructs a minute time range.
  /// @param m The number of minutes.
  /// @return A time range of *m* minutes.
  template <typename T>
  static time_range minutes(T m)
  {
    return std::chrono::minutes(m);
  }

  /// Constructs a hour time range.
  /// @param h The number of hours.
  /// @return A time range of *h* hours.
  template <typename T>
  static time_range hours(T h)
  {
    return std::chrono::hours(h);
  }

  /// Constructs a zero time range.
  time_range() = default;

  /// Constructs a time range from a `std::chrono::duration`.
  /// @param dur The `std::chrono::duration`.
  template <typename Rep, typename Period>
  time_range(std::chrono::duration<Rep, Period> dur)
    : duration_(std::chrono::duration_cast<duration_type>(dur))
  {
  }

  // Arithmetic operators.
  time_range operator+() const;
  time_range operator-() const;
  time_range& operator++();
  time_range operator++(int);
  time_range& operator--();
  time_range operator--(int);
  time_range& operator+=(time_range const& r);
  time_range& operator-=(time_range const& r);
  time_range& operator*=(rep const& r);
  time_range& operator/=(rep const& r);
  friend time_range operator+(time_range const& x, time_range const& y);
  friend time_range operator-(time_range const& x, time_range const& y);
  friend time_point operator+(time_point const& x, time_range const& y);
  friend time_point operator-(time_point const& x, time_range const& y);
  friend time_point operator+(time_range const& x, time_point const& y);

  // Relational operators.
  friend bool operator==(time_range const& x, time_range const& y);
  friend bool operator<(time_range const& x, time_range const& y);

  /// Returns a `std::chrono::duration` from the range.
  /// @return A `std::chrono::duration`.
  duration_type to_duration() const;

  /// Converts the time range to a `double`.
  double to_double() const;

  /// Lifts `std::chrono::duration::count`.
  rep count() const;

private:
  friend io::access;
  void serialize(io::serializer& sink);
  void deserialize(io::deserializer& source);

  duration_type duration_{0};
};

std::string to_string(time_range r);
std::ostream& operator<<(std::ostream& out, time_range r);

/// An absolute point in time having UTC time zone.
class time_point : util::totally_ordered<time_point>
{
public:
  typedef time_range::duration_type duration;
  typedef std::chrono::system_clock clock;
  typedef std::chrono::time_point<clock, duration> time_point_type;

  // The default format string used to convert time points into calendar types.
  // It has the form `YYYY-MM-DD HH:MM:SS`.
  static constexpr char const* format = "%Y-%m-%d+%H:%M:%S";

  /// Constructs a time point with the UNIX epoch.
  time_point() = default;

  /// Constructs a time point from a `std::chrono::time_point`.
  template <typename Clock, typename Duration>
  time_point(std::chrono::time_point<Clock, Duration> tp)
    : time_point_(tp.time_since_epoch())
  {
  }

  /// Creates a time point from a time range.
  /// @param range The time range.
  time_point(time_range range);

  /// Constructs a time point from a `std:tm` structure.
  explicit time_point(std::tm const& tm);

  /// Constructs a time point from a given format string.
  /// @param str The string to parse according to *fmt*.
  /// @param fmt The format string.
  /// @param locale The locale for *fmt*.
  time_point(std::string const& str,
             char const* fmt = format,
             char const* locale = nullptr);

  /// Constructs a UTC time point.
  explicit time_point(int year,
                      int month = 0,
                      int day = 0,
                      int hour = 0,
                      int min = 0,
                      int sec = 0);

  // Arithmetic operators.
  time_point& operator+=(time_range const& r);
  time_point& operator-=(time_range const& r);
  friend time_point operator+(time_point const& x, time_range const& y);
  friend time_point operator-(time_point const& x, time_range const& y);
  friend time_point operator+(time_range const& x, time_point const& y);

  // Relational operators.
  friend bool operator==(time_point const& x, time_point const& y);
  friend bool operator<(time_point const& x, time_point const& y);

  /// Computes the relative time with respect to this time point. Underflows
  /// and overflows behave intuitively for seconds, minutes, hours, and days.
  /// For months, a delta of *x* months means the same day of the current month
  /// shifted by *x* months. That is, *x* represents the number of days of the
  /// respective months, as opposed to always 30 days. Year calculations follow
  /// the style.
  ///
  /// @param secs The seconds to add/subtract.
  ///
  /// @param mins The minutes to add/subtract.
  ///
  /// @param hours The hours to add/subtract.
  ///
  /// @param days The days to to add/subtract.
  ///
  /// @param months The months to to add/subtract.
  ///
  /// @param years The years to to add/subtract.
  ///
  /// @return The relative time from now according to the unit specifications.
  time_point delta(int secs = 0,
                   int mins = 0,
                   int hours = 0,
                   int days = 0,
                   int months = 0,
                   int years = 0);

  /// Returns a time range representing the duration since the UNIX epoch.
  time_range since_epoch() const;

  /// Converts a time point to a `double`.
  /// @return The time point as fractional timestamp since the UNIX epoch.
  double to_double() const;

  /// Converts the time point to a `std::tm` structure.
  std::tm to_tm() const;

private:
  friend io::access;
  void serialize(io::serializer& sink);
  void deserialize(io::deserializer& source);

  time_point_type time_point_;
};

std::string to_string(time_point p);
std::ostream& operator<<(std::ostream& out, time_point p);

namespace detail {

/// Determines whether a given year is a leap year.
/// @param year The year to check.
/// @return `true` iff *year* is a leap year.
bool is_leap_year(int year);

/// Retrieves the number days in a given month of a particular year.
/// @param year The year.
/// @param month The month.
/// @return The number of days that *month* has in *year*.
int days_in_month(int year, int month);

/// Computes the number of days relative to a given year and month.
///
/// @param year The starting year.
///
/// @param month The starting month.
///
/// @param n The number of months to convert to days relative to *year* and
/// *month*.
///
/// @return The number months in days since *year*/*month*.
int days_from(int year, int month, int n);

time_t to_time_t(std::tm const& t);

/// Creates a new `std::tm` initialized to the 1970 epoch.
/// @return The `std::tm`.
std::tm make_tm();

/// Propagates underflowed and overflowed values up to the next higher unit.
void propagate(std::tm &t);

} // namespace detail

} // namespace vast

#endif
