#ifndef VAST_TIME_HPP
#define VAST_TIME_HPP

#include <chrono>
#include <cstdint>

namespace vast {

class json;

/// The clock that correctly represents calendar time.
using clock = std::chrono::system_clock;

/// A duration in time with nanosecond resolution.
using interval = std::chrono::duration<int64_t, std::nano>;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using timestamp = std::chrono::time_point<clock, interval>;

/// A helper type to represent fractional time stamps in type `double`.
using double_seconds = std::chrono::duration<double, std::ratio<1>>;

bool convert(interval dur, double& d);
bool convert(interval dur, json& j);

bool convert(timestamp tp, double& d);
bool convert(timestamp tp, json& j);

} // namespace vast

#endif
