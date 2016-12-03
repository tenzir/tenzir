#ifndef VAST_TIME_HPP
#define VAST_TIME_HPP

#include <chrono>
#include <cstdint>

namespace vast {

class json;

/// A duration in time with nanosecond resolution.
using timespan = std::chrono::duration<int64_t, std::nano>;

/// An absolute point in time with nanosecond resolution. It is capable to
/// represent +/- 292 years around the UNIX epoch.
using timestamp = std::chrono::time_point<std::chrono::system_clock, timespan>;

/// A helper type to represent fractional time stamps in type `double`.
using double_seconds = std::chrono::duration<double, std::ratio<1>>;

bool convert(timespan dur, double& d);
bool convert(timespan dur, json& j);

bool convert(timestamp tp, double& d);
bool convert(timestamp tp, json& j);

} // namespace vast

#endif
