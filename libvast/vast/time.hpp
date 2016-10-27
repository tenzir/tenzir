#ifndef VAST_TIME_HPP
#define VAST_TIME_HPP

#include <chrono>
#include <cstdint>

#include <caf/detail/scope_guard.hpp>

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

// Since ADL doesn't apply to type aliases, we must define these free functions
// either in namespace caf or namespace std. Both are poor choices, but since
// namespace caf has already a bunch of inspect overloads, we'll put 'em there.
// We should revisit this after this ticket has been addressed:
// https://github.com/actor-framework/actor-framework/issues/510.
namespace caf {

template <class Inspector>
std::enable_if_t<Inspector::reads_state, typename Inspector::result_type>
inspect(Inspector& f, vast::timestamp& t) {
  return f(t.time_since_epoch());
}

template <class Inspector>
std::enable_if_t<Inspector::writes_state, typename Inspector::result_type>
inspect(Inspector& f, vast::timestamp& t) {
  auto i = vast::interval{};
  auto guard = caf::detail::make_scope_guard([&] {
    t = vast::timestamp{i};
  });
  return f(i);
}

} // namespace caf

#endif
