#include "vast/json.hpp"
#include "vast/time.hpp"

namespace vast {

using std::chrono::duration_cast;

bool convert(timespan dur, double& d) {
  d = duration_cast<double_seconds>(dur).count();
  return true;
}

bool convert(timespan dur, json& j) {
  j = dur.count();
  return true;
}

bool convert(timestamp ts, double& d) {
  return convert(ts.time_since_epoch(), d);
}

bool convert(timestamp ts, json& j) {
  return convert(ts.time_since_epoch(), j);
}

} // namespace vast
