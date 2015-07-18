#include <iomanip>
#include <sstream>

#include "vast/config.h"
#include "vast/json.h"
#include "vast/concept/convertible/vast/time.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/time.h"

namespace vast {

bool convert(time::duration dur, double& d)
{
  d = dur.double_seconds();
  return true;
}

bool convert(time::duration dur, time::duration::duration_type& dt)
{
  dt = time::duration::duration_type{dur.count()};
  return true;
}

bool convert(time::duration dur, json& j)
{
  j = dur.count();
  return true;
}

bool convert(time::point p, double &d)
{
  d = p.time_since_epoch().double_seconds();
  return true;
}

bool convert(time::point p, std::tm& tm)
{
  auto td = time::duration::duration_type{p.time_since_epoch().count()};
  auto d = std::chrono::duration_cast<time::point::clock::duration>(td);
  auto tt =
    std::chrono::system_clock::to_time_t(time::point::clock::time_point(d));
  return ::gmtime_r(&tt, &tm) != nullptr;
}

bool convert(time::point p, json& j)
{
  j = p.time_since_epoch().count();
  return true;
}

bool convert(time::point p, std::string& str, char const* fmt)
{
  std::tm tm;
  if (! convert(p, tm))
    return false;
  std::ostringstream ss;
#ifdef VAST_CLANG
  ss << std::put_time(&tm, fmt);
  str = ss.str();
#else
  char buf[256];
  strftime(buf, sizeof(buf), fmt, &tm);
  str = buf;
#endif
  return true;
}

} // namespace vast
