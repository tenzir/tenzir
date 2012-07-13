#include <vast/util/profiler.h>

#include <iomanip>
#include <sys/resource.h>   // getrusage
#include <sys/time.h>       // gettimeofday

namespace vast {
namespace util {
namespace detail {

measurement::measurement()
{
  struct timeval begin;
  gettimeofday(&begin, 0);
  clock = static_cast<double>(begin.tv_sec) +
    static_cast<double>(begin.tv_usec) / 1000000.0;

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  struct timeval& u = ru.ru_utime;
  struct timeval& s = ru.ru_stime;

  usr_time = static_cast<double>(u.tv_sec) +
    static_cast<double>(u.tv_usec) / 1000000.0;

  sys_time = static_cast<double>(s.tv_sec) +
    static_cast<double>(s.tv_usec) / 1000000.0;
}

measurement& measurement::operator+=(measurement const& rhs)
{
  clock += rhs.clock;
  usr_time += rhs.usr_time;
  sys_time += rhs.sys_time;

  return *this;
}

measurement& measurement::operator-=(measurement const& rhs)
{
  clock -= rhs.clock;
  usr_time -= rhs.usr_time;
  sys_time -= rhs.sys_time;

  return *this;
}

std::ostream& operator<<(std::ostream& out, measurement const& m)
{
  out << std::fixed
    << std::setw(18) << m.clock
    << std::setw(14) << m.usr_time
    << std::setw(14) << m.sys_time;

  return out;
}

} // namespace detail

profiler::profiler()
  : timer_(io_service_)
{
}

void profiler::init(fs::path const& filename, ze::duration interval)
{
  interval_ = interval;

  file_.open(filename);
  file_.flags(std::ios::left);
  file_
    << std::setw(18) << "clock (c)"
    << std::setw(14) << "user (c)"
    << std::setw(14) << "sys (c)"
    << std::setw(18) << "clock (d)"
    << std::setw(14) << "user (d)"
    << std::setw(14) << "sys (d)"
    << std::endl;
}

void profiler::start()
{
  assert(file_.is_open());

  timer_.expires_from_now(
      std::chrono::duration_cast<ze::clock::duration>(interval_));

  detail::measurement now;
  timer_.async_wait(
      [&, now](boost::system::error_code const& ec) { handle_timer(ec, now); });
}

void profiler::stop()
{
  timer_.cancel();
}

void profiler::handle_timer(boost::system::error_code const& ec,
                            detail::measurement const& previous)
{
  if (ec == boost::asio::error::operation_aborted)
    return;

  detail::measurement now;
  file_ << now << now - previous << std::endl;

  timer_.expires_from_now(
      std::chrono::duration_cast<ze::clock::duration>(interval_));

  timer_.async_wait(
      [&, now](boost::system::error_code const& ec) { handle_timer(ec, now); });
}

} // namespace util
} // namespace vast
