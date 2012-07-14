#include <vast/util/profiler.h>

#include <iomanip>
#include <sys/resource.h>   // getrusage
#include <sys/time.h>       // gettimeofday
#include <vast/util/logger.h>

namespace vast {
namespace util {

profiler::measurement::measurement()
{
  struct timeval begin;
  gettimeofday(&begin, 0);
  clock = static_cast<double>(begin.tv_sec) +
    static_cast<double>(begin.tv_usec) / 1000000.0;

  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);
  struct timeval& u = ru.ru_utime;
  struct timeval& s = ru.ru_stime;

  usr = static_cast<double>(u.tv_sec) +
    static_cast<double>(u.tv_usec) / 1000000.0;

  sys = static_cast<double>(s.tv_sec) +
    static_cast<double>(s.tv_usec) / 1000000.0;
}

std::ostream& operator<<(std::ostream& out, profiler::measurement const& m)
{
  out << std::fixed
    << std::setw(18) << m.clock
    << std::setw(14) << m.usr
    << std::setw(14) << m.sys;

  return out;
}

profiler::profiler(std::string const& filename, std::chrono::milliseconds ms)
  : file_(filename)
{
  LOG(verbose, core)
    << "writing profiling data every " << ms.count() << "ms to " << filename;

  assert(file_.good());
  file_.flags(std::ios::left);
  file_
    << std::setw(18) << "clock (c)"
    << std::setw(14) << "user (c)"
    << std::setw(14) << "sys (c)"
    << std::setw(18) << "clock (d)"
    << std::setw(14) << "user (d)"
    << std::setw(14) << "sys (d)"
    << std::endl;

  using namespace cppa;
  init_state = (
      on(atom("run")) >> [=]
      {
        measurement now;
        delayed_send(self, ms, atom("data"), now.clock, now.usr, now.sys);
      },
      on(atom("data"), arg_match) >> [=](double clock, double usr, double sys)
      {
        measurement now;
        file_ << now;
        delayed_send(self, ms, atom("data"), now.clock, now.usr, now.sys);

        now.clock -= clock;
        now.usr -= usr;
        now.sys -= sys;
        file_ << now << std::endl;
      },
      on(atom("shutdown")) >> [=]
      {
        file_.close();
        self->quit();
        LOG(verbose, core) << "profiler terminated";
      });
}

} // namespace util
} // namespace vast
