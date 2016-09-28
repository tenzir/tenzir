#ifndef VAST_CONCEPT_PRINTABLE_STD_CHRONO_HPP
#define VAST_CONCEPT_PRINTABLE_STD_CHRONO_HPP

#include "vast/concept/printable/core.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/numeric/integral.hpp"
#include "vast/concept/printable/numeric/real.hpp"
#include "vast/detail/chrono_io.hpp"
#include "vast/time.hpp"

namespace vast {

template <typename Rep, typename Period>
struct duration_printer : printer<duration_printer<Rep, Period>> {
  using attribute = std::chrono::duration<Rep, Period>;

  template <typename Iterator>
  bool print(Iterator& out, std::chrono::duration<Rep, Period> d) const {
    static auto units = detail::date::detail::get_units<char>(
      std::chrono::duration<Rep, typename Period::type>{});
    auto p = make_printer<Rep>{} << units.data();
    return p.print(out, d.count());
  }
};

template <class Clock, class Duration>
class time_point_printer : printer<time_point_printer<Clock, Duration>> {
public:
  using attribute = std::chrono::time_point<Clock, Duration>;

  template <typename Iterator>
  bool print(Iterator& out, std::chrono::time_point<Clock, Duration> tp) const {
    // TODO: use Howard's timezone extensions for this. 
    return make_printer<Duration>{}.print(out, tp.time_since_epoch());
  }

private:
  char const* fmt_;
};

template <typename Rep, typename Period>
struct printer_registry<std::chrono::duration<Rep, Period>> {
  using type = duration_printer<Rep, Period>;
};

template <typename Clock, typename Duration>
struct printer_registry<std::chrono::time_point<Clock, Duration>> {
  using type = time_point_printer<Clock, Duration>;
};

} // namespace vast

#endif

