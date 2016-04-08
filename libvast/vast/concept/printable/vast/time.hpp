#ifndef VAST_CONCEPT_PRINTABLE_VAST_TIME_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_TIME_HPP

#include "vast/address.hpp"
#include "vast/concept/convertible/vast/time.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/concept/printable/numeric/real.hpp"

namespace vast {

template <typename Rep, typename Period>
struct time_duration_printer : printer<time_duration_printer<Rep, Period>> {
  using attribute = std::chrono::duration<Rep, Period>;

  static constexpr auto adaptive = true; // TODO: make configurable

  template <typename Iterator>
  bool print(Iterator& out, std::chrono::duration<Rep, Period> d) const {
    using namespace printers;
    if (adaptive) {
      auto cnt = d.count();
      if (cnt > 1000000000)
        return detail::print_numeric(out, cnt / 1000000000)
               && any.print(out, '.')
               && detail::print_numeric(out, (cnt % 1000000000) / 10000000)
               && any.print(out, 's');
      if (cnt > 1000000)
        return detail::print_numeric(out, cnt / 1000000) && any.print(out, '.')
               && detail::print_numeric(out, (cnt % 1000000) / 10000)
               && str.print(out, "ms");
      if (cnt > 1000)
        return detail::print_numeric(out, cnt / 1000) && str.print(out, "us");
      return detail::print_numeric(out, cnt) && str.print(out, "ns");
    }
    auto secs = time::duration_cast<time::double_seconds>(d);
    return printers::real.print(out, secs.count()) && any.print(out, 's');
  }

  template <typename Iterator>
  bool print(Iterator& out, time::duration d) const {
    return print(out, time::nanoseconds(d.count()));
  }
};

class time_point_printer : printer<time_point_printer> {
public:
  using attribute = time::point;

  time_point_printer(char const* fmt = time::point::format) : fmt_{fmt} {
  }

  template <typename Iterator>
  bool print(Iterator& out, time::point const& tp) const {
    // TODO: avoid going through convert(..).
    std::string str;
    return convert(tp, str, fmt_) && printers::str.print(out, str);
  }

private:
  char const* fmt_;
};

template <typename Rep, typename Period>
struct printer_registry<std::chrono::duration<Rep, Period>> {
  using type = time_duration_printer<Rep, Period>;
};

template <>
struct printer_registry<time::duration> {
  using type =
    time_duration_printer<
      time::nanoseconds::rep,
      time::nanoseconds::period
    >;
};

template <>
struct printer_registry<time::point> {
  using type = time_point_printer;
};

} // namespace vast

#endif
