#ifndef VAST_CONCEPT_PRINTABLE_VAST_TRIAL_H
#define VAST_CONCEPT_PRINTABLE_VAST_TRIAL_H

#include "vast/trial.h"
#include "vast/concept/printable/core/printer.h"
#include "vast/concept/printable/string/string.h"
#include "vast/concept/printable/vast/error.h"

namespace vast {

template <typename T>
struct trial_printer : printer<trial_printer<T>>
{
  using attribute = trial<T>;

  template <typename Iterator>
  bool print(Iterator& out, trial<T> const& t) const
  {
    static auto p = make_printer<T>{};
    static auto e = make_printer<error>{};
    return t ? p.print(out, *t) : e.print(out, t.error());
  }
};

template <T>
struct printer_registry<trial<T>, std::enable_if_t<has_printer<T>{}>>
{
  using type = trial_printer<T>;
};

} // namespace vast

#endif
