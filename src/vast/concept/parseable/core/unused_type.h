#ifndef VAST_CONCEPT_PARSEABLE_CORE_UNUSED_TYPE_H
#define VAST_CONCEPT_PARSEABLE_CORE_UNUSED_TYPE_H

#include "vast/util/operators.h"

namespace vast {

struct unused_type : util::equality_comparable<unused_type>,
                     util::integer_arithmetic<unused_type>
{
  unused_type() = default;

  template <typename T>
  unused_type(T const&)
  {
  }

  unused_type& operator=(unused_type const&) = default;

  unused_type const& operator=(unused_type const&) const
  {
    return *this;
  }

  template <typename T>
  unused_type& operator=(T const&)
  {
    return *this;
  }

  template <typename T>
  unused_type const& operator=(T const&) const
  {
    return *this;
  }

  template <typename T>
  unused_type const& operator+=(T&&) const
  {
    return *this;
  }

  template <typename T>
  unused_type const& operator-=(T&&) const
  {
    return *this;
  }

  template <typename T>
  unused_type const& operator*=(T&&) const
  {
    return *this;
  }

  template <typename T>
  unused_type const& operator/=(T&&) const
  {
    return *this;
  }
};

static auto unused = unused_type{};

//
// Unary
//

inline unused_type operator-(unused_type)
{
  return unused;
}

//
// Binary
//

inline bool operator==(unused_type, unused_type)
{
  return true;
}

} // namespace vast

#endif
