#ifndef VAST_CONCEPT_SUPPORT_UNUSED_TYPE_HPP
#define VAST_CONCEPT_SUPPORT_UNUSED_TYPE_HPP

#include "vast/detail/operators.hpp"

namespace vast {

struct unused_type : detail::equality_comparable<unused_type>,
                     detail::integer_arithmetic<unused_type> {
  unused_type() = default;

  template <typename T>
  unused_type(T const&) {
  }

  unused_type& operator=(unused_type const&) = default;

  unused_type const& operator=(unused_type const&) const {
    return *this;
  }

  template <typename T>
  unused_type& operator=(T const&) {
    return *this;
  }

  template <typename T>
  unused_type const& operator=(T const&) const {
    return *this;
  }

  template <typename T>
  unused_type const& operator+=(T&&) const {
    return *this;
  }

  template <typename T>
  unused_type const& operator-=(T&&) const {
    return *this;
  }

  template <typename T>
  unused_type const& operator*=(T&&) const {
    return *this;
  }

  template <typename T>
  unused_type const& operator/=(T&&) const {
    return *this;
  }
};

static auto unused = unused_type{};

//
// Unary
//

inline unused_type operator-(unused_type) {
  return unused;
}

//
// Binary
//

inline bool operator==(unused_type, unused_type) {
  return true;
}

} // namespace vast

#endif
