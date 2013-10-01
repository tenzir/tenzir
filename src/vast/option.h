#ifndef VAST_OPTION_HPP
#define VAST_OPTION_HPP

#include <cppa/optional.hpp>
#include "vast/serialization.h"

namespace vast {

/// An optional value of `T` with similar semantics as `std::optional`.
template <typename T>
class option : public cppa::optional<T>
{
  using super = cppa::optional<T>;

public:
  option()
    : super(cppa::none_t())
  {
  }

  option(T x)
    : super(std::move(x))
  {
  }

  template <typename T0, typename T1, typename... Ts>
  option(T0&& x0, T1&& x1, Ts&&... args) 
    : super(std::forward<T0>(x0),
            std::forward<T1>(x1),
            std::forward<Ts>(args)...)
  {
  }

  option(const option&) = default;
  option(option&&) = default;
  option& operator=(option const&) = default;
  option& operator=(option&&) = default;

  T const* operator->() const
  {
    return &cppa::optional<T>::get();
  }

  T* operator->()
  {
    return &cppa::optional<T>::get();
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    if (this->valid())
      sink << true << this->get();
    else
      sink << false;
  }

  void deserialize(deserializer& source)
  {
    bool flag;
    source >> flag;
    if (! flag)
      return;
    T x;
    source >> x;
    *this = {std::move(x)};
  }
};

} // namespace vast

#endif
