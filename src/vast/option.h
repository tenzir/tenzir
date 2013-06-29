#ifndef VAST_OPTION_HPP
#define VAST_OPTION_HPP

#include <cppa/option.hpp>

namespace vast {

/// An optional value of `T` with similar semantics as `std::optional`.
template <typename T>
class option : public cppa::option<T>
{
  typedef cppa::option<T> super;
public:
#ifdef VAST_HAVE_INHERTING_CONSTRUCTORS
  using super::option;
#else
  option() = default;

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
#endif

  constexpr T const* operator->() const
  {
    return &cppa::option<T>::get();
  }
};

} // namespace vast

#endif
