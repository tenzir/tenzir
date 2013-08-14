#ifndef VAST_COW_HPP
#define VAST_COW_HPP

#include <cppa/any_tuple.hpp>
#include <cppa/cow_tuple.hpp>
#include "vast/serialization.h"
#include "vast/traits.h"

namespace vast {

/// Elevates a type into a copy-on-write structure. A `cow<T>` can be
/// transparently used as a libcppa `any_tuple` or `cow_tuple<t>` and thus
/// forwarded as a message without incurring an unnecessary copy.
/// @tparam T A copy-constructible type.
template <typename T>
class cow
{
public:
  /// Default-constructs a cow instance.
  cow() = default;

  /// Constructs a `T` by forwarding the arguments to `cppa::cow_tuple`.
  /// @args The argument to forward to `cppa::cow_tuple`.
  template <
    typename... Args,
    typename = disable_if_same_or_derived<cow<T>, Args...>
  >
  cow(Args&&... args)
    : tuple_(std::forward<Args>(args)...)
  {
  }

  operator cppa::cow_tuple<T>() const
  {
    return tuple_;
  }

  operator cppa::any_tuple() const
  {
    return tuple_;
  }

private:
  friend access;

  template <typename U>
  friend U& get(cow<U>&);

  template <typename U>
  friend U const& cget(cow<U> const&);

  void serialize(serializer& sink) const
  {
    sink << cppa::get<0>(tuple_);
  }

  void deserialize(deserializer& source)
  {
    T x;
    source >> x;
    tuple_ = cppa::make_cow_tuple(std::move(x));
  }

  cppa::cow_tuple<T> tuple_;
};

template <typename T>
T& get(cow<T>& c)
{
  return cppa::get_ref<0>(c.tuple_);
}

template <typename T>
T const& cget(cow<T> const& c)
{
  return cppa::get<0>(c.tuple_);
}

} // namespace vast

#endif
