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
    typename = DisableIfSameOrDerived<cow<T>, Args...>
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

  T const& operator*() const
  {
    return read();
  }

  T const* operator->() const
  {
    return &read();
  }

  T const& read() const
  {
    return cppa::get<0>(tuple_);
  }

  T& write()
  {
    return cppa::get_ref<0>(tuple_);
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << read();
  }

  void deserialize(deserializer& source)
  {
    T x;
    source >> x;
    tuple_ = cppa::make_cow_tuple(std::move(x));
  }

  cppa::cow_tuple<T> tuple_;
};

} // namespace vast

#endif
