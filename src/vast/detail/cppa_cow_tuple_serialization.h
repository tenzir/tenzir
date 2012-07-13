#ifndef VAST_DETAIL_CPPA_COW_TUPLE_SERIALIZATION_H
#define VAST_DETAIL_CPPA_COW_TUPLE_SERIALIZATION_H

#include <cppa/util/static_foreach.hpp>
#include <cppa/cow_tuple.hpp>

namespace ze {
namespace serialization {

namespace detail {

template <typename Archive>
struct tuple_saver
{
  tuple_saver(Archive& oa)
    : oa(oa)
  {
  }

  template <typename T>
  void operator()(T const& x)
  {
    oa << x;
  }

  Archive& oa;
};

} // namespace detail

template <typename Archive, typename... T>
void save(Archive& oa, cppa::cow_tuple<T...> const& tuple)
{
  cppa::util::static_foreach<0, sizeof...(T)>::eval(
      tuple,
      detail::tuple_saver<Archive>(oa));
};

// Note that there is no analogue load function, since this would require
// mutable access to the tuple elements. This is, by definition,  not possible
// without occuring a copy of the tuple.

} // namespace serialization
} // namespace ze

#endif
