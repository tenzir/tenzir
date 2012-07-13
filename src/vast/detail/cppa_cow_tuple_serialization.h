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

template <typename Archive>
struct tuple_loader
{
  tuple_loader(Archive& ia)
    : ia(ia)
  {
  }

  template <typename T>
  void operator()(T& x)
  {
    ia >> x;
  }

  Archive& ia;
};

} // namespace detail

template <typename Archive, typename... T>
void save(Archive& oa, cppa::cow_tuple<T...> const& tuple)
{
  cppa::util::static_foreach<0, sizeof...(T)>::eval(
      tuple, detail::tuple_saver<Archive>(oa));
};

template <typename Archive, typename... T>
void load(Archive& ia, cppa::cow_tuple<T...>& tuple)
{
  cppa::cow_tuple<T...> t;

  std::tuple<T...> m;
  cppa::util::static_foreach<0, sizeof...(T)>::eval(
      m, detail::tuple_loader<Archive>(ia));

  // TODO: how to move the mutable tuple 
  assert(! "Not yet implemented");

  tuple = std::move(t);
};


} // namespace serialization
} // namespace ze

#endif
