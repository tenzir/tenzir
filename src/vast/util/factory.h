#ifndef VAST_UTIL_FACTORY_H
#define VAST_UTIL_FACTORY_H

#include <map>
#include <type_traits>

namespace vast {
namespace util {

template <typename T>
struct bare_pointer_construction
{
  typedef T* result_type;

  template <typename... Args>
  static result_type construct(Args&&... args)
  {
    return new T(std::forward<Args>(args)...);
  }
};

template <typename T>
struct unique_pointer_construction
{
  typedef std::unique_ptr<T> result_type;

  template <typename... Args>
  static result_type construct(Args&&... args)
  {
    return std::make_unique<T>(std::forward<Args>(args)...);
  }
};

template <typename T>
struct value_construction
{
  using result_type = typename std::remove_pointer<T>::type;

  template <typename... Args>
  static result_type construct(Args&&... args)
  {
    return T(std::forward<Args>(args)...);
  }
};

/// A factory that constructs objects according to a given construction policy.
/// The interface is similar to a `std::function` in that `operator()`
/// constructs the object.
/// @tparam T The object type to construct.
/// @tparam Constructor The construction policy.
template <
  typename T,
  template <typename> class Constructor = unique_pointer_construction
>
struct factory
{
  typedef typename std::remove_pointer<typename std::remove_cv<T>::type>::type
    type_tag;

  typedef Constructor<type_tag> constructor;
  typedef typename constructor::result_type result_type;

  template <typename... Args>
  result_type operator()(Args&&... args) const
  {
    return constructor::construct(std::forward<Args>(args)...);
  }
};

/// A factory that constructs polymorphic objects from registered types.
/// @tparam T The type of the base class of the polymorphic type hierarchy.
/// @tparam K The index type used to construct instances with.
template <typename T, typename K>
class polymorphic_factory
{
  template <typename U>
  using factory_type = factory<U, unique_pointer_construction>;

public:
  typedef typename factory_type<T>::result_type result_type;

  template <typename Derived>
  void announce(K key)
  {
    static_assert(std::is_base_of<T, Derived>::value,
                  "invalid base class of announced type");

    factories_.emplace(std::move(key), factory_type<Derived>());
  };

  result_type construct(K const& key) const
  {
    auto i = factories_.find(key);
    if (i != factories_.end())
      return i->second();
    return {};
  }

private:
  std::map<K, std::function<result_type()>> factories_;
};


} // namespace util
} // namespace vast

#endif
