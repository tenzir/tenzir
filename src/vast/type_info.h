#ifndef VAST_TYPE_INFO_H
#define VAST_TYPE_INFO_H

#include <cstdint>
#include <string>
#include <typeinfo>
#include <type_traits>
#include "vast/fwd.h"
#include "vast/typedefs.h"
#include "vast/detail/demangle.h"
#include "vast/util/operators.h"

namespace vast {

class object;
namespace detail {
class type_manager;
} // namespace detail

class global_type_info
  : util::equality_comparable<global_type_info>,
    util::equality_comparable<global_type_info, std::type_info>,
    util::totally_ordered<global_type_info>
{
  friend detail::type_manager;
  friend bool operator<(global_type_info const& x, global_type_info const& y);
  friend bool operator==(global_type_info const& x, global_type_info const& y);
  friend bool operator==(global_type_info const& x, std::type_info const& y);

public:
  type_id id() const;

  std::string const& name() const;

  object construct() const;

  virtual bool equals(std::type_info const& ti) const = 0;

  virtual void destroy(void const* instance) const = 0;

  virtual void* create(void const* instance = nullptr) const = 0;

  virtual void serialize(void const* instance, serializer& sink) const = 0;

  virtual void deserialize(void* instance, deserializer& source) const = 0;

protected:
  global_type_info(std::string name);

private:
  type_id id_ = 0;
  std::string name_;
};

/// A concrete type info that suits most common types.
template <typename T>
class concrete_type_info : public global_type_info
{
public:
  concrete_type_info()
    : global_type_info(detail::demangle(typeid(T)))
  {
  }

  virtual bool equals(std::type_info const& ti) const override
  {
    return typeid(T) == ti;
  }

  virtual void destroy(void const* instance) const override
  {
    delete cast(instance);
  }

  virtual void* create(void const* instance) const override
  {
    return instance ? new T(*cast(instance)) : new T();
  }

  virtual void serialize(void const* instance, serializer& sink) const override
  {
    sink << *cast(instance);
  }

  virtual void deserialize(void* instance, deserializer& source) const override
  {
    source >> *cast(instance);
  }

private:
  static T const* cast(void const* ptr)
  {
    return reinterpret_cast<T const*>(ptr);
  }

  static T* cast(void* ptr)
  {
    return reinterpret_cast<T*>(ptr);
  }
};

namespace detail {
bool announce(std::type_info const& ti, global_type_info* gti);
} // namespace detail

/// Registers a type with VAST's runtime type system.
/// @tparam T The type to register.
/// @tparam TypeInfo The type information to associate with *T*.
/// @note The order of invocations determines the underlying type
/// identifier. For example:
///
///     announce<T>();
///     announce<U>();
///
/// is not the same as:
///
///     announce<U>();
///     announce<T>();
///
/// It is therefore crucial to ensure a consistent order during announcement.
template <typename T, typename TypeInfo = concrete_type_info<T>>
bool announce()
{
  static_assert(std::is_default_constructible<T>::value,
                "announced types must be default-constructible");

  return detail::announce(typeid(T), new TypeInfo);
}

/// Retrieves runtime type information about a given type.
/// @param ti A C++ type info instance.
/// @return A global_type_info instance if `T` is known and `nullptr` otherwise.
global_type_info const* global_typeid(std::type_info const& ti);

/// Retrieves runtime type information about a given type.
/// @param id A unique type identifier.
/// @return A global_type_info instance if `T` is known and `nullptr` otherwise.
global_type_info const* global_typeid(type_id id);

/// Retrieves runtime type information about a given type.
/// @param name A unique type name.
/// @return A global_type_info instance if `T` is known and `nullptr` otherwise.
global_type_info const* global_typeid(std::string const& name);

/// Retrieves runtime type information about a given type.
/// @tparam T The type to inquire information about.
/// @return A global_type_info instance if `T` is known and `nullptr` otherwise.
template <typename T>
global_type_info const* global_typeid()
{
  return global_typeid(typeid(T));
}

} // namespace vast

#endif
