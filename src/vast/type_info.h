#ifndef VAST_TYPE_INFO_H
#define VAST_TYPE_INFO_H

#include <cassert>
#include <cstdint>
#include <string>
#include <functional>
#include <typeinfo>
#include "vast/traits.h"
#include "vast/fwd.h"
#include "vast/typedefs.h"
#include "vast/detail/demangle.h"
#include "vast/util/operators.h"

namespace vast {

class object;
namespace detail {
class type_manager;
} // namespace detail

/// Enhanced RTTI.
class global_type_info
  : util::equality_comparable<global_type_info>,
    util::equality_comparable<std::type_info>,
    util::totally_ordered<global_type_info>
{
  friend bool operator==(global_type_info const& x, global_type_info const& y);
  friend bool operator==(global_type_info const& x, std::type_info const& y);
  friend bool operator<(global_type_info const& x, global_type_info const& y);

public:
  /// Retrieves the ID of this type.
  /// @return The ID of this type.
  type_id id() const;

  /// Retrieves the demangled and globally unique type name.
  /// @param The name of this type.
  std::string const& name() const;

  /// Default-constructs an object of this type.
  /// @return an object with this type.
  object create() const;

  /// Determines whether this type corresponds to C++ type information.
  /// @param ti The C++ type information.
  /// @return `true` if this type corresponds to *ti*.
  virtual bool equals(std::type_info const& ti) const = 0;

  /// Determines whether two instances of this type are equal.
  /// @param instance1 An instance of this type.
  /// @param instance2 An instance of this type.
  /// @return `true` iff `*instance1 == *instance2`.
  /// @pre Both *instance1* and *instance2* must be of this type.
  virtual bool equals(void const* instance1, void const* instance2) const = 0;

  /// Deletes an instance of this type.
  /// @param instance The instance to delete.
  virtual void destruct(void const* instance) const = 0;

  /// Default- or copy-constructs an instance of this type.
  ///
  /// @param instance If `nullptr`, this function returns a heap-allocated
  /// instance of this type and creates a copy of the given parameter
  /// otherwise.
  ///
  /// @return A heap-allocated instance of this type.
  virtual void* construct(void const* instance = nullptr) const = 0;

  /// Serializes an instance of this type.
  /// @param sink The serializer to write into.
  /// @param instance A valid instance of of this type.
  /// @pre `instance != nullptr`
  virtual void serialize(serializer& sink, void const* instance) const = 0;

  /// Deserializes an instance of this type.
  /// @param source The deserializer to read from.
  /// @param instance A valid instance of of this type.
  /// @pre `instance != nullptr`
  /// @post `*instance` holds a valid instance of this type.
  virtual void deserialize(deserializer& source, void* instance) const = 0;

protected:
  global_type_info(type_id id, std::string name);

private:
  type_id id_ = 0;
  std::string name_;
};

/// A concrete type info that suits most common types.
/// @tparam T The type to wrap.
template <typename T>
class concrete_type_info : public global_type_info
{
public:
  concrete_type_info(type_id id)
    : global_type_info(id, detail::demangle(typeid(T)))
  {
  }

  virtual bool equals(std::type_info const& ti) const override
  {
    return typeid(T) == ti;
  }

  virtual bool equals(void const* inst1, void const* inst2) const override
  {
    assert(inst1 != nullptr);
    assert(inst2 != nullptr);
    return *cast(inst1) == *cast(inst2);
  }

  virtual void destruct(void const* instance) const override
  {
    delete cast(instance);
  }

  virtual void* construct(void const* instance) const override
  {
    return instance ? new T(*cast(instance)) : new T();
  }

  virtual void serialize(serializer& sink, void const* instance) const override
  {
    assert(instance != nullptr);
    detail::save(sink, *cast(instance));
  }

  virtual void deserialize(deserializer& source, void* instance) const override
  {
    assert(instance != nullptr);
    detail::load(source, *cast(instance));
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

template <typename T>
global_type_info const* global_typeid();

namespace detail {

bool register_type(std::type_info const& ti,
                   std::function<global_type_info*(type_id)> f);

bool add_link(global_type_info const* from, std::type_info const& to);

template <typename From, typename To, typename... Ts>
struct converter
{
  static bool link()
  {
    return converter<From, To>::link() && converter<From, Ts...>::link();
  }
};

template <typename From, typename To>
struct converter<From, To>
{
  static bool link()
  {
    using BareFrom = RemovePointer<Unqualified<From>>;
    using BareTo = RemovePointer<Unqualified<To>>;
    static_assert(std::is_convertible<BareFrom*, BareTo*>::value,
                  "From* not convertible to To*.");

    auto gti = global_typeid<BareFrom>();
    if (! gti)
      throw std::logic_error("conversion requires announced type information");

    return detail::add_link(gti, typeid(BareTo));
  }
};

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
  static_assert(! std::is_same<T, object>::value,
                "objects cannot be announced");

  static_assert(std::is_default_constructible<T>::value,
                "announced types must be default-constructible");

  auto factory = [](type_id id) { return new TypeInfo(id); };
  return detail::register_type(typeid(T), factory);
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

/// Registers a convertible-to relationship for an announced type.
/// @tparam From The announced type to convert to *To*.
/// @tparam To The type to convert *From* to.
/// @return `true` iff the runtime accepted the conversion registration.
template <typename From, typename To, typename... Ts>
bool make_convertible()
{
  return detail::converter<From, To, Ts...>::link();
}

/// Checks a convertible-to relationship for an announced type.
/// @tparam From The announced type to convert to *To*.
/// @tparam To The type to convert *From* to.
/// @return `true` iff it is feasible to convert between *From* and *To*.
template <typename From, typename To>
bool is_convertible()
{
  return is_convertible(global_typeid<From>(), typeid(To));
}

/// Checks a convertible-to relationship for an announced type.
/// @param from The announced type to convert to *to*.
/// @param to The type to convert *from* to.
/// @return `true` iff it is feasible to convert between *from* and *to*.
bool is_convertible(global_type_info const* from, std::type_info const& to);

} // namespace vast

#endif
