#ifndef VAST_OBJECT_H
#define VAST_OBJECT_H

#include "vast/type_info.h"
#include "vast/io/fwd.h"

namespace vast {

class stable_type_info;

/// Wraps a value of an announced type.
class object
{
public:
  template <typename T>
  static object create(T x)
  {
    auto ti = stable_typeid<T>();
    if (! ti)
      throw std::invalid_argument("missing type info for type T");
    return {new T(std::move(x)), ti};
  }

  template <typename T>
  static object adopt(T* x)
  {
    auto ti = stable_typeid<T>();
    if (! ti)
      throw std::invalid_argument("missing type info for type T");
    return {x, ti};
  }

  /// Default-constructs an empty object.
  object() = default;

  /// Copy-constructs an object by creating a deep copy.
  /// @param other The object to copy.
  object(const object& other);

  /// Move-constructs an object.
  /// @param other The object to move.
  object(object&& other);

  /// Assigns on object to this instance.
  /// @param other The RHS of the assignment.
  object& operator=(object other);

  /// Constructs an object from an existing value.
  /// @param value
  /// @warning Takes ownership of *value*.
  object(void* value, stable_type_info const* type);
  
  explicit operator bool() const;

  void const* value() const;

  void* mutable_value() const;

  stable_type_info const* type() const;

private:
  friend io::access;
  void serialize(io::serializer& sink) const;
  void deserialize(io::deserializer& source);

  void* value_ = nullptr;
  stable_type_info const* type_ = nullptr;
};

template <typename T>
T& get(object& o)
{
  static_assert(!std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");

  if (!(*(o.type()) == typeid(T)))
    throw std::invalid_argument("object type does not match T");

  return *reinterpret_cast<T*>(o.mutable_value());
}

template<typename T>
T const& cget(object const& o)
{
  static_assert(!std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");

  if (!(*(o.type()) == typeid(T)))
    throw std::invalid_argument("object type does not match T");

  return *reinterpret_cast<T*>(o.value());
}

} // namespace vast

#endif
