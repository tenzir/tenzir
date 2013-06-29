#ifndef VAST_OBJECT_H
#define VAST_OBJECT_H

#include "vast/type_info.h"
#include "vast/fwd.h"

namespace vast {

class global_type_info;

/// Wraps an heap-allocated value of an announced type.
class object
{
public:
  /// Creates an object by transferring ownership of an heap-allocated pointer.
  /// @tparam T An announced type.
  /// @param x The instance to move.
  /// @return An object encapsulating *x*.
  /// @pre *x* must be a heap-allocated instance of `T`.
  template <typename T>
  static object adopt(T* x)
  {
    assert(x != nullptr);
    auto ti = global_typeid<T>();
    if (! ti)
      throw std::invalid_argument("missing type info for type T");
    return {x, ti};
  }

  /// Default-constructs an empty object.
  object() = default;

  /// Constructs an object from an announced type.
  /// @tparam T An announced type.
  /// @param x The instance to copy.
  template <typename T>
  object(T x)
  {
    auto ti = global_typeid<T>();
    if (! ti)
      throw std::invalid_argument("missing type info for type T");
    return {new T(std::move(x)), ti};
  }

  /// Copy-constructs an object by creating a deep copy.
  /// @param other The object to copy.
  object(const object& other);

  /// Move-constructs an object.
  /// @param other The object to move.
  object(object&& other) = default;

  /// Assigns on object to this instance.
  /// @param other The RHS of the assignment.
  object& operator=(object other);

  /// Constructs an object from an existing value.
  /// @param value
  /// @warning Takes ownership of *value*.
  object(void* value, global_type_info const* type);

  explicit operator bool() const;

  void const* value() const;

  void* value();

  global_type_info const* type() const;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  void* value_ = nullptr;
  global_type_info const* type_ = nullptr;
};

template <typename T>
T& get(object& o)
{
  static_assert(!std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");

  if (! (*(o.type()) == typeid(T)))
    throw std::invalid_argument("object type does not match T");

  return *reinterpret_cast<T*>(o.value());
}

template<typename T>
T const& cget(object const& o)
{
  static_assert(!std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");

  if (! (*(o.type()) == typeid(T)))
    throw std::invalid_argument("object type does not match T");

  return *reinterpret_cast<T*>(o.value());
}

} // namespace vast

#endif
