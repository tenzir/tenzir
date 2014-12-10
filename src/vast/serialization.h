#ifndef VAST_SERIALIZATION_H
#define VAST_SERIALIZATION_H

#include <cstdint>
#include <typeinfo>
#include <memory>
#include "vast/access.h"
#include "vast/io/coded_stream.h"
#include "vast/util/meta.h"
#include "vast/util/operators.h"

namespace vast {

/// Uniquely identifies a VAST type.
using type_id = uint64_t;

class global_type_info;

/// Abstract base class for serializers.
class serializer
{
public:
  virtual ~serializer() = default;

  /// Begins writing an instance of a given type.
  /// @param ti The type information describing the object to write afterwards.
  /// @returns `true` on success.
  /// @note The default implementation does nothing.
  virtual bool begin_instance(std::type_info const& ti);

  /// Finishes writing an object.
  /// @returns `true` on success.
  /// @note The default implementation does nothing.
  virtual bool end_instance();

  /// Begins writing a sequence.
  /// @param size The size of the sequence.
  /// @returns `true` on success.
  virtual bool begin_sequence(uint64_t size) = 0;

  /// Finishes writing a sequence.
  /// @returns `true` on success.
  /// @note The default implementation does nothing.
  virtual bool end_sequence();

  /// Writes a value.
  /// @param x The value to write.
  /// @returns `true` on success.
  virtual bool write_bool(bool x) = 0;
  virtual bool write_int8(int8_t x) = 0;
  virtual bool write_uint8(uint8_t x) = 0;
  virtual bool write_int16(int16_t x) = 0;
  virtual bool write_uint16(uint16_t x) = 0;
  virtual bool write_int32(int32_t x) = 0;
  virtual bool write_uint32(uint32_t x) = 0;
  virtual bool write_int64(int64_t x) = 0;
  virtual bool write_uint64(uint64_t x) = 0;
  virtual bool write_double(double x) = 0;

  /// Writes a string
  /// @param data The beginning of the string.
  /// @param size The number of bytes to write.
  /// @returns `true` on success.
  virtual bool write_string(char const* data, size_t size);

  /// Writes raw bytes.
  /// @param data The data to write.
  /// @param size The number of bytes to write.
  /// @returns `true` on success.
  virtual bool write_raw(void const* data, size_t size) = 0;

  /// Writes type information.
  /// @param gti The type information to write.
  /// @returns `true` on success.
  /// @note The default implementation writes out the type ID.
  /// @pre `gti != nullptr`
  virtual bool write_type(global_type_info const* gti);

protected:
  serializer() = default;
};

/// Abstract base class for deserializers.
class deserializer
{
public:
  virtual ~deserializer() = default;

  /// Begins reading an object of a given type.
  /// @param ti The type information describing the object to read afterwards.
  /// @returns `true` on success.
  /// @note The default implementation does nothing.
  virtual bool begin_instance(std::type_info const& ti);

  /// Finishes reading an object.
  /// The default implementation does nothing.
  /// @returns `true` on success.
  virtual bool end_instance();

  /// Begins reading a sequence.
  /// @param size The size of the sequence.
  /// @returns `true` on success.
  virtual bool begin_sequence(uint64_t& size) = 0;

  /// Finishes reading a sequence.
  /// The default implementation does nothing.
  /// @returns `true` on success.
  virtual bool end_sequence();

  /// Reads a value.
  /// @param x The value to read into.
  /// @returns `true` on success.
  virtual bool read_bool(bool& x) = 0;
  virtual bool read_int8(int8_t& x) = 0;
  virtual bool read_uint8(uint8_t& x) = 0;
  virtual bool read_int16(int16_t& x) = 0;
  virtual bool read_uint16(uint16_t& x) = 0;
  virtual bool read_int32(int32_t& x) = 0;
  virtual bool read_uint32(uint32_t& x) = 0;
  virtual bool read_int64(int64_t& x) = 0;
  virtual bool read_uint64(uint64_t& x) = 0;
  virtual bool read_double(double& x) = 0;

  /// Reads a string.
  /// @param data The beginning of the string.
  /// @param size The number of bytes to read.
  /// @returns `true` on success.
  virtual bool read_string(char* data, size_t size);

  /// Reads raw bytes.
  /// @param data The location to read into.
  /// @param size The number of bytes to read.
  /// @returns `true` on success.
  virtual bool read_raw(void* data, size_t size) = 0;

  /// Reads type information.
  ///
  /// @param gti The result parameter which receives either a pointer to an
  /// announced type or `nullptr` if the type identifer does not map to an
  /// announced type.
  ///
  /// @returns `true` on success.
  virtual bool read_type(global_type_info const*& gti);

protected:
  deserializer() = default;
};

/// Serializes binary objects into an input stream.
class binary_serializer : public serializer
{
public:
  /// Constructs a deserializer with an output stream.
  /// @param source The output stream to write into.
  binary_serializer(io::output_stream& sink);

  virtual bool begin_sequence(uint64_t size) override;
  virtual bool write_bool(bool x) override;
  virtual bool write_int8(int8_t x) override;
  virtual bool write_uint8(uint8_t x) override;
  virtual bool write_int16(int16_t x) override;
  virtual bool write_uint16(uint16_t x) override;
  virtual bool write_int32(int32_t x) override;
  virtual bool write_uint32(uint32_t x) override;
  virtual bool write_int64(int64_t x) override;
  virtual bool write_uint64(uint64_t x) override;
  virtual bool write_double(double x) override;
  virtual bool write_raw(void const* data, size_t size) override;
  virtual size_t bytes() const;

private:
  io::coded_output_stream sink_;
  size_t bytes_ = 0;
};

/// Deserializes binary objects from an input stream.
class binary_deserializer : public deserializer
{
public:
  /// Constructs a deserializer with an input stream.
  /// @param source The input stream to read from.
  binary_deserializer(io::input_stream& source);

  virtual bool begin_sequence(uint64_t& size) override;
  virtual bool read_bool(bool& x) override;
  virtual bool read_int8(int8_t& x) override;
  virtual bool read_uint8(uint8_t& x) override;
  virtual bool read_int16(int16_t& x) override;
  virtual bool read_uint16(uint16_t& x) override;
  virtual bool read_int32(int32_t& x) override;
  virtual bool read_uint32(uint32_t& x) override;
  virtual bool read_int64(int64_t& x) override;
  virtual bool read_uint64(uint64_t& x) override;
  virtual bool read_double(double& x) override;
  virtual bool read_raw(void* data, size_t size) override;
  virtual size_t bytes() const;

private:
  io::coded_input_stream source_;
  size_t bytes_ = 0;
};

/// Provides clean access of private class internals to the serialization
/// framework.
struct access::serializable
{
  template <typename T, typename... Ts>
  static auto make(Ts&&... xs)
    -> std::enable_if_t<! std::is_pointer<T>::value, T>
  {
    return T(std::forward<Ts>(xs)...);
  }

  template <typename T, typename... Ts>
  static auto make(Ts&&... xs)
    -> std::enable_if_t<std::is_pointer<T>::value, T>
  {
    return new std::remove_pointer_t<T>(std::forward<Ts>(xs)...);
  }

  template <typename T>
  static auto save(serializer& sink, T const& x, int)
    -> decltype(x.serialize(sink), void())
  {
    x.serialize(sink);
  }

  template <typename T>
  static auto save(serializer& sink, T const& x, long)
    -> decltype(serialize(sink, x), void())
  {
    serialize(sink, x);
  }

  template <typename T>
  static auto save(serializer& sink, T const& x)
  {
    return save(sink, x, 0);
  }

  template <typename T>
  static auto load(deserializer& source, T& x, int)
    -> decltype(x.deserialize(source), void())
  {
    x.deserialize(source);
  }

  template <typename T>
  static auto load(deserializer& source, T& x, long)
    -> decltype(deserialize(source, x), void())
  {
    deserialize(source, x);
  }

  template <typename T>
  static auto load(deserializer& source, T& x)
  {
    return load(source, x, 0);
  }
};

class object;

/// Enhanced RTTI.
class global_type_info : util::totally_ordered<global_type_info>
{
  friend bool operator==(global_type_info const& x, global_type_info const& y);
  friend bool operator==(global_type_info const& x, std::type_info const& y);
  friend bool operator!=(global_type_info const& x, std::type_info const& y);
  friend bool operator<(global_type_info const& x, global_type_info const& y);

public:
  /// Retrieves the ID of this type.
  /// @returns The ID of this type.
  type_id id() const;

  /// Default-constructs an object of this type.
  /// @returns an object with this type.
  object create() const;

  /// Determines whether this type corresponds to C++ type information.
  /// @param ti The C++ type information.
  /// @returns `true` if this type corresponds to *ti*.
  virtual bool equals(std::type_info const& ti) const = 0;

  /// Determines whether two instances of this type are equal.
  /// @param instance1 An instance of this type.
  /// @param instance2 An instance of this type.
  /// @returns `true` iff `*instance1 == *instance2`.
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
  /// @returns A heap-allocated instance of this type.
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
  global_type_info(type_id id);

private:
  type_id id_ = 0;
};

/// A concrete type info that suits most common types.
/// @tparam T The type to wrap.
template <typename T>
class concrete_type_info : public global_type_info
{
public:
  concrete_type_info(type_id id)
    : global_type_info(id)
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
    return instance ? new T(*cast(instance)) : access::serializable::make<T*>();
  }

  virtual void serialize(serializer& sink, void const* instance) const override
  {
    assert(instance != nullptr);
    access::serializable::save(sink, *cast(instance), 0);
  }

  virtual void deserialize(deserializer& source, void* instance) const override
  {
    assert(instance != nullptr);
    access::serializable::load(source, *cast(instance));
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
    using BareFrom = std::remove_pointer_t<std::decay_t<From>>;
    using BareTo = std::remove_pointer_t<std::decay_t<To>>;
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
///
/// @tparam T The type to register.
///
/// @tparam TypeInfo The type information to associate with *T*.
///
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

  auto factory = [](type_id id) { return new TypeInfo(id); };
  return detail::register_type(typeid(T), factory);
}

/// Retrieves runtime type information about a given type.
/// @param ti A C++ type info instance.
/// @returns A global_type_info instance if `T` is known and `nullptr` otherwise.
global_type_info const* global_typeid(std::type_info const& ti);

/// Retrieves runtime type information about a given type.
/// @param id A unique type identifier.
/// @returns A global_type_info instance if `T` is known and `nullptr` otherwise.
global_type_info const* global_typeid(type_id id);

/// Retrieves runtime type information about a given type.
/// @tparam T The type to inquire information about.
/// @returns A global_type_info instance if `T` is known and `nullptr` otherwise.
template <typename T>
global_type_info const* global_typeid()
{
  return global_typeid(typeid(T));
}

/// Registers a convertible-to relationship for an announced type.
/// @tparam From The announced type to convert to *To*.
/// @tparam To The type to convert *From* to.
/// @returns `true` iff the runtime accepted the conversion registration.
template <typename From, typename To, typename... Ts>
bool make_convertible()
{
  return detail::converter<From, To, Ts...>::link();
}

/// Checks a convertible-to relationship for an announced type.
/// @tparam From The announced type to convert to *To*.
/// @tparam To The type to convert *From* to.
/// @returns `true` iff it is feasible to convert between *From* and *To*.
template <typename From, typename To>
bool is_convertible()
{
  return is_convertible(global_typeid<From>(), typeid(To));
}

/// Checks a convertible-to relationship for an announced type.
/// @param from The announced type to convert to *to*.
/// @param to The type to convert *from* to.
/// @returns `true` iff it is feasible to convert between *from* and *to*.
bool is_convertible(global_type_info const* from, std::type_info const& to);

/// Announces all known builtin types.
/// This function shall be called if one uses the serialization framework.
/// @see ::announce
void announce_builtin_types();


/// Wraps a heap-allocated value of an announced type. Objects are type-erased
/// data which carry VAST type information.
class object : util::equality_comparable<object>
{
public:
  /// Creates an object by transferring ownership of an heap-allocated pointer.
  /// @tparam T An announced type.
  /// @param x The instance to move.
  /// @returns An object encapsulating *x*.
  /// @pre *x* must be a heap-allocated instance of `T`.
  template <typename T>
  static object adopt(T* x)
  {
    assert(x != nullptr);
    auto ti = global_typeid(typeid(*x));
    if (! ti)
      throw std::invalid_argument("missing type info for type T");
    return {ti, x};
  }

  /// Default-constructs an empty object.
  object() = default;

  /// Constructs an object from an announced type.
  /// @tparam T An announced type.
  /// @param x The instance to copy.
  template <typename T>
  object(T x)
  {
    type_ = global_typeid(typeid(x));
    if (! type_)
      throw std::invalid_argument("missing type info for type T");
    value_ = new T(std::move(x));
  }

  /// Constructs an object from an existing value.
  /// @param type The type of the object.
  /// @param value An heap-allocated instance of type *type*.
  /// @pre `type != nullptr && value != nullptr`
  /// @warning Takes ownership of *value*.
  object(global_type_info const* type, void* value);

  /// Copy-constructs an object by creating a deep copy.
  /// @param other The object to copy.
  object(const object& other);

  /// Move-constructs an object.
  /// @param other The object to move.
  object(object&& other) noexcept;

  /// Destructs an object.
  ~object();

  /// Assigns on object to this instance.
  /// @param other The RHS of the assignment.
  object& operator=(object other);

  explicit operator bool() const;

  friend bool operator==(object const& x, object const& y);

  /// Retrieves the type of the object.
  /// @returns The type information for this object.
  global_type_info const* type() const;

  /// Retrieves the raw object.
  /// @returns The raw `void const` pointer of this object.
  void const* value() const;

  /// Retrieves the raw object.
  /// @returns The raw `void` pointer of this object.
  void* value();

  /// Checks whether the object is convertible to a given type.
  /// @tparam T The type to check.
  /// @returns `true` iff the object is convertible to `T`.
  template <typename T>
  bool convertible_to() const
  {
    return *this && (type()->equals(typeid(T)) || is_convertible(type(), typeid(T)));
  }

  /// Relinquishes ownership of the object's contained instance.
  ///
  /// @returns A `void*` pointing to an heap-allocated pointer that the caller
  /// must now properly cast and delete.
  ///
  /// @post `! *this`
  void* release();

  /// Checks whether the object is convertible to a given type.
  /// @tparam T The type to check.
  /// @returns `true` iff the object is convertible to `T`.
  template <typename T>
  T* release_as()
  {
    return convertible_to<T>() ? reinterpret_cast<T*>(release()) : nullptr;
  }

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  global_type_info const* type_ = nullptr;
  void* value_ = nullptr;
};

/// Retrieves an object value in a type-safe manner.
/// @tparam T The type to convert the object to.
/// @returns A reference of type `T`.
template <typename T>
T& get(object& o)
{
  static_assert(! std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");
  if (! o.convertible_to<T>())
    throw std::invalid_argument("cannot convert object to requested type");
  return *reinterpret_cast<T*>(o.value());
}

template <typename T>
T const& get(object const& o)
{
  static_assert(! std::is_pointer<T>::value && !std::is_reference<T>::value,
                "T must not be a reference or a pointer type.");
  if (! o.convertible_to<T>())
    throw std::invalid_argument("cannot convert object to requested type");
  return *reinterpret_cast<T const*>(o.value());
}

//
// Basic primitives of the serialization framework.
//

/// Writes announced type instance in the form of an objet.
/// @tparam T A serializable type.
/// @param sink The serializer to write into.
/// @param x An instance of type `T`.
/// @returns `true` on success.
template <typename T>
bool write_object(serializer& sink, T const& x)
{
  if (! sink.begin_instance(typeid(x)))
    return false;
  auto gti = global_typeid(typeid(x));
  if (gti == nullptr || ! sink.write_type(gti))
    return false;
  access::serializable::save(sink, x);
  return sink.end_instance();
}

/// Reads an announced type instance in the form of an objet.
/// @tparam T A deserializable type.
/// @param source The deserializer to read from.
/// @param x An instance of type `T`.
/// @returns `true` on success.
template <typename T>
bool read_object(deserializer& source, T& x)
{
  if (! source.begin_instance(typeid(x)))
    return false;
  auto want = global_typeid(typeid(x));
  if (! want)
    return false;
  global_type_info const* got = nullptr;
  if (! (source.read_type(got) && got && *got == *want))
    return false;
  access::serializable::load(source, x);
  return source.end_instance();
}

/// Writes a serializable instance to a serializer.
/// @tparam T A serializable type.
/// @param sink The serializer to write into.
/// @param x An instance of type `T`.
/// @returns `true` on success.
template <typename T>
bool write(serializer& sink, T const& x)
{
  if (! sink.begin_instance(typeid(x)))
    return false;
  access::serializable::save(sink, x);
  return sink.end_instance();
}

/// Reads a deserializable instance from a deserializer.
/// @tparam T A deserializable type.
/// @param source The deserializer to read from.
/// @param x An instance of type `T`.
/// @returns `true` on success.
template <typename T>
bool read(deserializer& source, T& x)
{
  if (! source.begin_instance(typeid(x)))
    return false;
  access::serializable::load(source, x);
  return source.end_instance();
}

/// Serializes a serializable instance.
/// A chainable shorthand for ::write.
/// @tparam T the type of the instance to serialize.
/// @param sink The serializer to write a `T` into.
/// @param x An instance of type `T`.
/// @returns A reference to *sink*.
template <typename T>
serializer& operator<<(serializer& sink, T const& x)
{
  write(sink, x);
  return sink;
}

/// Deserializes a deserializable instance.
/// A chainable shorthand for ::read.
/// @tparam T the type of the instance to serialize.
/// @param source The deserializer to extract a `T` from.
/// @param x An instance of type `T`.
/// @returns A reference to *source*.
template <typename T>
deserializer& operator>>(deserializer& source, T& x)
{
  read(source, x);
  return source;
}

} // namespace vast

#endif
