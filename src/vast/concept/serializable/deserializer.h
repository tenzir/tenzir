#ifndef VAST_CONCEPT_SERIALIZABLE_DESERIALIZER_H
#define VAST_CONCEPT_SERIALIZABLE_DESERIALIZER_H

#include <cstdint>
#include <type_traits>

namespace vast {

/// Base class for deserializers.
template <typename Derived>
class deserializer
{
public:
  template <typename T>
  friend Derived& operator>>(Derived& source, T& x)
  {
    source.get(x);
    return source;
  }

  /// Reads an arithmetic value.
  /// @param x The value to write.
  template <typename T>
  auto read(T& x)
    -> std::enable_if_t<std::is_arithmetic<T>::value>;

  /// Reads raw bytes.
  /// @param data A pointer to the destination of the read.
  /// @param size The number of bytes to read.
  void read(void* data, size_t size);

  /// Hook executed before deserializing an instance.
  /// @tparam T The type of the instance to be deserialized.
  /// @returns The version of `T`.
  template <typename T>
  uint32_t begin_instance()
  {
    return 0;
  }

  /// Hook executed after deserializing an instance.
  /// @tparam T The type of the just deserialized instance.
  template <typename T>
  void end_instance()
  {
    // nop
  }

  /// Begins reading a sequence.
  /// @returns The size of the sequence.
  uint64_t begin_sequence();

  /// Completes writing a sequence.
  void end_sequence()
  {
    // nop
  }

  /// Deserializes an instance.
  /// @param x The instance to read into.
  template <typename T>
  auto get(T& x)
    -> decltype(deserialize(std::declval<Derived&>(), x, 0))
  {
    auto version = derived()->template begin_instance<T>();
    deserialize(*derived(), x, version);
    derived()->template end_instance<T>();
  }

  template <typename T>
  auto get(T& x)
    -> decltype(deserialize(std::declval<Derived&>(), x))
  {
    derived()->template begin_instance<T>();
    deserialize(*derived(), x);
    derived()->template end_instance<T>();
  }

  template <typename T, typename... Ts>
  void get(T& x, Ts&... xs)
  {
    get(x);
    get(xs...);
  }

  /// The number of bytes read from the underlying souce.
  /// @returns The number of bytes this instance has read.
  uint64_t bytes() const;

private:
  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }
};

} // namespace vast

#endif
