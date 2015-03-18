#ifndef VAST_CONCEPT_SERIALIZABLE_SERIALIZER_H
#define VAST_CONCEPT_SERIALIZABLE_SERIALIZER_H

#include <cstdint>
#include <type_traits>

namespace vast {

/// Base class for serialziers.
template <typename Derived>
class serializer
{
public:
  template <typename T>
  friend Derived& operator<<(Derived& sink, T const& x)
  {
    sink.put(x);
    return sink;
  }

  /// Writes an arithmetic value.
  /// @param x The value to write.
  template <typename T>
  auto write(T x)
    -> std::enable_if_t<std::is_arithmetic<T>::value>;

  /// Writes raw bytes.
  /// @param data A pointer to the source of the write.
  /// @param size The number of bytes to write.
  void write(void const* data, size_t size);

  /// Hook executed before serializing an instance.
  /// @tparam T The type of the instance to be serialized.
  /// @returns The version of type `T`.
  template <typename T>
  uint32_t begin_instance()
  {
    return 0;
  }

  /// Hook executed after serializing an instance.
  /// @tparam T The type of the just serialized instance.
  template <typename T>
  void end_instance()
  {
    // nop
  }

  /// Begins writing a sequence of a given size.
  /// @param size The size of the sequence.
  void begin_sequence(uint64_t size);

  /// Finishes writing a sequence.
  void end_sequence();

  /// Serializes an instance.
  /// @param x The instance to write.
  template <typename T>
  auto put(T const& x)
    -> decltype(serialize(std::declval<Derived&>(), x, 0))
  {
    auto version = derived()->template begin_instance<T>();
    serialize(*derived(), x, version);
    derived()->template end_instance<T>();
  }

  template <typename T>
  auto put(T const& x)
    -> decltype(serialize(std::declval<Derived&>(), x))
  {
    derived()->template begin_instance<T>();
    serialize(*derived(), x);
    derived()->template end_instance<T>();
  }

  template <typename T, typename... Ts>
  void put(T const& x, Ts const&... xs)
  {
    put(x);
    put(xs...);
  }

  /// The number of bytes written into the underlying sink.
  /// @returns The number of bytes this instance has written.
  uint64_t bytes() const;

private:
  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }
};

} // namespace vast

#endif
