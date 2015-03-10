#ifndef VAST_CONCEPT_SERIALIZABLE_CAF_ADAPTERS_H
#define VAST_CONCEPT_SERIALIZABLE_CAF_ADAPTERS_H

#include <caf/serializer.hpp>
#include <caf/deserializer.hpp>
#include <caf/detail/uniform_type_info_map.hpp>

#include "vast/die.h"
#include "vast/concept/serializable/deserializer.h"
#include "vast/concept/serializable/serializer.h"
#include "vast/concept/serializable/std/string.h"

namespace vast {

/// Wraps a VAST serializer into a CAF serializer.
template <typename Serializer>
class vast_to_caf_serializer : public caf::serializer
{
  class writer : public caf::static_visitor<>
  {
  public:
    writer(Serializer& sink) : sink_{sink} {}

    template <typename T>
    auto operator()(T x) const
      -> std::enable_if_t<std::is_arithmetic<T>::value>
    {
      sink_.write(x);
    }

    void operator()(long double) const
    {
      die("type error"); // Not used in VAST.
    }

    template <typename T>
    auto operator()(T) const
      -> std::enable_if_t<! std::is_arithmetic<T>::value>
    {
      die("type error"); // Not used in VAST.
    }

  private:
    Serializer& sink_;
  };

public:
  vast_to_caf_serializer(Serializer& sink)
    : sink_{sink}
  {
  }

  void begin_sequence(size_t size) override
  {
    sink_.begin_sequence(size);
  }

  void end_sequence() override
  {
    sink_.end_sequence();
  }

  void begin_object(caf::uniform_type_info const* uti) override
  {
    serialize(sink_, uti->name());
  }

  void end_object() override
  {
    // nop
  }

  void write_value(caf::primitive_variant const& value) override
  {
    writer w{sink_};
    caf::apply_visitor(w, value);
  }

  void write_raw(size_t num_bytes, const void* data) override
  {
    sink_.write(data, num_bytes);
  }

private:
  Serializer& sink_;
};

/// Wraps a VAST deserializer into a CAF deserializer.
template <typename Deserializer>
class vast_to_caf_deserializer : public caf::deserializer
{
  class reader : public caf::static_visitor<>
  {
  public:
    reader(Deserializer& source) : source_{source} {}

    template <typename T>
    auto operator()(T& x) const
      -> std::enable_if_t<std::is_arithmetic<T>::value>
    {
      source_.read(x);
    }

    void operator()(long double&) const
    {
      die("type error"); // Not used in VAST.
    }

    template <typename T>
    auto operator()(T&) const
      -> std::enable_if_t<! std::is_arithmetic<T>::value>
    {
      die("type error"); // Not used in VAST.
    }

  private:
    Deserializer& source_;
  };

public:
  vast_to_caf_deserializer(Deserializer& source)
    : source_{source}
  {
  }

  size_t begin_sequence() override
  {
    return source_.begin_sequence();
  }

  void end_sequence() override
  {
    source_.end_sequence();
  }

  caf::uniform_type_info const* begin_object() override
  {
    std::string name;
    deserialize(source_, name);
    auto uti_map = caf::detail::singletons::get_uniform_type_info_map();
    auto uti = uti_map->by_uniform_name(name);
    if (uti == nullptr)
      die("no type information available");
    return uti;
  }

  void end_object() override
  {
    // nop
  }

  void read_value(caf::primitive_variant& value) override
  {
    reader r{source_};
    caf::apply_visitor(r, value);
  }

  void read_raw(size_t num_bytes, void* data) override
  {
    source_.read(data, num_bytes);
  }

private:
  Deserializer& source_;
};

/// Wraps a CAF serializer to model a VAST serializer.
class caf_to_vast_serializer : public serializer<caf_to_vast_serializer>
{
public:
  caf_to_vast_serializer(caf::serializer& sink)
    : sink_{sink}
  {
  }

  void begin_sequence(uint64_t size)
  {
    sink_.begin_sequence(size);
  }

  void end_sequence() { }

  template <typename T>
  auto write(T x)
    -> std::enable_if_t<std::is_arithmetic<T>::value>
  {
    sink_.write(x);
  }

  void write(void const* data, size_t size)
  {
    sink_.write_raw(size, data);
  }

private:
  caf::serializer& sink_;
};

/// Wraps a CAF serializer to model a VAST serializer.
class caf_to_vast_deserializer : public deserializer<caf_to_vast_deserializer>
{
public:
  caf_to_vast_deserializer(caf::deserializer& source)
    : source_{source}
  {
  }

  uint64_t begin_sequence()
  {
    return source_.begin_sequence();
  }

  void end_sequence() { }

  template <typename T>
  auto read(T& x)
    -> std::enable_if_t<std::is_arithmetic<T>::value>
  {
    x = source_.read<T>();
  }

  void read(void* data, size_t size)
  {
    source_.read_raw(size, data);
  }

private:
  caf::deserializer& source_;
};

} // namespace vast

#endif
