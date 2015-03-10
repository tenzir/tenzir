#ifndef VAST_IO_SERIALIZATION_H
#define VAST_IO_SERIALIZATION_H

#include "vast/config.h"
#include "vast/concept/serializable/binary_serializer.h"
#include "vast/concept/serializable/binary_deserializer.h"
#include "vast/io/container_stream.h"
#include "vast/io/compressed_stream.h"
#include "vast/io/file_stream.h"
#include "vast/util/trial.h"

namespace vast {
namespace io {

template <
  typename... Ts,
  typename Container,
  typename Serializer = binary_serializer
>
auto archive(Container& c, Ts const&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>())
{
  auto sink = io::make_container_output_stream(c);
  Serializer s{sink};
  s.put(xs...);
  return nothing;
}

template <
  typename... Ts,
  typename Container,
  typename Deserializer = binary_deserializer
>
auto unarchive(Container const& c, Ts&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>())
{
  auto source = make_container_input_stream(c);
  Deserializer d{source};
  d.get(xs...);
  return nothing;
}

template <
  typename... Ts,
  typename Serializer = binary_serializer
>
trial<void> archive(path const& filename, Ts const&... xs)
{
  file f{filename};
  auto t = f.open(file::write_only);
  if (! t)
    return t;
  io::file_output_stream sink{f};
  Serializer s{sink};
  s.put(xs...);
  return nothing;
}

template <
  typename... Ts,
  typename Deserializer = binary_deserializer
>
trial<void> unarchive(path const& filename, Ts&... xs)
{
  file f{filename};
  auto t = f.open(file::read_only);
  if (! t)
    return t;
  io::file_input_stream source{f};
  Deserializer d{source};
  d.get(xs...);
  return nothing;
}

template <typename... Ts, typename Container>
auto compress(compression method, Container& c, Ts const&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>())
{
  auto buf = make_container_output_stream(c);
  std::unique_ptr<compressed_output_stream> out{
      make_compressed_output_stream(method, buf)};
  binary_serializer s{*out};
  s.put(xs...);
  return nothing;
}

template <typename... Ts, typename Container>
auto decompress(compression method, Container const& c, Ts&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>())
{
  auto buf = make_array_input_stream(c);
  std::unique_ptr<compressed_input_stream> in{
      make_compressed_input_stream(method, buf)};
  binary_deserializer d{*in};
  d.get(xs...);
  return nothing;
}

template <typename... Ts>
trial<void> compress(compression method, path const& filename, Ts const&... xs)
{
  file f{filename};
  auto t = f.open(file::write_only);
  if (! t)
    return t;
  io::file_output_stream sink{f};
  std::unique_ptr<compressed_output_stream> out{
      make_compressed_output_stream(method, sink)};
  binary_serializer s{*out};
  s.put(xs...);
  return nothing;
}

template <typename... Ts>
trial<void> decompress(compression method, path const& filename, Ts&... xs)
{
  file f{filename};
  auto t = f.open(file::read_only);
  if (! t)
    return t;
  io::file_input_stream source{f};
  std::unique_ptr<compressed_input_stream> in{
      make_compressed_input_stream(method, source)};
  binary_deserializer d{*in};
  d.get(xs...);
  return nothing;
}

} // namespace io
} // namespace vast

#endif
