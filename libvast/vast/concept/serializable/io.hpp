#ifndef VAST_CONCEPT_SERIALIZABLE_IO_HPP
#define VAST_CONCEPT_SERIALIZABLE_IO_HPP

#include "vast/concept/serializable/binary_serializer.hpp"
#include "vast/concept/serializable/binary_deserializer.hpp"
#include "vast/concept/printable/vast/filesystem.hpp"
#include "vast/io/container_stream.hpp"
#include "vast/io/compressed_stream.hpp"
#include "vast/io/file_stream.hpp"
#include "vast/trial.hpp"

namespace vast {

template <
  typename... Ts,
  typename Container,
  typename Serializer = binary_serializer
>
auto save(Container& c, Ts const&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>()) {
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
auto load(Container const& c, Ts&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>()) {
  auto source = io::make_container_input_stream(c);
  Deserializer d{source};
  d.get(xs...);
  return nothing;
}

template <
  typename... Ts,
  typename Serializer = binary_serializer
>
trial<void> save(path const& filename, Ts const&... xs) {
  io::file_output_stream sink{filename};
  Serializer s{sink};
  s.put(xs...);
  return nothing;
}

template <
  typename... Ts,
  typename Deserializer = binary_deserializer
>
trial<void> load(path const& filename, Ts&... xs) {
  if (! exists(filename))
    return error{"no such file: ", filename};
  io::file_input_stream source{filename};
  Deserializer d{source};
  d.get(xs...);
  return nothing;
}

template <typename... Ts, typename Container>
auto compress(Container& c, io::compression method, Ts const&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>()) {
  auto buf = io::make_container_output_stream(c);
  auto out = io::make_compressed_output_stream(method, buf);
  binary_serializer s{*out};
  s.put(xs...);
  return nothing;
}

template <typename... Ts, typename Container>
auto decompress(Container const& c, io::compression method, Ts&... xs)
  -> decltype(detail::is_byte_container<Container>(), trial<void>()) {
  auto buf = io::make_array_input_stream(c);
  auto in = io::make_compressed_input_stream(method, buf);
  binary_deserializer d{*in};
  d.get(xs...);
  return nothing;
}

template <typename... Ts>
trial<void> compress(path const& filename, io::compression method,
                     Ts const&... xs) {
  io::file_output_stream sink{filename};
  auto out = io::make_compressed_output_stream(method, sink);
  binary_serializer s{*out};
  s.put(xs...);
  return nothing;
}

template <typename... Ts>
trial<void> decompress(path const& filename, io::compression method,
                       Ts&... xs) {
  if (!exists(filename))
    return error{"no such file: ", filename};
  io::file_input_stream source{filename};
  auto in = io::make_compressed_input_stream(method, source);
  binary_deserializer d{*in};
  d.get(xs...);
  return nothing;
}

} // namespace vast

#endif
