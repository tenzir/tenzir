#ifndef VAST_IO_SERIALIZATION_H
#define VAST_IO_SERIALIZATION_H

#include "vast/serialization.h"
#include "vast/io/container_stream.h"
#include "vast/io/compressed_stream.h"

namespace vast {
namespace io {

namespace detail {

template <typename Serializer, typename T>
void do_serialize(Serializer& s, T const& x)
{
  s << x;
}

template <typename Serializer, typename T0, typename... Ts>
void do_serialize(Serializer& s, T0 const& x, Ts const&... xs)
{
  do_serialize(s, x);
  do_serialize(s, xs...);
}

template <typename Deserializer, typename T>
void do_deserialize(Deserializer& s, T& x)
{
  s >> x;
}

template <typename Deserializer, typename T0, typename... Ts>
void do_deserialize(Deserializer& s, T0& x, Ts&... xs)
{
  do_deserialize(s, x);
  do_deserialize(s, xs...);
}

} // namespace detail

template <
  typename... Ts,
  typename Container,
  typename Serializer = binary_serializer
>
void archive(Container& c, Ts const&... xs)
{
  auto sink = io::make_container_output_stream(c);
  Serializer s(sink);
  detail::do_serialize(s, xs...);
}

template <
  typename... Ts,
  typename Container,
  typename Deserializer = binary_deserializer
>
void unarchive(Container const& c, Ts&... xs)
{
  auto source = make_container_input_stream(c);
  Deserializer d(source);
  detail::do_deserialize(d, xs...);
}

template <typename... Ts, typename Container>
void compress(compression method, Container& c, Ts const&... xs)
{
  auto buf = make_container_output_stream(c);
  std::unique_ptr<compressed_output_stream> out(
      make_compressed_output_stream(method, buf));
  binary_serializer s(*out);
  detail::do_serialize(s, xs...);
}

template <typename... Ts, typename Container>
void decompress(compression method, Container const& c, Ts&... xs)
{
  auto buf = make_array_input_stream(c);
  std::unique_ptr<compressed_input_stream> in(
      make_compressed_input_stream(method, buf));
  binary_deserializer d(*in);
  detail::do_deserialize(d, xs...);
}

} // namespace io
} // namespace vast

#endif
