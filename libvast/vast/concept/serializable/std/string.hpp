#ifndef VAST_CONCEPT_SERIALIZABLE_STD_STRING_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_STRING_HPP

#include <string>

#include "vast/concept/serializable/builtin.hpp"

namespace vast {

//
// std::string
//

template <typename Serializer>
void serialize(Serializer& sink, std::string const& str) {
  sink.begin_sequence(str.size());
  if (!str.empty())
    sink.write(str.data(), str.size());
  sink.end_sequence();
}

template <typename Deserializer>
void deserialize(Deserializer& source, std::string& str) {
  auto size = source.begin_sequence();
  if (size > 0) {
    str.resize(size);
    source.read(const_cast<std::string::value_type*>(str.data()), size);
  }
  source.end_sequence();
}

//
// C-strings (compatible to std::string)
//

template <typename Serializer>
void serialize(Serializer& sink, char const* str) {
  size_t n = 0;
  for (;;)
    if (str[n] != '\0')
      ++n;
    else
      break;
  sink.begin_sequence(n);
  if (n > 0)
    sink.write(str, n);
  sink.end_sequence();
}

} // namespace vast

#endif
