#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_CHAR_HELPERS_H
#define VAST_CONCEPT_PARSEABLE_DETAIL_CHAR_HELPERS_H

#include <string>
#include <vector>

namespace vast {
namespace detail {

template <typename Attribute, typename T>
void absorb(Attribute& a, T&& x) {
  a = std::move(x);
}

inline void absorb(std::string& str, char c) {
  str += c;
}

inline void absorb(std::vector<char> v, char c) {
  v.push_back(c);
}

} // namespace detail
} // namespace vast

#endif
