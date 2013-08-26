#ifndef VAST_UTIL_DICTIONARY_H
#define VAST_UTIL_DICTIONARY_H

#include <string>
#include <unordered_map>
#include "vast/serialization.h"

namespace vast {
namespace util {

/// A bijection between a string and an integral type.
template <typename Codomain>
class dictionary
{
  static_assert(std::is_integral<Codomain>::value,
                "a dictionary requires an integral codomain type");
public:
  typedef std::string string_type;

  /// Retrieves the ID of a given string.
  /// @param str The string to lookup.
  /// @return The ID of *str*.
  Codomain const* operator[](string_type const& str) const
  {
    return locate(str);
  }

  /// Retrieves the string corresponding to a given ID.
  /// @param id The ID to lookup.
  /// @return The string having ID *id*.
  string_type const* operator[](Codomain id) const
  {
    return extract(id);
  }

  /// Inserts a string into the dictionary.
  ///
  /// @param str The string mapping to *id*.
  ///
  /// @return A pointer to the inserted value that *str* maps to or `nullptr`
  /// on failure.
  virtual Codomain const* insert(string_type const& str) = 0;

protected:
  virtual Codomain const* locate(string_type const& str) const = 0;
  virtual string_type const* extract(Codomain id) const = 0;

  Codomain next_ = 0;
};

/// A dictionary based on an STL hash table.
template <typename Codomain>
class map_dictionary : public dictionary<Codomain>
{
  typedef dictionary<Codomain> super;
  using typename super::string_type;

public:
  virtual Codomain const* locate(string_type const& str) const
  {
    auto i = map_.find(str);
    return i == map_.end() ? nullptr : &i->second;
  }

  virtual string_type const* extract(Codomain id) const
  {
    for (auto& p : map_)
      if (p.second == id)
        return &p.first;
    return nullptr;
  }

  virtual Codomain const* insert(string_type const& str)
  {
    if (locate(str))
      return nullptr;

    auto p = map_.emplace(str, super::next_);
    if (p.second)
    {
      ++super::next_;
      return &p.first->second;
    }
    return nullptr;
  }

private:
  friend access;
  void serialize(serializer& sink) const
  {
    sink << map_;
  }

  void deserialize(deserializer& source)
  {
    source >> map_;
  }

  std::unordered_map<string_type, Codomain> map_;
};

} // namespace util
} // namespace vast

#endif
