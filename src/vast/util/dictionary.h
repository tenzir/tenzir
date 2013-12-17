#ifndef VAST_UTIL_DICTIONARY_H
#define VAST_UTIL_DICTIONARY_H

#include <string>
#include <unordered_map>
#include "vast/serialization.h"

namespace vast {
namespace util {

/// A bijection between a string and an integral type.
/// @tparam Derived The CRTP client.
/// @tparam Domain A string type.
/// @tparam Codomain An integral type.
template <typename Derived, typename Domain, typename Codomain>
class dictionary
{
  static_assert(! std::is_integral<Domain>::value,
                "a dictionary requires an non-integral domain type");

  static_assert(std::is_integral<Codomain>::value,
                "a dictionary requires an integral codomain type");
public:
  /// Retrieves the ID of a given string.
  /// @param str The string to lookup.
  /// @returns The ID of *str*.
  Codomain const* operator[](Domain const& str) const
  {
    return derived()->locate(str);
  }

  /// Retrieves the string corresponding to a given ID.
  /// @param id The ID to lookup.
  /// @returns The string having ID *id*.
  Domain const* operator[](Codomain id) const
  {
    return derived()->extract(id);
  }

  /// Inserts a string into the dictionary.
  ///
  /// @param str The string mapping to *id*.
  ///
  /// @returns A pointer to the inserted value that *str* maps to or `nullptr`
  /// on failure.
  Codomain const* insert(Domain const& str)
  {
    if (derived()->locate(str))
      return nullptr;

    auto success = derived()->insert(str, next_);
    if (success != nullptr)
      ++next_;

    return success;
  }

protected:
  void serialize(serializer& sink) const
  {
    sink << next_;
  }

  void deserialize(deserializer& source)
  {
    source >> next_;
  }

private:
  friend access;

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Codomain next_ = 0;
};

/// A dictionary based on an STL hash table.
template <typename Domain, typename Codomain>
class map_dictionary 
  : public dictionary<map_dictionary<Domain, Codomain>, Domain, Codomain>
{
  using super = dictionary<map_dictionary<Domain, Codomain>, Domain, Codomain>;

public:
  using super::insert;

  Codomain const* locate(Domain const& str) const
  {
    auto i = map_.find(str);
    return i == map_.end() ? nullptr : &i->second;
  }

  Domain const* extract(Codomain id) const
  {
    for (auto& p : map_)
      if (p.second == id)
        return &p.first;
    return nullptr;
  }

  Codomain const* insert(Domain const& str, Codomain next)
  {
    auto p = map_.emplace(str, next);
    return p.second ? &p.first->second : nullptr;
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    super::serialize(sink);
    sink << map_;
  }

  void deserialize(deserializer& source)
  {
    super::deserialize(source);
    source >> map_;
  }

  std::unordered_map<Domain, Codomain> map_;
};

} // namespace util
} // namespace vast

#endif
