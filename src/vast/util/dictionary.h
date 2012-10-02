#ifndef VAST_UTIL_DICTIONARY_H
#define VAST_UTIL_DICTIONARY_H

#include <string>
#include <unordered_map>

namespace vast {
namespace util {

/// A bijection between a string and an integral type.
template <typename Codomain>
class dictionary
{
  static_assert(std::is_integral<Codomain>::value,
                "a dictionary requires an integral codomain type");
public:
  /// Retrieves the ID of a given string.
  /// @param str The string to lookup.
  /// @return The ID of *str*.
  Codomain const* operator[](std::string const& str) const
  {
      return locate(str);
  }

  /// Retrieves the string corresponding to a given ID.
  /// @param id The ID to lookup.
  /// @return str The string having ID *id*.
  std::string const* operator[](Codomain id) const
  {
      return extract(id);
  }

  /// Inserts a string into the dictionary.
  /// @param str The string mapping to *id*.
  /// @return `true` if the insertion succeeded.
  virtual Codomain const* insert(std::string const& str) = 0;

protected:
  virtual Codomain const* locate(std::string const& str) const = 0;
  virtual std::string const* extract(Codomain id) const = 0;

  Codomain next_ = 0;
};

/// A dictionary based on an STL hash table.
template <typename Codomain>
class map_dictionary : public dictionary<Codomain>
{
  typedef dictionary<Codomain> super;
public:
  virtual Codomain const* locate(std::string const& str) const
  {
    auto i = map_.find(str);
    return i == map_.end() ? nullptr : &i->second;
  }

  virtual std::string const* extract(Codomain id) const
  {
    for (auto& p : map_)
      if (p.second == id)
        return &p.first;
    return nullptr;
  }

  virtual Codomain const* insert(std::string const& str)
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
  std::unordered_map<std::string, Codomain> map_;
};

} // namespace util
} // namespace vast

#endif
