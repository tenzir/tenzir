#ifndef VAST_UUID_H
#define VAST_UUID_H

#include <functional>
#include <boost/uuid/uuid.hpp>
#include "vast/fwd.h"
#include "vast/util/operators.h"

namespace vast {

class uuid : util::totally_ordered<uuid>
{
public:
  typedef boost::uuids::uuid::const_iterator const_iterator;

  static uuid random();
  static uuid nil();

  uuid() = default;
  uuid(std::string const& str);

  const_iterator begin() const;
  const_iterator end() const;

  size_t hash() const;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  friend bool operator==(uuid const& x, uuid const& y);
  friend bool operator<(uuid const& x, uuid const& y);
  friend void swap(uuid& x, uuid& y);

  friend std::string to_string(uuid const&);

  boost::uuids::uuid id_;
};

std::string to_string(uuid const& u);
std::ostream& operator<<(std::ostream& out, uuid const& u);

} // namespace vast

namespace std {

template <>
struct hash<vast::uuid>
{
  size_t operator()(vast::uuid const& u) const;
};

} // namespace std

#endif
