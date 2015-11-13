#include "vast/schema.h"

#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/type.h"

namespace vast {

maybe<schema> schema::merge(schema const& s1, schema const& s2) {
  auto result = s1;
  if (! result.add(s2))
    return {};
  return result;
}

bool schema::add(type t) {
  if (is<none>(t) || t.name().empty() || find(t.name()))
    return false;
  types_.push_back(std::move(t));
  return true;
}

bool schema::add(schema const& sch) {
  for (auto& t : sch) {
    if (auto u = find(t.name())) {
      if (t != *u && t.name() == u->name())
        // Type clash: cannot accomodate two types with same name.
        return false;
    } else {
      types_.push_back(t);
    }
  }
  return true;
}

type const* schema::find(std::string const& name) const {
  for (auto& t : types_)
    if (t.name() == name)
      return &t;
  return nullptr;
}

schema::const_iterator schema::begin() const {
  return types_.begin();
}

schema::const_iterator schema::end() const {
  return types_.end();
}

size_t schema::size() const {
  return types_.size();
}

bool schema::empty() const {
  return types_.empty();
}

void schema::clear() {
  types_.clear();
}

bool operator==(schema const& x, schema const& y) {
  return x.types_ == y.types_;
}

} // namespace vast
