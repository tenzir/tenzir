#include "vast/schema.hpp"

#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/concept/printable/vast/type.hpp"

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

// TODO: we should figure out a better way to (de)serialize. Going through
// strings is not very efficient, although we currently have no other way to
// keep the pointer relationships of the types intact.

void serialize(caf::serializer& sink, schema const& sch) {
  sink << to_string(sch);
}

void serialize(caf::deserializer& source, schema& sch) {
  std::string str;
  source >> str;
  if (str.empty())
    return;
  sch.clear();
  auto i = str.begin();
  parse(i, str.end(), sch);
}

} // namespace vast
