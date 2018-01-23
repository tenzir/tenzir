/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/schema.hpp"

#include "vast/concept/parseable/parse.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/schema.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/json.hpp"

namespace vast {

optional<schema> schema::merge(const schema& s1, const schema& s2) {
  auto result = s2;
  for (auto& t : s1) {
    if (auto u = s2.find(t.name())) {
      if (t != *u && t.name() == u->name())
        // Type clash: cannot accomodate two types with same name.
        return {};
    } else {
      result.types_.push_back(t);
    }
  }
  return result;
}

bool schema::add(const type& t) {
  if (is<none_type>(t) || t.name().empty() || find(t.name()))
    return false;
  types_.push_back(std::move(t));
  return true;
}

const type* schema::find(const std::string& name) const {
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

bool operator==(const schema& x, const schema& y) {
  return x.types_ == y.types_;
}

// TODO: we should figure out a better way to (de)serialize: use manual pointer
// tracking to save types exactly once. Something along those lines:
//
//namespace {
//
//struct pointer_hash {
//  size_t operator()(const type& t) const noexcept {
//    return reinterpret_cast<size_t>(t.ptr_.get());
//  }
//};
//
//using type_cache = std::unordered_set<type, pointer_hash>;
//
//template <class Serializer>
//struct type_serializer {
//
//  type_serializer(Serializer& sink, type_cache& cache)
//    : sink_{sink}, cache_{cache} {
//  }
//
//  void save_type(type const t) const {
//    if (t.name().empty()) {
//      visit(*this, t); // recurse
//      return;
//    }
//    if (cache_.count(t)) {
//      sink_ << t.name();
//      return;
//    }
//    visit(*this, t); // recurse
//    cache_.insert(t.name());
//  }
//
//  template <class T>
//  void operator()(const T& x) const {
//    sink_ << x;
//  };
//
//  void operator()(const vector_type& t) const {
//    save_type(t.value_type);
//  }
//
//  void operator()(const set_type& t) const {
//    save_type(t.value_type);
//  }
//
//  void operator()(const table_type& t) const {
//    save_type(t.key_type);
//    save_type(t.value_type);
//  }
//
//  void operator()(const record_type& t) const {
//    auto size = t.fields.size();
//    sink_.begin_sequence(size);
//    for (auto& f : t.fields) {
//      sink_ << f.name;
//      save_type(f.type);
//    }
//    sink_.end_sequence();
//  }
//
//  Serializer& sink_;
//  type_cache& cache_;
//};
//
//} // namespace <anonymous>

void serialize(caf::serializer& sink, const schema& sch) {
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

bool convert(const schema& s, json& j) {
  json::object o;
  json::array a;
  std::transform(s.begin(), s.end(), std::back_inserter(a),
                 [](auto& t) { return to_json(t); });
  o["types"] = std::move(a);
  j = std::move(o);
  return true;
}

} // namespace vast
