#include "vast/json.h"
#include "vast/type.h"
#include "vast/concept/convertible/vast/address.h"
#include "vast/concept/convertible/vast/type.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/address.h"
#include "vast/concept/printable/vast/type.h"

namespace vast {

namespace {

struct jsonizer {
  jsonizer(json& j, bool flatten = false)
    : json_{j},
      flatten_{flatten} {
  }

  template <typename T>
  bool operator()(T const&) {
    json_ = {};
    return true;
  }

  bool operator()(type::enumeration const& e) {
    json::array a;
    std::transform(e.fields().begin(),
                   e.fields().end(),
                   std::back_inserter(a),
                   [](auto& x) { return json{x}; });
    json_ = std::move(a);
    return true;
  }

  bool operator()(type::vector const& v) {
    json::object o;
    if (!convert(v.elem(), o["elem"], flatten_))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(type::set const& s) {
    json::object o;
    if (!convert(s.elem(), o["elem"], flatten_))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(type::table const& t) {
    json::object o;
    if (!convert(t.value(), o["key"], flatten_))
      return false;
    if (!convert(t.value(), o["value"], flatten_))
      return false;
    json_ = std::move(o);
    return true;
  }

  bool operator()(type::record const& r) {
    json::object o;
    if (flatten_) {
      for (auto& e : type::record::each{r})
        if (!convert(e.trace.back()->type, o[to_string(e.key())], flatten_))
          return false;
    } else {
      for (auto& field : r.fields())
        if (!convert(field.type, o[to_string(field.name)], flatten_))
          return false;
    }
    json_ = std::move(o);
    return true;
  }

  bool operator()(type::alias const& a) {
    return convert(a.type(), json_, flatten_);
  }

  json& json_;
  bool flatten_;
};

} // namespace <anonymous>

bool convert(type const& t, json& j, bool flatten) {
  json::object o;
  o["name"] = t.name();
  o["kind"] = to_string(which(t));
  if (!visit(jsonizer{o["structure"], flatten}, t))
    return false;
  json::array a;
  std::transform(t.attributes().begin(),
                 t.attributes().end(),
                 std::back_inserter(a),
                 [](auto& x) { return to_json(x); });
  o["attributes"] = std::move(a);
  j = std::move(o);
  return true;
}

bool convert(type::attribute const& a, json& j) {
  switch (a.key) {
    case type::attribute::invalid:
      j = "invalid";
      break;
    case type::attribute::skip:
      j = "skip";
      break;
    case type::attribute::default_:
      j = json::array{"default", a.value};
      break;
  }
  return true;
}

} // namespace vast
