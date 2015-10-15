#include "vast/json.h"
#include "vast/data.h"
#include "vast/concept/convertible/vast/address.h"
#include "vast/concept/convertible/vast/data.h"
#include "vast/concept/convertible/vast/port.h"
#include "vast/concept/convertible/vast/subnet.h"
#include "vast/concept/convertible/vast/time.h"

namespace vast {

namespace {

struct jsonizer {
  jsonizer(json& j) : j_{j} { }

  bool operator()(none) const {
    return true;
  }

  bool operator()(std::string const& str) const {
    j_ = str;
    return true;
  }

  template <typename T>
  bool operator()(T const& x) const {
    return convert(x, j_);
  }

  json& j_;
};

} // namespace <anonymous>

bool convert(vector const& v, json& j) {
  json::array a(v.size());
  for (auto i = 0u; i < v.size(); ++i)
    if (!visit(jsonizer{a[i]}, v[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(set const& s, json& j) {
  json::array a(s.size());
  for (auto i = 0u; i < s.size(); ++i)
    if (!visit(jsonizer{a[i]}, s[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(table const& t, json& j) {
  json::array values;
  for (auto& p : t) {
    json::array a;
    json j;
    if (!visit(jsonizer{j}, p.first))
      return false;
    a.push_back(std::move(j));
    if (!visit(jsonizer{j}, p.second))
      return false;
    a.push_back(std::move(j));
    values.emplace_back(std::move(a));
  };
  j = std::move(values);
  return true;
}

bool convert(record const& r, json& j) {
  json::array a(r.size());
  for (auto i = 0u; i < r.size(); ++i)
    if (!visit(jsonizer{a[i]}, r[i]))
      return false;
  j = std::move(a);
  return true;
}

bool convert(data const& d, json& j) {
  return visit(jsonizer{j}, d);
}

bool convert(data const& d, json& j, type const& t) {
  auto r = get<record>(d);
  auto tr = get<type::record>(t);
  if (r && tr) {
    if (r->size() != tr->fields().size())
      return false;
    json::object o;
    for (auto i = 0u; i < r->size(); ++i) {
      auto& f = tr->fields()[i];
      if (! convert((*r)[i], o[f.name], f.type))
        return false;
    }
    j = std::move(o);
    return true;
  }
  return convert(d, j);
}

} // namespace vast
