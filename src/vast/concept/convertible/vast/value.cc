#include "vast/json.h"
#include "vast/value.h"
#include "vast/concept/convertible/vast/data.h"
#include "vast/concept/convertible/vast/value.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/type.h"
#include "vast/concept/printable/vast/data.h"

namespace vast {

bool convert(value const& v, json& j)
{
  json::object o;
  std::string type_string;
  if (! printers::type<policy::signature>(type_string, v.type()))
    return false;
  o["type"] = std::move(type_string);
  if (! is<type::record>(v.type()))
  {
    if (! convert(v.data(), o["data"]))
      return false;
  }
  else
  {
    json::object rec;
    type::record::each record_range{*get<type::record>(v.type())};
    record::each data_range{*get<record>(v.data())};
    auto r = record_range.begin();
    auto d = data_range.begin();
    while (r != record_range.end())
    {
      rec[r->key().back()] = to_json(d->data());
      ++r;
      ++d;
    }
    VAST_ASSERT(d == data_range.end());
    o["data"] = std::move(rec);
  }
  j = std::move(o);
  return true;
}

} // namespace vast

