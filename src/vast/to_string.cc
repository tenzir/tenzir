#include "vast/to_string.h"

#include <set>

namespace vast {

std::string to_string(schema::type const& type)
{
  std::string str;
  if (dynamic_cast<schema::bool_type const*>(&type))
  {
    str += "bool";
  }
  else if (dynamic_cast<schema::int_type const*>(&type))
  {
    str += "int";
  }
  else if (dynamic_cast<schema::uint_type const*>(&type))
  {
    str += "count";
  }
  else if (dynamic_cast<schema::double_type const*>(&type))
  {
    str += "double";
  }
  else if (dynamic_cast<schema::time_frame_type const*>(&type))
  {
    str += "interval";
  }
  else if (dynamic_cast<schema::time_point_type const*>(&type))
  {
    str += "time";
  }
  else if (dynamic_cast<schema::string_type const*>(&type))
  {
    str += "string";
  }
  else if (dynamic_cast<schema::regex_type const*>(&type))
  {
    str += "pattern";
  }
  else if (dynamic_cast<schema::address_type const*>(&type))
  {
    str += "addr";
  }
  else if (dynamic_cast<schema::prefix_type const*>(&type))
  {
    str += "subnet";
  }
  else if (dynamic_cast<schema::port_type const*>(&type))
  {
    str += "port";
  }
  else if (auto* e = dynamic_cast<schema::enum_type const*>(&type))
  {
    str += "enum {";
    auto first = e->fields.begin();
    auto last = e->fields.end();
    while (first != last)
    {
        str += *first;
        if (++first != last)
            str += ", ";
    }
    str += '}';
  }
  else if (auto* v = dynamic_cast<schema::vector_type const*>(&type))
  {
    str += "vector of ";
    str += to_string(v->elem_type);
  }
  else if (auto* s = dynamic_cast<schema::set_type const*>(&type))
  {
    str += "set[";
    str += to_string(s->elem_type);
    str += ']';
  }
  else if (auto* t = dynamic_cast<schema::table_type const*>(&type))
  {
    str += "table[";
    str += to_string(t->key_type);
    str += "] of ";
    str += to_string(t->value_type);
  }
  else if (auto* r = dynamic_cast<schema::record_type const*>(&type))
  {
    str += "record {";
    auto first = r->args.begin();
    auto last = r->args.end();
    while (first != last)
    {
        str += to_string(*first);
        if (++first != last)
            str += ", ";
    }
    str += '}';
  }

  return str;
}

std::string to_string(schema::type_info const& ti)
{
  return ti.name != "<anonymous>" ? ti.name : to_string(*ti.type);
}

std::string to_string(schema::event const& e)
{
  std::string str("event ");
  str += e.name + "(";
  auto first = e.args.begin();
  auto last = e.args.end();
  while (first != last)
  {
    str += to_string(*first);
    if (++first != last)
      str += ", ";
  }
  str += ')';
  return str;
}

std::string to_string(schema::argument const& a)
{
  return a.name + ": " + to_string(a.type);
}

std::string to_string(schema const& s)
{
  std::string str;

  // Ignore aliases and built-in types.
  // ...and also all aliases.
  std::set<std::string> aliases;
  static std::set<std::string> builtin{"bool", "int", "count", "double",
                                       "interval", "time", "string", "pattern",
                                       "addr", "subnet", "port"};
  for (auto& t : s.types_)
  {
    if (! (builtin.count(t.name) || aliases.count(t.name)))
    {
      str += "type " + t.name + ": " + to_string(*t.type) + '\n';
      for (auto& a : t.aliases)
      {
        str += "type " + a + ": " + t.name + '\n';
        aliases.insert(a);
      }
    }
  }

  if (! s.events_.empty())
    str += '\n';

  for (auto& e : s.events_)
    str += to_string(e) + '\n';

  return str;
}


} // namespace vast
