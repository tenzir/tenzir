#include "vast/schema.h"

#include <fstream>
#include "vast/logger.h"
#include "vast/optional.h"
#include "vast/serialization.h"
#include "vast/detail/ast/schema.h"
#include "vast/detail/parser/schema.h"
#include "vast/io/container_stream.h"
#include "vast/util/convert.h"

namespace vast {
namespace {

class type_factory
{
public:
  using result_type = intrusive_ptr<schema::type>;

  type_factory(schema const& s)
    : schema_(s)
  {
  }

  result_type operator()(detail::ast::schema::basic_type type) const
  {
    switch (type)
    {
      default:
        assert(! "missing type implementation");
      case detail::ast::schema::bool_type:
        return new schema::bool_type;
      case detail::ast::schema::int_type:
        return new schema::int_type;
      case detail::ast::schema::uint_type:
        return new schema::uint_type;
      case detail::ast::schema::double_type:
        return new schema::double_type;
      case detail::ast::schema::time_frame_type:
        return new schema::time_frame_type;
      case detail::ast::schema::time_point_type:
        return new schema::time_point_type;
      case detail::ast::schema::string_type:
        return new schema::string_type;
      case detail::ast::schema::regex_type:
        return new schema::regex_type;
      case detail::ast::schema::address_type:
        return new schema::address_type;
      case detail::ast::schema::prefix_type:
        return new schema::prefix_type;
      case detail::ast::schema::port_type:
        return new schema::port_type;
    }
  }

  result_type operator()(detail::ast::schema::enum_type const& type) const
  {
    auto t = new schema::enum_type;
    t->fields = type.fields;
    return t;
  }

  result_type operator()(detail::ast::schema::vector_type const& type) const
  {
    auto t = new schema::vector_type;
    t->elem_type = create_type_info(type.element_type);
    return t;
  }

  result_type operator()(detail::ast::schema::set_type const& type) const
  {
    auto t = new schema::set_type;
    t->elem_type = create_type_info(type.element_type);
    return t;
  }

  result_type operator()(detail::ast::schema::table_type const& type) const
  {
    auto t = new schema::table_type;
    t->key_type = create_type_info(type.key_type);
    t->value_type = create_type_info(type.value_type);
    return t;
  }

  result_type operator()(detail::ast::schema::record_type const& type) const
  {
    auto record = new schema::record_type;
    for (auto& arg : type.args)
      record->args.emplace_back(create_argument(arg));
    return record;
  }

  schema::argument create_argument(
      detail::ast::schema::argument_declaration const& a) const
  {
    schema::argument arg;
    arg.name = a.name;
    arg.type = create_type_info(a.type);

    if (a.attrs)
    {
      for (auto& attr : *a.attrs)
      {
        if (attr.key == "optional")
          arg.optional = true;

        if (attr.key == "skip")
          arg.indexed = false;
      }
    }

    return arg;
  }

  schema::type_info create_type_info(
      detail::ast::schema::type_info const& ti) const
  {
    auto info = schema_.info(ti.name);
    if (info)
      return info;
    return {ti.name, boost::apply_visitor(*this, ti.type)};
  }

private:
  schema const& schema_;
};

struct schema_factory
{
  using result_type = void;

  schema_factory(schema& s)
    : tm_{s}, schema_{s}
  {
  }

  result_type operator()(detail::ast::schema::type_declaration const& td)
  {
    if (auto p = boost::get<detail::ast::schema::type_type>(&td.type))
    {
      if (! schema_.add_type(td.name, boost::apply_visitor(std::ref(tm_), *p)))
        error_ = error{"erroneous type declaration: " + td.name};
    }
    else if (auto p = boost::get<detail::ast::schema::type_info>(&td.type))
    {
      if (! schema_.add_type_alias(p->name, td.name))
        error_ = error{"could not create type alias"};
    }
  }

  result_type operator()(detail::ast::schema::event_declaration const& ed)
  {
    schema::event e;
    e.name = ed.name;

    if (ed.args)
      for (auto& arg : *ed.args)
        e.args.emplace_back(tm_.create_argument(arg));

    schema_.add_event(std::move(e));
  }

  type_factory tm_;
  schema& schema_;
  optional<error> error_;
};

} // namespace <anonymous>

schema::type_info::type_info(std::string name, intrusive_ptr<schema::type> t)
  : name(std::move(name)),
    type(std::move(t))
{
}

bool schema::type_info::convert(std::string& str) const
{
  str = name != "<anonymous>" ? name : to<std::string>(*type);
  return true;
}

#define VAST_DEFINE_BASIC_TYPE_CONVERT(concrete_type, desc)    \
bool schema::concrete_type::convert(std::string& str) const    \
{                                                              \
  str = desc;                                                  \
  return true;                                                 \
}

VAST_DEFINE_BASIC_TYPE_CONVERT(bool_type, "bool")
VAST_DEFINE_BASIC_TYPE_CONVERT(int_type, "int")
VAST_DEFINE_BASIC_TYPE_CONVERT(uint_type, "count")
VAST_DEFINE_BASIC_TYPE_CONVERT(double_type, "double")
VAST_DEFINE_BASIC_TYPE_CONVERT(time_frame_type, "interval")
VAST_DEFINE_BASIC_TYPE_CONVERT(time_point_type, "time")
VAST_DEFINE_BASIC_TYPE_CONVERT(string_type, "string")
VAST_DEFINE_BASIC_TYPE_CONVERT(regex_type, "pattern")
VAST_DEFINE_BASIC_TYPE_CONVERT(address_type, "addr")
VAST_DEFINE_BASIC_TYPE_CONVERT(prefix_type, "subnet")
VAST_DEFINE_BASIC_TYPE_CONVERT(port_type, "port")

#undef VAST_DEFINE_BASIC_TYPE_CONVERT

bool schema::enum_type::convert(std::string& str) const
{
  str.clear();
  str += "enum {";
  auto first = fields.begin();
  auto last = fields.end();
  while (first != last)
  {
    str += *first;
    if (++first != last)
      str += ", ";
  }
  str += '}';
  return true;
}

bool schema::vector_type::convert(std::string& str) const
{
  str = "vector of " + to<std::string>(elem_type);
  return true;
}

bool schema::set_type::convert(std::string& str) const
{
  str = "set[" + to<std::string>(elem_type) + ']';
  return true;
}

bool schema::table_type::convert(std::string& str) const
{
  str = "table["
    + to<std::string>(key_type)
    + "] of "
    + to<std::string>(value_type);
  return true;
}

bool schema::record_type::convert(std::string& str) const
{
  str = "record {";
  auto first = args.begin();
  auto last = args.end();
  while (first != last)
  {
    str += to<std::string>(*first);
    if (++first != last)
      str += ", ";
  }
  str += '}';
  return true;
}

bool schema::event::convert(std::string& str) const
{
  str = "event ";
  str += name + "(";
  auto first = args.begin();
  auto last = args.end();
  while (first != last)
  {
    str += to<std::string>(*first);
    if (++first != last)
      str += ", ";
  }
  str += ')';
  return true;
}

bool schema::argument::convert(std::string& str) const
{
  str = name + ": " + to<std::string>(type);
  return true;
}

trial<schema> schema::load(std::string const& contents)
{
  schema sch;

  VAST_LOG_DEBUG("parsing schema");

  auto i = contents.begin();
  auto end = contents.end();
  using iterator_type = std::string::const_iterator;
  std::string err;
  detail::parser::error_handler<iterator_type> on_error{i, end, err};
  detail::parser::schema<iterator_type> grammar(on_error);
  detail::parser::skipper<iterator_type> skipper;
  detail::ast::schema::schema ast;
  bool success = phrase_parse(i, end, grammar, skipper, ast);
  if (! success || i != end)
    return error{std::move(err)};

  type_factory tf{sch};
  grammar.basic_type_.for_each(
      [&](std::string const& name, detail::ast::schema::type_info const& info)
      {
        auto t = boost::apply_visitor(std::ref(tf), info.type);
        sch.add_type(name, t);
      });

  schema_factory sf{sch};
  for (auto& statement : ast)
  {
    boost::apply_visitor(std::ref(sf), statement);
    if (sf.error_)
      return *sf.error_;
  }

  return {std::move(sch)};
}

trial<schema> schema::read(const std::string& filename)
{
  std::ifstream in(filename);
  std::string storage;
  in.unsetf(std::ios::skipws);
  std::copy(std::istream_iterator<char>(in),
            std::istream_iterator<char>(),
            std::back_inserter(storage));

  return load(storage);
}

void schema::write(std::string const& filename) const
{
  std::ofstream(filename) << to_string(*this);
}

std::vector<schema::type_info> const& schema::types() const
{
  return types_;
}

std::vector<schema::event> const& schema::events() const
{
  return events_;
}

schema::type_info schema::info(std::string const& name) const
{
  auto i = std::find_if(
      types_.begin(),
      types_.end(),
      [&name](type_info const& ti) { return ti.name == name; });

  return i != types_.end() ? *i : type_info();
}


trial<std::vector<offset>>
schema::lookup(record_type const* rec,
                       std::vector<std::string> const& ids)
{
  if (ids.empty())
    return error{"empty ID sequence"};

  std::vector<offset> offs;
  for (size_t i = 0; i < rec->args.size(); ++i)
  {
    auto& ti = rec->args[i].type;
    auto r = dynamic_cast<record_type const*>(ti.type.get());
    if (ti.name == ids.front())
    {
      if (ids.size() == 1)
      {
        offs.push_back({i});
      }
      else if (r)
      {
        auto arg_offs = resolve(r, {ids.begin() + 1, ids.end()});
        if (! arg_offs)
          return arg_offs.failure();

        arg_offs->insert(arg_offs->begin(), i);
        offs.push_back(std::move(*arg_offs));
      }
    }
    else if (auto r = dynamic_cast<record_type const*>(ti.type.get()))
    {
      auto inner = lookup(r, ids);
      if (! inner)
        return inner.failure();

      for (auto& v : *inner)
        v.insert(v.begin(), i);

      offs.insert(offs.end(), inner->begin(), inner->end());
    }
  }

  return {std::move(offs)};
}

trial<offset> schema::resolve(
    record_type const* rec,
    std::vector<std::string> const& ids)
{
  if (ids.empty())
    return error{"empty ID sequence"};

  offset off;
  auto found = true;
  for (auto sym = ids.begin(); sym != ids.end() && found; ++sym)
  {
    found = false;
    assert(rec != nullptr);
    for (size_t i = 0; i < rec->args.size(); ++i)
    {
      if (rec->args[i].name == *sym)
      {
        // There can be only exactly one argument with the given name.
        found = true;

        // If the name matches, we have to check whether we're dealing
        // with the last argument name or yet another intermediate
        // rec.
        auto type_ptr = rec->args[i].type.type.get();
        rec = dynamic_cast<record_type const*>(type_ptr);
        if (sym + 1 != ids.end() && ! rec)
          return error{"intermediate arguments must be records"};

        off.push_back(i);
        break;
      }
    }
  }

  if (! found)
    return error{"non-existant argument name"};

  return {std::move(off)};
}

bool schema::add_type(std::string name, intrusive_ptr<type> t)
{
  if (info(name))
  {
    VAST_LOG_ERROR("duplicate type: " << name);
    return false;
  }

  VAST_LOG_DEBUG("adding type " << name << ": " << to_string(*t));
  types_.emplace_back(std::move(name), std::move(t));

  return true;
}

bool schema::add_type_alias(std::string const& type, std::string const& alias)
{
  auto i = std::find_if(
      types_.begin(),
      types_.end(),
      [&type](type_info const& ti) { return ti.name == type; });

  if (i == types_.end())
    return false;

  add_type(alias, i->type);

  VAST_LOG_DEBUG("making type alias: " << alias << " -> " << type);
  i->aliases.push_back(alias);
  return true;
}

void schema::add_event(event e)
{
  VAST_LOG_DEBUG("adding event: " << to_string(e));
  events_.emplace_back(std::move(e));
}

void schema::serialize(serializer& sink) const
{
  sink << to_string(*this);
}

void schema::deserialize(deserializer& source)
{
  std::string str;
  source >> str;
  if (auto s = load(str))
    *this = std::move(*s);
  else
    VAST_LOG_ERROR("failed during schema deserialization");
}

bool schema::convert(std::string& to) const
{
  // Ignore aliases and built-in types.
  std::set<std::string> aliases;
  static std::set<std::string> builtin{"bool", "int", "count", "double",
                                       "interval", "time", "string", "pattern",
                                       "addr", "subnet", "port"};
  for (auto& t : types_)
  {
    if (! (builtin.count(t.name) || aliases.count(t.name)))
    {
      to += "type " + t.name + ": " + to_string(*t.type) + '\n';
      for (auto& a : t.aliases)
      {
        to += "type " + a + ": " + t.name + '\n';
        aliases.insert(a);
      }
    }
  }

  if (! events_.empty())
    to += '\n';

  for (auto& e : events_)
    to += to_string(e) + '\n';

  return true;
}

bool operator==(schema const& x, schema const& y)
{
  return x.types_ == y.types_ && x.events_ == y.events_;
}

bool operator!=(schema const& x, schema const& y)
{
  return ! (x == y);
}

bool operator==(schema::type_info const& x, schema::type_info const& y)
{
  return x.name == y.name && x.type == y.type;
}

bool operator==(schema::argument const& x, schema::argument const& y)
{
  return x.name == y.name && x.type == y.type;
}

bool operator==(schema::event const& x, schema::event const& y)
{
  return x.name == y.name && x.args == y.args;
}

} // namespace vast

namespace std {

size_t hash<vast::schema>::operator()(vast::schema const& sch) const
{
  std::string str;
  {
    auto out = vast::io::make_container_output_stream(str);
    vast::binary_serializer sink(out);
    sink << sch;
  }
  return hash<std::string>()(str);
}

} // namespace std
