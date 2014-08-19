#ifndef VAST_SCHEMA_H
#define VAST_SCHEMA_H

#include <functional>
#include <vector>
#include <string>
#include "vast/fwd.h"
#include "vast/offset.h"
#include "vast/type.h"
#include "vast/detail/ast/schema.h"
#include "vast/detail/parser/schema.h"
#include "vast/util/intrusive.h"
#include "vast/util/operators.h"

namespace vast {

class schema : util::equality_comparable<schema>
{
public:
  using const_iterator = std::vector<type>::const_iterator;
  using iterator = std::vector<type>::iterator;

  /// Merges two schemata.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The merged schema.
  static trial<schema> merge(schema const& s1, schema const& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `nothing` on success.
  trial<void> add(type t);

  /// Retrieves the type for a given type name.
  /// @param name The name of the type to lookup.
  /// @returns The type registered as *name* or an empty pointer if *name* does
  /// not exist.
  type const* find_type(std::string const& name) const;

  /// Retrieves the type(s) matching a given type.
  /// @param t The ype to look for.
  /// @returns The type(s) having type *t*.
  std::vector<type> find_types(type const& t) const;

  /// Checks whether a given event complies with the schema.
  /// @param e The event to test.
  /// @returns `nothing` iff *e* complies with this schema.
  //trial<void> complies(event const& e) const;

  // Container API.
  const_iterator begin() const;
  const_iterator end() const;

  /// Retrieves the number of types in the schema.
  /// @returns The number of types this schema has.
  size_t size() const;

  /// Checks whether the schema is empty.
  /// @returns `size() == 0`.
  bool empty() const;

private:
  std::vector<type> types_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(schema const& s, Iterator&& out)
  {
    for (auto& t : s.types_)
    {
      if (t.name().empty())
        continue;

      // TODO: fix laziness.
      print("type ", out);
      print(t.name(), out);
      print(": ", out);
      print(t, out, false);
      print("\n", out);
    }

    return nothing;
  }

  template <typename Iterator>
  friend trial<void> parse(schema& sch, Iterator& begin, Iterator end);

  friend bool operator==(schema const& x, schema const& y);
};

namespace detail {

class type_factory
{
public:
  using result_type = type;

  type_factory(schema const& s, std::string name = "")
    : schema_{s},
      name_{std::move(name)}
  {
  }

  type operator()(detail::ast::schema::basic_type bt) const
  {
    type t;
    switch (bt)
    {
      default:
        assert(! "missing type implementation");
      case detail::ast::schema::bool_type:
        t = type::boolean{};
        break;
      case detail::ast::schema::int_type:
        t = type::integer{};
        break;
      case detail::ast::schema::uint_type:
        t = type::count{};
        break;
      case detail::ast::schema::double_type:
        t = type::real{};
        break;
      case detail::ast::schema::time_point_type:
        t = type::time_point{};
        break;
      case detail::ast::schema::time_frame_type:
        t = type::time_duration{};
        break;
      case detail::ast::schema::string_type:
        t = type::string{};
        break;
      case detail::ast::schema::regex_type:
        t = type::pattern{};
        break;
      case detail::ast::schema::address_type:
        t = type::address{};
        break;
      case detail::ast::schema::prefix_type:
        t = type::subnet{};
        break;
      case detail::ast::schema::port_type:
        t = type::port{};
        break;
    }

    t.name(name_);
    return t;
  }

  type operator()(detail::ast::schema::enum_type const& t) const
  {
    auto e = type::enumeration{t.fields};
    e.name(name_);
    return e;
  }

  type operator()(detail::ast::schema::vector_type const& t) const
  {
    auto v = type::vector{make_type(t.element_type)};
    v.name(name_);
    return v;
  }

  type operator()(detail::ast::schema::set_type const& t) const
  {
    auto s = type::set{make_type(t.element_type)};
    s.name(name_);
    return s;
  }

  type operator()(detail::ast::schema::table_type const& t) const
  {
    auto m = type::table{make_type(t.key_type), make_type(t.value_type)};
    m.name(name_);
    return m;
  }

  type operator()(detail::ast::schema::record_type const& t) const
  {
    std::vector<type::record::field> fields;
    for (auto& arg : t.args)
      fields.push_back({arg.name, make_type(arg.type)});

    auto r = type::record{std::move(fields)};
    r.name(name_);
    return r;
  }

  type make_type(detail::ast::schema::type const& t) const
  {
    if (auto x = schema_.find_type(t.name))
      return *x;

    return boost::apply_visitor(type_factory{schema_}, t.info);
  }

private:
  schema const& schema_;
  std::string name_;
};

struct schema_factory
{
  using result_type = trial<void>;

  schema_factory(schema& s)
    : schema_{s}
  {
  }

  trial<void> operator()(detail::ast::schema::type_declaration const& td)
  {
    if (auto ti = boost::get<detail::ast::schema::type_info>(&td.type))
    {
      auto t = boost::apply_visitor(type_factory{schema_, td.name}, *ti);
      if (! schema_.add(t))
        return error{"erroneous type declaration: " + td.name};
    }
    else if (auto x = boost::get<detail::ast::schema::type>(&td.type))
    {
      auto t = schema_.find_type(x->name);
      if (t)
      {
        auto u = type::alias{*t};
        u.name(td.name);
        schema_.add(u);
      }
      else
      {
        auto u = boost::apply_visitor(type_factory{schema_, td.name}, x->info);
        schema_.add(u);
      }
    }

    return nothing;
  }

  trial<void> operator()(detail::ast::schema::event_declaration const&)
  {
    /* We will soon no longer treat events any different from types. */
    return nothing;
  }

  schema& schema_;
};

} // namespace detail

template <typename Iterator>
trial<void> parse(schema& sch, Iterator& begin, Iterator end)
{
  std::string err;
  detail::parser::error_handler<Iterator> on_error{begin, end, err};
  detail::parser::schema<Iterator> grammar{on_error};
  detail::parser::skipper<Iterator> skipper;
  detail::ast::schema::schema ast;

  // TODO: get rid of the exception in the schema grammar.
  try
  {
    bool success = phrase_parse(begin, end, grammar, skipper, ast);
    if (! success || begin != end)
      return error{std::move(err)};
  }
  catch (std::runtime_error const& e)
  {
    return error{e.what()};
  }

  sch.types_.clear();
  detail::schema_factory sf{sch};
  for (auto& statement : ast)
  {
    auto t = boost::apply_visitor(std::ref(sf), statement);
    if (! t)
      return t.error();
  }

  return nothing;
}

} // namespace vast

#endif
