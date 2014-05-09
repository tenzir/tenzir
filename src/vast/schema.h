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
  using const_iterator = std::vector<type_const_ptr>::const_iterator;
  using iterator = std::vector<type_const_ptr>::iterator;

  /// Merges two schemata.
  /// @param s1 The first schema.
  /// @param s2 The second schema.
  /// @returns The merged schema.
  static trial<schema> merge(schema const& s1, schema const& s2);

  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `nothing` on success.
  trial<void> add(type_const_ptr t);

  /// Retrieves the type for a given type name.
  /// @param name The name of the type to lookup.
  /// @returns The type registered as *name* or an empty pointer if *name* does
  /// not exist.
  type_const_ptr find_type(string const& name) const;

  /// Retrieves the type(s) for a given type name.
  /// @param ti The [type information](::type_info) to look for.
  /// @returns The type(s) with type information *ti*.
  std::vector<type_const_ptr> find_type_info(type_info const& ti) const;

  /// Checks whether a given event complies with the schema.
  /// @param e The event to test.
  /// @returns `nothing` iff *e* complies with this schema.
  //trial<void> complies(event const& e) const;

  // Container API.
  const_iterator begin() const;
  const_iterator end() const;
  iterator begin();
  iterator end();

  /// Retrieves the number of types in the schema.
  /// @returns The number of types this schema has.
  size_t size() const;

  /// Checks whether the schema is empty.
  /// @returns `size() == 0`.
  bool empty() const;

private:
  std::vector<type_const_ptr> types_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  friend trial<void> print(schema const& s, Iterator&& out)
  {
    for (auto& t : s.types_)
    {
      if (t->name().empty())
        continue;

      // TODO: fix laziness.
      print("type ", out);
      print(t->name(), out);
      print(": ", out);
      print(*t, out, false);
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
  using result_type = type_const_ptr;

  type_factory(schema const& s, string name = "")
    : schema_{s},
      name_{std::move(name)}
  {
  }

  type_const_ptr operator()(detail::ast::schema::basic_type t) const
  {
    switch (t)
    {
      default:
        assert(! "missing type implementation");
      case detail::ast::schema::bool_type:
        return type::make<bool_type>(name_);
      case detail::ast::schema::int_type:
        return type::make<int_type>(name_);
      case detail::ast::schema::uint_type:
        return type::make<uint_type>(name_);
      case detail::ast::schema::double_type:
        return type::make<double_type>(name_);
      case detail::ast::schema::time_frame_type:
        return type::make<time_range_type>(name_);
      case detail::ast::schema::time_point_type:
        return type::make<time_point_type>(name_);
      case detail::ast::schema::string_type:
        return type::make<string_type>(name_);
      case detail::ast::schema::regex_type:
        return type::make<regex_type>(name_);
      case detail::ast::schema::address_type:
        return type::make<address_type>(name_);
      case detail::ast::schema::prefix_type:
        return type::make<prefix_type>(name_);
      case detail::ast::schema::port_type:
        return type::make<port_type>(name_);
    }
  }

  type_const_ptr operator()(detail::ast::schema::enum_type const& t) const
  {
    std::vector<string> v;
    for (auto& str : t.fields)
      v.emplace_back(str);

    return type::make<enum_type>(name_, std::move(v));
  }

  type_const_ptr operator()(detail::ast::schema::vector_type const& t) const
  {
    return type::make<vector_type>(name_, make_type(t.element_type));
  }

  type_const_ptr operator()(detail::ast::schema::set_type const& t) const
  {
    return type::make<set_type>(name_, make_type(t.element_type));
  }

  type_const_ptr operator()(detail::ast::schema::table_type const& t) const
  {
    auto k = make_type(t.key_type);
    auto y = make_type(t.value_type);
    return type::make<table_type>(name_, k, y);
  }

  type_const_ptr operator()(detail::ast::schema::record_type const& t) const
  {
    record_type r;
    for (auto& arg : t.args)
      r.args.emplace_back(make_argument(arg));

    return type::make<record_type>(name_, std::move(r));
  }

  argument make_argument(
      detail::ast::schema::argument_declaration const& a) const
  {
    return {a.name, make_type(a.type)};
  }

  type_const_ptr make_type(detail::ast::schema::type const& t) const
  {
    if (auto x = schema_.find_type(t.name))
      return x;

    return boost::apply_visitor(type_factory{schema_}, t.info);
  }

private:
  schema const& schema_;
  string name_;
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
        t = t->clone(td.name);
      else
        t = boost::apply_visitor(type_factory{schema_, td.name}, x->info);

      assert(t);
      schema_.add(t);
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
