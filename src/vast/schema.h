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
      print(" = ", out);
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

inline std::vector<type::attribute> make_attrs(
    std::vector<ast::schema::attribute> const& attrs)
{
  std::vector<type::attribute> r;
  for (auto& a : attrs)
  {
    auto key = type::attribute::invalid;
    if (a.key == "skip")
      key = type::attribute::skip;
    else if (a.key == "default")
      key = type::attribute::default_;

    std::string value;
    if (a.value)
      value = *a.value;

    r.emplace_back(key, std::move(value));
  }

  return r;
}

class type_factory
{
public:
  using result_type = trial<type>;

  type_factory(schema const& s, std::vector<ast::schema::attribute> const& as)
    : schema_{s},
      attrs_{make_attrs(as)}
  {
  }

  trial<type> operator()(std::string const& type_name) const
  {
    if (auto x = schema_.find_type(type_name))
      return *x;
    else
      return error{"unknown type: ", type_name};
  }

  trial<type> operator()(ast::schema::basic_type bt) const
  {
    switch (bt)
    {
      default:
        return error{"missing type implementation"};
      case ast::schema::bool_type:
        return {type::boolean{attrs_}};
      case ast::schema::int_type:
        return {type::integer{attrs_}};
      case ast::schema::uint_type:
        return {type::count{attrs_}};
      case ast::schema::double_type:
        return {type::real{attrs_}};
      case ast::schema::time_point_type:
        return {type::time_point{attrs_}};
      case ast::schema::time_frame_type:
        return {type::time_duration{attrs_}};
      case ast::schema::string_type:
        return {type::string{attrs_}};
      case ast::schema::regex_type:
        return {type::pattern{attrs_}};
      case ast::schema::address_type:
        return {type::address{attrs_}};
      case ast::schema::prefix_type:
        return {type::subnet{attrs_}};
      case ast::schema::port_type:
        return {type::port{attrs_}};
    }
  }

  trial<type> operator()(ast::schema::enum_type const& t) const
  {
    return {type::enumeration{t.fields, attrs_}};
  }

  trial<type> operator()(ast::schema::vector_type const& t) const
  {
    auto elem = make_type(t.element_type);
    if (! elem)
      return elem;

    return {type::vector{std::move(*elem), attrs_}};
  }

  trial<type> operator()(ast::schema::set_type const& t) const
  {
    auto elem = make_type(t.element_type);
    if (! elem)
      return elem;

    return {type::set{std::move(*elem), attrs_}};
  }

  trial<type> operator()(ast::schema::table_type const& t) const
  {
    auto k = make_type(t.key_type);
    if (! k)
      return k;

    auto v = make_type(t.value_type);
    if (! v)
      return v;

    return {type::table{std::move(*k), std::move(*v), attrs_}};
  }

  trial<type> operator()(ast::schema::record_type const& t) const
  {
    std::vector<type::record::field> fields;
    for (auto& arg : t.args)
    {
      auto arg_type = make_type(arg.type);
      if (! arg_type)
        return arg_type;

      fields.push_back({arg.name, std::move(*arg_type)});
    }

    return {type::record{std::move(fields), attrs_}};
  }

  trial<type> make_type(ast::schema::type const& t) const
  {
    return boost::apply_visitor(type_factory{schema_, t.attrs}, t.info);
  }

private:
  schema const& schema_;
  std::vector<type::attribute> attrs_;
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

  bool success = phrase_parse(begin, end, grammar, skipper, ast);
  if (! success || begin != end)
    return error{std::move(err)};

  sch.types_.clear();
  for (auto& type_decl : ast)
  {
    // If we have a top-level identifier, we're dealing with a type alias.
    // Everywhere else (e.g., inside records or table types), and identifier
    // will be resolved to the corresponding type.
    if (auto id = boost::get<std::string>(&type_decl.type.info))
    {
      auto t = sch.find_type(*id);
      if (! t)
        return error{"unkonwn type: ", *id};

      auto a = type::alias{*t, detail::make_attrs(type_decl.type.attrs)};
      a.name(type_decl.name);

      if (! sch.add(std::move(a)))
        return error{"failed to add type alias", *id};
    }

    auto t = boost::apply_visitor(
        detail::type_factory{sch, type_decl.type.attrs}, type_decl.type.info);

    if (! t)
      return t.error();

    t->name(type_decl.name);

    if (! sch.add(std::move(*t)))
      return error{"failed to add type declaration", type_decl.name};
  }

  return nothing;
}

} // namespace vast

#endif
