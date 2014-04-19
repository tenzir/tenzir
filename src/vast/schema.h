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
#include "vast/util/parse.h"
#include "vast/util/print.h"
#include "vast/util/trial.h"

namespace vast {
class schema : util::equality_comparable<schema>,
               util::parsable<schema>,
               util::printable<schema>
{
public:
  /// Adds a new type to the schema.
  /// @param t The type to add.
  /// @returns `true` on success and `false` if the type exists already.
  bool add(type_ptr t);

  /// Adds an event schema.
  /// @param ei The event schema.
  bool add(event_info ei);

  /// Retrieves the number of types in the schema.
  /// @returns The number of types this schema has.
  size_t types() const;

  /// Retrieves the number of events in the schema.
  /// @returns The number of events this schema has.
  size_t events() const;

  /// Retrieves the type for a given type name.
  /// @param name The name of the type to lookup.
  /// @returns The type registered as *name* or an empty pointer if *name* does
  /// not exist.
  type_ptr find_type(string const& name) const;

  /// Retrieves the type for a given event and offset.
  /// @param name The event name.
  /// @param off The event name.
  /// @returns The type at offset *off* in event *name* or `nullptr` if *name*
  /// and *off* do not resolve.
  type_ptr find_type(string const& name, offset const& off) const;

  /// Retrieves the type(s) for a given type name.
  /// @param ti The [type information](::type_info) to look for.
  /// @returns The type(s) with type information *ti*.
  std::vector<type_ptr> find_type_info(type_info const& ti) const;

  /// Retrieves <event, offset> pairs for a given argument name sequence.
  /// @param ids The arguments name sequence to look for.
  /// @returns The resolved offsets according to *ids*.
  std::multimap<string, offset>
  find_offsets(std::vector<string> const& ids) const;

  /// Retrieves the [event info](::event_info) for a given event name.
  /// @param name The name of the event to lookup.
  /// @returns The event *name* or `nullptr` if *name* does not exist.
  event_info const* find_event(string const& name) const;

  /// Checks whether a given event complies with the schema.
  /// @param e The event to test.
  /// @returns `nothing` iff *e* complies with this schema.
  //trial<nothing> complies(event const& e) const;

private:
  std::vector<type_ptr> types_;
  std::vector<event_info> events_;

private:
  friend access;
  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  template <typename Iterator>
  bool parse(Iterator& start, Iterator end);

  template <typename Iterator>
  bool print(Iterator& out) const
  {
    for (auto& t : types_)
    {
      if (t->name().empty())
        continue;

      render(out, "type ");
      render(out, t->name());
      render(out, ": ");
      render(out, *t, false);
      render(out, "\n");
    }

    if (! events_.empty())
      render(out, "\n");

    for (auto& ei : events_)
    {
      render(out, "event ");
      render(out, ei.name);
      render(out, "(");

      auto first = ei.args.begin();
      auto last = ei.args.end();
      while (first != last)
      {
        render(out, first->name);
        render(out, ": ");
        render(out, *first->type);

        if (++first != last)
          render(out, ", ");
      }

      render(out, ")\n");
    }

    return true;
  }

  friend bool operator==(schema const& x, schema const& y);
};

namespace detail {

class type_factory
{
public:
  using result_type = type_ptr;

  type_factory(schema const& s)
    : schema_{s}
  {
  }

  type_ptr operator()(detail::ast::schema::basic_type t) const
  {
    switch (t)
    {
      default:
        assert(! "missing type implementation");
      case detail::ast::schema::bool_type:
        return type::make<bool_type>();
      case detail::ast::schema::int_type:
        return type::make<int_type>();
      case detail::ast::schema::uint_type:
        return type::make<uint_type>();
      case detail::ast::schema::double_type:
        return type::make<double_type>();
      case detail::ast::schema::time_frame_type:
        return type::make<time_range_type>();
      case detail::ast::schema::time_point_type:
        return type::make<time_point_type>();
      case detail::ast::schema::string_type:
        return type::make<string_type>();
      case detail::ast::schema::regex_type:
        return type::make<regex_type>();
      case detail::ast::schema::address_type:
        return type::make<address_type>();
      case detail::ast::schema::prefix_type:
        return type::make<prefix_type>();
      case detail::ast::schema::port_type:
        return type::make<port_type>();
    }
  }

  type_ptr operator()(detail::ast::schema::enum_type const& t) const
  {
    std::vector<string> v;
    for (auto& str : t.fields)
      v.emplace_back(str);

    return type::make<enum_type>(std::move(v));
  }

  type_ptr operator()(detail::ast::schema::vector_type const& t) const
  {
    return type::make<vector_type>(make_type(t.element_type));
  }

  type_ptr operator()(detail::ast::schema::set_type const& t) const
  {
    return type::make<set_type>(make_type(t.element_type));
  }

  type_ptr operator()(detail::ast::schema::table_type const& t) const
  {
    auto k = make_type(t.key_type);
    auto y = make_type(t.value_type);
    return type::make<table_type>(k, y);
  }

  type_ptr operator()(detail::ast::schema::record_type const& t) const
  {
    record_type r;
    for (auto& arg : t.args)
      r.args.emplace_back(make_argument(arg));

    return type::make<record_type>(std::move(r));
  }

  argument make_argument(
      detail::ast::schema::argument_declaration const& a) const
  {
    return {a.name, make_type(a.type)};
  }

  type_ptr make_type(detail::ast::schema::type const& t) const
  {
    if (auto x = schema_.find_type(t.name))
      return x;

    auto x = boost::apply_visitor(*this, t.info);
    x->name(t.name);

    return x;
  }

private:
  schema const& schema_;
};

struct schema_factory
{
  using result_type = void;

  schema_factory(schema& s)
    : tf_{s},
      schema_{s}
  {
  }

  void operator()(detail::ast::schema::type_declaration const& td)
  {
    if (auto ti = boost::get<detail::ast::schema::type_info>(&td.type))
    {
      auto t = boost::apply_visitor(std::ref(tf_), *ti);
      t->name(td.name);
      if (! schema_.add(t))
        error_ = error{"erroneous type declaration: " + td.name};
    }
    else if (auto t = boost::get<detail::ast::schema::type>(&td.type))
    {
      auto x = schema_.find_type(t->name);
      if (x)
        x = x->clone();
      else
        x = boost::apply_visitor(std::ref(tf_), t->info);

      assert(x);
      x->name(td.name);
      schema_.add(x);
    }
  }

  void operator()(detail::ast::schema::event_declaration const& ed)
  {
    event_info ei;
    ei.name = ed.name;

    if (ed.args)
      for (auto& arg : *ed.args)
        ei.args.emplace_back(tf_.make_argument(arg));

    schema_.add(std::move(ei));
  }

  type_factory tf_;
  schema& schema_;
  error error_;
};

} // namespace detail

template <typename Iterator>
bool schema::parse(Iterator& start, Iterator end)
{
  std::string err;
  detail::parser::error_handler<Iterator> on_error{start, end, err};
  detail::parser::schema<Iterator> grammar{on_error};
  detail::parser::skipper<Iterator> skipper;
  detail::ast::schema::schema ast;

  bool success = phrase_parse(start, end, grammar, skipper, ast);
  if (! success || start != end)
    return false;
    // TODO: return error{std::move(err)};

  detail::schema_factory sf{*this};
  for (auto& statement : ast)
  {
    boost::apply_visitor(std::ref(sf), statement);
    if (! sf.error_.msg().empty())
      return false;
      // TODO: return error{*sf.error_};
  }

  return true;
}

} // namespace vast

#endif
