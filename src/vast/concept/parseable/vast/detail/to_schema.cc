#include "vast/concept/parseable/vast/detail/schema.h"

namespace vast {
namespace detail {

trial<vast::schema> to_schema(std::string const& str)
{
  using iterator_type = std::string::const_iterator;
  std::string err;
  auto f = str.begin();
  auto l = str.end();
  detail::parser::error_handler<iterator_type> on_error{f, l, err};
  detail::parser::schema<iterator_type> grammar{on_error};
  detail::parser::skipper<iterator_type> skipper;
  detail::ast::schema::schema ast;
  bool success = phrase_parse(f, l, grammar, skipper, ast);
  if (! success)
    return error{std::move(err)};
  schema sch;
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
  return sch;
}

} // namespace detail
} // namespace vast
