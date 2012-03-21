#include "vast/meta/taxonomy_visitor.h"

#include "vast/meta/argument.h"
#include "vast/meta/event.h"
#include "vast/meta/type.h"
#include "vast/util/logger.h"

namespace vast {
namespace meta {

// Create an argument from an argument declaration.
static argument_ptr make_arg(
        const type_map& types,
        const detail::argument_declaration& ad)
{
    type_ptr t = boost::apply_visitor(type_creator(types), ad.type);
    argument_ptr arg(new argument(ad.name, t));
    // TODO: parse attributes and change argument accordingly.
    if (ad.attrs)
        ;

    return arg;
}


taxonomy_visitor::taxonomy_visitor(type_map& types, event_map& events)
  : types_(types)
  , events_(events)
{
}

void taxonomy_visitor::build(const detail::ast& ast) const
{
    for (const auto& statement : ast)
        boost::apply_visitor(*this, statement);
}

void taxonomy_visitor::operator()(const detail::type_declaration& td) const
{
    type_ptr t = boost::apply_visitor(type_creator(types_), td.type);
    t = t->symbolize(td.name);

    // FIXME: uncomment as soon as the GNU STL implements emplace.
    //types_.emplace(t->name(), t);
    types_.insert(type_map::value_type(t->name(), t));

    LOG(debug, meta) << "new type '" << t->name() << "': " << t->to_string();
}

void taxonomy_visitor::operator()(const detail::event_declaration& ed) const
{
    std::vector<argument_ptr> args;
    if (ed.args)
        for (const auto& arg : *ed.args)
            args.emplace_back(make_arg(types_, arg));

    event_ptr e(new event(ed.name, args));
    // FIXME: uncomment as soon as the GNU STL implements emplace.
    //events_.emplace(e->name(), e);
    events_.insert(event_map::value_type(e->name(), e));

    LOG(debug, meta) << "new event '" << e->name() << "': " << e->to_string();
}


type_creator::type_creator(const type_map& types)
  : types_(types)
{
}

type_ptr type_creator::operator()(const detail::type_info& info) const
{
    return boost::apply_visitor(*this, info);
}

type_ptr type_creator::operator()(const std::string& str) const
{
    auto i = types_.find(str);
    assert(i != types_.end());

    return i->second;
}

type_ptr type_creator::operator()(const detail::plain_type& t) const
{
    return boost::apply_visitor(*this, t);
}

#define VAST_CREATE_POLYMORPHIC_TYPE(from, to)                             \
    type_ptr type_creator::operator()(const from& t) const                 \
    {                                                                      \
        return type_ptr(new to);                                           \
    }

VAST_CREATE_POLYMORPHIC_TYPE(detail::unknown_type, unknown_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::addr_type, addr_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::bool_type, bool_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::count_type, count_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::double_type, double_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::int_type, int_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::interval_type, interval_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::file_type, file_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::port_type, port_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::string_type, string_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::subnet_type, subnet_type)
VAST_CREATE_POLYMORPHIC_TYPE(detail::time_type, time_type)

type_ptr type_creator::operator()(const detail::enum_type& t) const
{
    enum_type* e = new enum_type;
    e->fields = t.fields;

    return type_ptr(e);
}

type_ptr type_creator::operator()(const detail::vector_type& t) const
{
    vector_type* v = new vector_type;
    v->elem_type = boost::apply_visitor(*this, t.element_type);

    return type_ptr(v);
}

type_ptr type_creator::operator()(const detail::set_type& t) const
{
    set_type* s = new set_type;
    s->elem_type = boost::apply_visitor(*this, t.element_type);

    return type_ptr(s);
}

type_ptr type_creator::operator()(const detail::table_type& t) const
{
    table_type* tt = new table_type;
    tt->key_type = boost::apply_visitor(*this, t.key_type);
    tt->value_type = boost::apply_visitor(*this, t.value_type);

    return type_ptr(tt);
}

type_ptr type_creator::operator()(const detail::record_type& t) const
{
    record_type* r = new record_type;
    for (const auto& arg : t.args)
        r->args.emplace_back(make_arg(types_, arg));

    return type_ptr(r);
}

} // namespace meta
} // namespace vast
