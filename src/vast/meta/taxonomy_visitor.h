#ifndef VAST_META_TAXONOMY_VISITOR_H
#define VAST_META_TAXONOMY_VISITOR_H

#include "vast/meta/detail/taxonomy_types.h"
#include "vast/meta/forward.h"

namespace vast {
namespace meta {

/// Populates the type and event tables from a taxonomy AST. 
class taxonomy_visitor : public boost::static_visitor<>
{
public:
    taxonomy_visitor(type_map& types, event_map& events);

    /// Builds the meta objects from a taxonomy AST.
    /// @param ast The taxonomy AST.
    void build(const detail::ast& a) const;

    void operator()(const detail::statement& stmt) const;
    void operator()(const detail::type_declaration& td) const;
    void operator()(const detail::event_declaration& ed) const;

private:
    type_map& types_;
    event_map& events_;
};

/// Create a type from a taxonomy type. Conceptually, this visitor is a factory
/// that creates the right polymorphic vast::meta::type based on the type
/// details from the taxonomy AST.
class type_creator : public boost::static_visitor<type_ptr>
{
public:
    type_creator(const type_map& types);

    type_ptr operator()(const detail::type_info& info) const;
    type_ptr operator()(const std::string& str) const;
    type_ptr operator()(const detail::plain_type& t) const;

    type_ptr operator()(const detail::unknown_type& t) const;
    type_ptr operator()(const detail::addr_type& t) const;
    type_ptr operator()(const detail::bool_type& t) const;
    type_ptr operator()(const detail::count_type& t) const;
    type_ptr operator()(const detail::double_type& t) const;
    type_ptr operator()(const detail::int_type& t) const;
    type_ptr operator()(const detail::interval_type& t) const;
    type_ptr operator()(const detail::file_type& t) const;
    type_ptr operator()(const detail::port_type& t) const;
    type_ptr operator()(const detail::string_type& t) const;
    type_ptr operator()(const detail::subnet_type& t) const;
    type_ptr operator()(const detail::time_type& t) const;
    type_ptr operator()(const detail::enum_type& t) const;
    type_ptr operator()(const detail::vector_type& t) const;
    type_ptr operator()(const detail::set_type& t) const;
    type_ptr operator()(const detail::table_type& t) const;
    type_ptr operator()(const detail::record_type& t) const;

private:
    const type_map& types_;
};


} // namespace meta
} // namespace vast

#endif
