#ifndef VAST_META_DETAIL_TAXONOMY_GRAMMAR_CTOR_H
#define VAST_META_DETAIL_TAXONOMY_GRAMMAR_CTOR_H

#include "vast/meta/detail/taxonomy_grammar.h"

namespace vast {
namespace meta {
namespace detail {

template <typename Iterator>
taxonomy_grammar<Iterator>::taxonomy_grammar()
  : taxonomy_grammar::base_type(start)
  , add_type(symbols)
{
    namespace phx = boost::phoenix;
    using boost::spirit::_1;
    using boost::spirit::_2;
    using boost::spirit::_3;
    using boost::spirit::_4;
    using namespace qi::labels;
    using qi::on_error;
    using qi::eps;
    using qi::fail;
    using qi::lexeme;
    using qi::lit;
    using qi::raw;
    using ascii::alnum;
    using ascii::alpha;
    using ascii::char_;
    using ascii::space;

    start
        %= *(  type_decl
            |  event_decl
            )
        ;

    event_decl
        %=  lit("event")
        >   id
        >   '('
        >   -(argument % ',')
        >   ')'
        ;

    type_decl
        %=  lit("type")
        >   id
        >   ':'
        >   vast_type
        >   eps         [add_type(phx::at_c<0>(_val), phx::at_c<1>(_val))]
        ;

    argument
        %=  id
        >   ':'
        >   vast_type
        >   -(+attr)
        ;

    attr
        %= lexeme
           [
                '&'
            >   id
            >   -(  '='
                 >  (( '"'
                       > *(char_ - '"')
                       > '"' ) // Extra parentheses remove compiler warning.
                    |  +(char_ - space)
                    )
                 )
           ]
        ;

    plain
        %=  unknown
        |   addr
        |   bool_
        |   count
        |   double_
        |   interval
        |   int_
        |   file
        |   port
        |   string
        |   subnet
        |   time
        |   enum_
        |   vector
        |   set
        |   table
        |   record
        ;

    vast_type
        =   raw[symbols]   [_val = phx::construct<std::string>(
                                    phx::begin(_1), phx::end(_1))]
        |   plain          [_val = _1]
        ;

    // Without eps, Boost.Spirit passes an iterator range to the constructor of
    // these types, which inevitably fails because such a constructor does not
    // exist.
    unknown    = eps(false);
    addr       = eps >> "addr";
    bool_      = eps >> "bool";
    count      = eps >> "count";
    double_    = eps >> "double";
    int_       = eps >> "int";
    interval   = eps >> "interval";
    file       = eps >> "file";
    port       = eps >> "port";
    string     = eps >> "string";
    subnet     = eps >> "subnet";
    time       = eps >> "time";

    enum_
        %=  lit("enum")
        >   '{'
        >   id % ','
        >   '}'
        ;

    vector
        %=  lit("vector")
        >   "of"
        >   vast_type
        ;

    set
        %=  lit("set")
        >   '['
        >   vast_type
        >   ']'
        ;

    table
        %=  lit("table")
        >   '['
        >   vast_type
        >   ']'
        >   "of"
        >   vast_type
        ;

    record
        %=  lit("record")
        >   '{'
        >   argument % ','
        >   '}'
        ;

    id
        %=  alpha >> *(alnum | char_('_') | char_('-'))
        ;

    on_error<fail>(start, handle_error(_4, _1, _2, _3));
    on_error<fail>(type_decl, handle_error(_4, _1, _2, _3));
    on_error<fail>(event_decl, handle_error(_4, _1, _2, _3));
    on_error<fail>(argument, handle_error(_4, _1, _2, _3));
    on_error<fail>(attr, handle_error(_4, _1, _2, _3));
    on_error<fail>(plain, handle_error(_4, _1, _2, _3));
    on_error<fail>(vast_type, handle_error(_4, _1, _2, _3));

    start.name("taxonomy");
    type_decl.name("type declaration");
    event_decl.name("event declaration");
    argument.name("argument");
    attr.name("attribute");
    plain.name("type");
    vast_type.name("VAST type");
    unknown.name("unknown type");
    addr.name("addr type");
    bool_.name("bool type");
    count.name("count type");
    double_.name("double type");
    int_.name("int type");
    interval.name("interval type");
    file.name("file type");
    port.name("port type");
    string.name("string type");
    subnet.name("subnet type");
    time.name("time type");
    enum_.name("enum type");
    vector.name("vector type");
    set.name("set type");
    table.name("table type");
    record.name("record type");
    id.name("identifier");
}

} // namespace detail
} // namespace meta
} // namespace vast

#endif
