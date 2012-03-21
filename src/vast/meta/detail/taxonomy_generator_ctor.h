#ifndef VAST_META_DETAIL_TAXONOMY_GENERATOR_CTOR_H
#define VAST_META_DETAIL_TAXONOMY_GENERATOR_CTOR_H

#include "vast/meta/detail/taxonomy_generator.h"

namespace vast {
namespace meta {
namespace detail {

template <typename Iterator>
taxonomy_generator<Iterator>::taxonomy_generator()
  : taxonomy_generator::base_type(start)
{
    using namespace karma::labels;
    using karma::eps;
    using karma::lit;

    start
        =  *(  type_decl
            |  event_decl
            )
        ;

    event_decl
        =   lit("event ")
        <<  karma::string
        <<  "("
        <<  -(argument % ", ")
        <<  ')'
        <<  '\n'
        ;

    type_decl
        =   lit("type ")
        <<  karma::string
        <<  ": "
        <<  vast_type
        <<  '\n'
        ;

    argument
        =   karma::string
        <<  ": "
        <<  vast_type
        <<  -(' ' << +attr)
        ;

    attr
        =   lit('&')
        <<  karma::string
        <<  -(  '"'
            <<  karma::string
            <<  '"'
             )
        ;

    addr       = eps << "addr";
    bool_      = eps << "bool";
    count      = eps << "count";
    double_    = eps << "double";
    int_       = eps << "int";
    interval   = eps << "interval";
    file       = eps << "file";
    port       = eps << "port";
    string     = eps << "string";
    subnet     = eps << "subnet";
    time       = eps << "time";

    plain
        =   addr
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
        =   karma::string
        |   plain
        ;

    enum_
        =   lit("enum {")
        <<  karma::string % ','
        <<  '}'
        ;

    vector
        =   lit("vector of ")
        <<  vast_type
        ;

    set
        =   lit("set[")
        <<  vast_type
        <<  ']'
        ;

    table
        =   lit("table[")
        <<  vast_type
        <<  "] of "
        <<  vast_type
        ;

    record
        =   lit("record ")
        <<  "{ "
        <<  argument % ", "
        <<  " }"
        ;
}

} // namespace detail
} // namespace meta
} // namespace vast

#endif
