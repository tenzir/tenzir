#ifndef VAST_META_DETAIL_TAXONOMY_GENERATOR_H
#define VAST_META_DETAIL_TAXONOMY_GENERATOR_H

#include <boost/spirit/include/karma.hpp>
#include "vast/meta/detail/taxonomy_types.h"

namespace vast {
namespace meta {
namespace detail {

namespace karma = boost::spirit::karma;

template <typename Iterator>
struct taxonomy_generator : karma::grammar<Iterator, ast()>
{
    taxonomy_generator();

    karma::rule<Iterator, ast()> start;
    karma::rule<Iterator, type_declaration()> type_decl;
    karma::rule<Iterator, event_declaration()> event_decl;
    karma::rule<Iterator, argument_declaration()> argument;
    karma::rule<Iterator, attribute()> attr;
    karma::rule<Iterator, plain_type()> plain;
    karma::rule<Iterator, type_info()> vast_type;
    karma::rule<Iterator, unknown_type()> unknown;
    karma::rule<Iterator, addr_type()> addr;
    karma::rule<Iterator, bool_type()> bool_;
    karma::rule<Iterator, count_type()> count;
    karma::rule<Iterator, double_type()> double_;
    karma::rule<Iterator, int_type()> int_;
    karma::rule<Iterator, interval_type()> interval;
    karma::rule<Iterator, file_type()> file;
    karma::rule<Iterator, port_type()> port;
    karma::rule<Iterator, string_type()> string;
    karma::rule<Iterator, subnet_type()> subnet;
    karma::rule<Iterator, time_type()> time;
    karma::rule<Iterator, enum_type()> enum_;
    karma::rule<Iterator, vector_type()> vector;
    karma::rule<Iterator, set_type()> set;
    karma::rule<Iterator, table_type()> table;
    karma::rule<Iterator, record_type()> record;
};

} // namespace detail
} // namespace meta
} // namespace vast

#endif
