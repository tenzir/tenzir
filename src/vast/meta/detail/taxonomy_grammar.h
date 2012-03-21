#ifndef VAST_META_DETAIL_TAXONOMY_GRAMMAR_H
#define VAST_META_DETAIL_TAXONOMY_GRAMMAR_H

#include <boost/spirit/include/qi.hpp>
// TODO: remove unncessary phoenix includes
#include <boost/spirit/include/phoenix_bind.hpp>
#include <boost/spirit/include/phoenix_container.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_fusion.hpp>
#include <boost/spirit/include/phoenix_object.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_statement.hpp>
#include <boost/spirit/include/phoenix_stl.hpp>
#include "vast/util/logger.h"
#include "vast/meta/detail/taxonomy_types.h"
#include "vast/meta/exception.h"

namespace vast {
namespace meta {
namespace detail {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

/// The error handler that is invoked for parse errors.
struct error_handler
{
    template <typename, typename, typename, typename>
    struct result { typedef void type; };

    template <typename Iterator>
    void operator()(const qi::info& what,
            Iterator first, Iterator last, Iterator err) const
    {
        Iterator sol = err;
        while (*sol != '\n' && sol != first)
            --sol;

        Iterator eol = err;
        while (*eol != '\n' && eol != last)
            ++eol;

        LOG(fatal, meta) << "parse error";
        LOG(fatal, meta) << "  -> after:     \""
            << std::string(sol + 1, err - 1) << '"';
        LOG(fatal, meta) << "  -> got:       \""
            << std::string(err, eol) << '"';
        LOG(fatal, meta) << "  -> expecting: \"" << what << '"';
    }
};

/// A functor to add types to a symbol table.
template <typename SymbolTable>
struct type_adder
{
    typedef SymbolTable table_type;
    typedef typename table_type::value_type value_type;

    template <typename, typename>
    struct result
    {
        typedef void type;
    };

    type_adder(SymbolTable& symbol_table)
      : symbol_table(symbol_table)
    {
    }

    void operator()(const std::string& symbol, value_type type) const
    {
        if (symbol_table.find(symbol))
        {
            LOG(error, meta) << "duplicate type: " << symbol;
            throw semantic_exception("duplicate symbol");
        }

        symbol_table.add(symbol, type);
    };

    table_type& symbol_table;
};

/// The skip grammar. It skips white space and comments starting with \c #.
template <typename Iterator>
struct skipper : qi::grammar<Iterator>
{
    skipper()
     : skipper::base_type(start)
    {
        using namespace boost::spirit;
        using namespace boost::spirit::qi;

        start
            =
                space
            |   '#' >> *(char_ - '\n') >> '\n'
            ;
    }

    qi::rule<Iterator> start;
};

/// The grammar of the taxonomy.
template <typename Iterator>
struct taxonomy_grammar : qi::grammar<Iterator, ast(), skipper<Iterator>>
{
    taxonomy_grammar();

    typedef qi::symbols<char, type_info> symbol_table;
    symbol_table symbols;

    qi::rule<Iterator, ast(), skipper<Iterator>> start;
    qi::rule<Iterator, type_declaration(), skipper<Iterator>> type_decl;
    qi::rule<Iterator, event_declaration(), skipper<Iterator>> event_decl;
    qi::rule<Iterator, argument_declaration(), skipper<Iterator>> argument;
    qi::rule<Iterator, attribute(), skipper<Iterator>> attr;
    qi::rule<Iterator, plain_type(), skipper<Iterator>> plain;
    qi::rule<Iterator, type_info(), skipper<Iterator>> vast_type;
    qi::rule<Iterator, unknown_type()> unknown;
    qi::rule<Iterator, addr_type()> addr;
    qi::rule<Iterator, bool_type()> bool_;
    qi::rule<Iterator, count_type()> count;
    qi::rule<Iterator, double_type()> double_;
    qi::rule<Iterator, int_type()> int_;
    qi::rule<Iterator, interval_type()> interval;
    qi::rule<Iterator, file_type()> file;
    qi::rule<Iterator, port_type()> port;
    qi::rule<Iterator, string_type()> string;
    qi::rule<Iterator, subnet_type()> subnet;
    qi::rule<Iterator, time_type()> time;
    qi::rule<Iterator, enum_type(), skipper<Iterator>> enum_;
    qi::rule<Iterator, vector_type(), skipper<Iterator>> vector;
    qi::rule<Iterator, set_type(), skipper<Iterator>> set;
    qi::rule<Iterator, table_type(), skipper<Iterator>> table;
    qi::rule<Iterator, record_type(), skipper<Iterator>> record;
    qi::rule<Iterator, std::string()> id;

    boost::phoenix::function<error_handler> handle_error;
    boost::phoenix::function<type_adder<symbol_table>> add_type;
};

} // namespace detail
} // namespace meta
} // namespace vast

#endif
