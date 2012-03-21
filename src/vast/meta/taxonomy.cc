#include "vast/meta/taxonomy.h"

#include "vast/fs/fstream.h"
#include "vast/meta/detail/taxonomy_grammar.h"
#include "vast/meta/detail/taxonomy_grammar_ctor.h"
#include "vast/meta/detail/taxonomy_generator.h"
#include "vast/meta/detail/taxonomy_generator_ctor.h"
#include "vast/meta/argument.h"
#include "vast/meta/exception.h"
#include "vast/meta/event.h"
#include "vast/meta/taxonomy_visitor.h"
#include "vast/meta/type.h"
#include "vast/util/logger.h"

namespace vast {
namespace meta {

void taxonomy::load(const std::string& contents)
{
    LOG(verbose, meta) << "parsing taxonomy";

    detail::ast ast;
    detail::taxonomy_grammar<std::string::const_iterator> grammar;
    detail::skipper<std::string::const_iterator> skipper;

    auto i = contents.begin();
    auto end = contents.end();
    bool r = boost::spirit::qi::phrase_parse(i, end, grammar, skipper, ast);

    if (! r || i != end)
        throw syntax_exception();

    taxonomy_visitor visitor(types_, events_);
    visitor.build(ast);

    typedef std::back_insert_iterator<std::string> iterator_type;
    detail::taxonomy_generator<iterator_type> generator;
    iterator_type j(ast_);
    r = boost::spirit::karma::generate(j, generator, ast);

    if (! r)
    {
        LOG(error, meta) << "could not generate taxonomy AST";
        throw taxonomy_exception();
    }
}

void taxonomy::load(const fs::path& filename)
{
    fs::ifstream in(filename);

    std::string storage;
    in.unsetf(std::ios::skipws); // No white space skipping.
    std::copy(std::istream_iterator<char>(in),
            std::istream_iterator<char>(),
            std::back_inserter(storage));

    load(storage);
}

void taxonomy::save(const fs::path& filename) const
{
    fs::ofstream(filename) << to_string();
}

std::string taxonomy::to_string() const
{
    // TODO: process types and events rather than just returning the
    // user-supplied taxonomy.

    return ast_;
}

} // namespace meta
} // namespace vast
