#include "vast/meta/detail/taxonomy_grammar_ctor.h"

namespace vast {
namespace meta {
namespace detail {
namespace {

// This function is actually not called, its only purpose is to instantiate the
// constructor of the grammar in order to reduce the compile time. It makes it
// possible to change the files including the grammar without needing recompile
// the insane template-heavy grammar code.
void instantiate()
{
    taxonomy_grammar<std::string::const_iterator> grammar;
}

} // namespace
} // namespace detail
} // namespace meta
} // namespace vast
