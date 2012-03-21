#include "vast/meta/detail/taxonomy_generator_ctor.h"

namespace vast {
namespace meta {
namespace detail {
namespace {

// This function is actually not called, its only purpose is to instantiate the
// constructor of the generator in order to reduce the compile time. It makes it
// possible to change the files including the generator without needing
// recompile the insane template-heavy generator code.
void instantiate()
{
    typedef std::back_insert_iterator<std::string> iterator_type;
    taxonomy_generator<iterator_type> generator;
}

} // namespace
} // namespace detail
} // namespace meta
} // namespace vast
