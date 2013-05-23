#include "vast/detail/parser/duration_definition.h"

typedef std::string::const_iterator iterator_type;
template struct vast::detail::parser::duration<iterator_type>;
