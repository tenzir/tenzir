#include "vast/detail/parser/value_definition.h"

typedef std::string::const_iterator iterator_type;
template struct vast::detail::parser::value<iterator_type>;
