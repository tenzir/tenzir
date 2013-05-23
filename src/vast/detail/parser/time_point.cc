#include "vast/detail/parser/time_point_definition.h"

typedef std::string::const_iterator iterator_type;
template struct vast::detail::parser::time_point<iterator_type>;
