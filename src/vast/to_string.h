#ifndef VAST_TO_STRING_H
#define VAST_TO_STRING_H

#include <string>
#include "vast/schema.h"

namespace vast {

class expression;

std::string to_string(schema::type const& t);
std::string to_string(schema::type_info const& ti);
std::string to_string(schema::event const& e);
std::string to_string(schema::argument const& a);
std::string to_string(schema const& s);
std::string to_string(expression const& e);

} // namespace vast

#endif
