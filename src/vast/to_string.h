#ifndef VAST_TO_STRING_H
#define VAST_TO_STRING_H

#include <string>
#include "vast/operator.h"
#include "vast/schema.h"

namespace vast {

class expression;

std::string to_string(bool b);
std::string to_string(int64_t i);
std::string to_string(uint64_t i);
std::string to_string(double d);

std::string to_string(boolean_operator op);
std::string to_string(arithmetic_operator op);
std::string to_string(relational_operator op);

std::string to_string(schema::type const& t);
std::string to_string(schema::type_info const& ti);
std::string to_string(schema::event const& e);
std::string to_string(schema::argument const& a);
std::string to_string(schema const& s);

std::string to_string(expression const& e);

} // namespace vast

#endif
