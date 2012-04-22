#include "vast/meta/argument.h"

#include <sstream>
#include "vast/meta/type.h"

namespace vast {
namespace meta {

argument::argument(std::string const& name, type_ptr type)
  : name_(name)
  , type_(type)
{
}

bool argument::operator==(argument const& rhs) const
{
    return name_ == rhs.name_ && *type_ == *rhs.type_;
}

const std::string& argument::name() const
{
    return name_;
}

type_ptr argument::type() const
{
    return type_;
}

std::ostream& operator<<(std::ostream& out, argument const& a)
{
    out << a.name() << ": " << a.type()->to_string();
    return out;
}

} // namespace meta
} // namespace vast
