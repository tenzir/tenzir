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

const std::string& argument::name() const
{
    return name_;
}

type_ptr argument::type() const
{
    return type_;
}

bool operator==(argument const& x, argument const& y)
{
    return x.name() == y.name() && *x.type() == *y.type();
}

bool operator!=(argument const& x, argument const& y)
{
    return ! (x == y);
}

std::ostream& operator<<(std::ostream& out, argument const& a)
{
    out << a.name() << ": " << a.type()->to_string();
    return out;
}

} // namespace meta
} // namespace vast
