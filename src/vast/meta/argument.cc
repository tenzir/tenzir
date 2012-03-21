#include "vast/meta/argument.h"

#include <sstream>
#include "vast/meta/type.h"

namespace vast {
namespace meta {

argument::argument(const std::string& name, type_ptr type)
  : name_(name)
  , type_(type)
{
}

bool argument::operator==(const argument& rhs) const
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

std::string argument::to_string() const
{
    std::stringstream ss;
    ss << name_ << ": " << type_->to_string();
    return ss.str();
}

} // namespace meta
} // namespace vast
