#include "vast/meta/event.h"

#include <sstream>
#include "vast/meta/argument.h"

namespace vast {
namespace meta {

event::event(const std::string& name, const std::vector<argument_ptr>& args)
  : name_(name)
  , args_(args)
{
}

bool event::operator==(const event& rhs) const
{
    if (name_ != rhs.name_)
        return false;

    if (args_.size() != rhs.args_.size())
        return false;

    for (std::size_t i = 0; i < args_.size(); ++i)
        if (*args_[i] != *rhs.args_[i])
            return false;

    return true;
}

const std::string& event::name() const
{
    return name_;
}

std::string event::to_string() const
{
    std::stringstream ss;
    ss << '(';

    auto first = args_.begin();
    auto last = args_.end();
    while (first != last)
    {
        ss << (*first)->to_string();
        if (++first != last)
            ss << ", ";
    }

    ss << ')';

    return ss.str();
}

} // namespace meta
} // namespace vast
