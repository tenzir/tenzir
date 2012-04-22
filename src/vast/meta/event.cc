#include "vast/meta/event.h"

#include "vast/meta/argument.h"
#include "vast/meta/type.h"

namespace vast {
namespace meta {

event::event(const std::string& name, const std::vector<argument_ptr>& args)
  : name_(name)
  , args_(args)
{
}

event::~event()
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

std::ostream& operator<<(std::ostream& out, event const& e)
{
    out << e.name();
    out << '(';
    auto first = e.args_.begin();
    auto last = e.args_.end();
    while (first != last)
    {
        out << **first;
        if (++first != last)
            out << ", ";
    }
    out << ')';

    return out;
}

} // namespace meta
} // namespace vast
