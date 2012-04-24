#include "vast/meta/event.h"

#include <ostream>
#include "vast/meta/argument.h"
#include "vast/meta/type.h"

namespace vast {
namespace meta {

event::event(std::string const& name, std::vector<argument_ptr> const& args)
  : name_(name)
  , args_(args)
{
}

event::~event()
{
}

std::string const& event::name() const
{
    return name_;
}

bool operator==(event const& x, event const& y)
{
    if (x.name_ != y.name_)
        return false;

    if (x.args_.size() != y.args_.size())
        return false;

    return std::equal(
        x.args_.begin(), x.args_.end(),
        y.args_.begin(),
        [](argument_ptr const& x, argument_ptr const& y)
        {
            return *x == *y;
        });
}

bool operator!=(event const& x, event const& y)
{
    return ! (x == y);
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
