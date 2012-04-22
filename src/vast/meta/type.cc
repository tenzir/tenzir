#include "vast/meta/type.h"

#include <sstream>
#include "vast/meta/argument.h"

namespace vast {
namespace meta {

type::type()
{
    // TODO: compute and save the type checksum.
}

type::~type()
{
}

bool type::operator==(type const& rhs) const
{
    return checksum_ == rhs.checksum_;
}

bool type::is_symbol() const
{
    return ! aliases_.empty();
}

std::string type::name() const
{
    return is_symbol() ? aliases_.back() : "";
}

type_ptr type::symbolize(std::string const& name)
{
    type* t = is_symbol() ? clone() : this;
    t->aliases_.push_back(name);
    return t;
}

std::string type::to_string(bool resolve) const
{
    if (! is_symbol())
        return to_string_impl();
    else if (aliases_.size() == 1)
        return resolve ? to_string_impl() : aliases_.back();
    else
    {
        assert(aliases_.size() > 1);
        return  *(aliases_.end() - (resolve ? 2 : 1));
    }
}

basic_type::basic_type()
  : type()
{
}

basic_type::~basic_type()
{
}

complex_type::complex_type()
  : type()
{
}

complex_type::~complex_type()
{
}

container_type::container_type()
  : complex_type()
{
}

container_type::~container_type()
{
}

#define VAST_DEFINE_TYPE(t, parent)                                 \
    t::~t()                                                         \
    {                                                               \
    }                                                               \
                                                                    \
    t::t()                                                          \
      : parent()                                                    \
    {                                                               \
    }                                                               \
                                                                    \
    t* t::clone() const                                             \
    {                                                               \
        return new t(*this);                                        \
    }

VAST_DEFINE_TYPE(bool_type, basic_type)
VAST_DEFINE_TYPE(int_type, basic_type)
VAST_DEFINE_TYPE(uint_type, basic_type)
VAST_DEFINE_TYPE(double_type, basic_type)
VAST_DEFINE_TYPE(duration_type, basic_type)
VAST_DEFINE_TYPE(timepoint_type, basic_type)
VAST_DEFINE_TYPE(string_type, basic_type)
VAST_DEFINE_TYPE(regex_type, basic_type)
VAST_DEFINE_TYPE(address_type, basic_type)
VAST_DEFINE_TYPE(prefix_type, basic_type)
VAST_DEFINE_TYPE(port_type, basic_type)
VAST_DEFINE_TYPE(enum_type, complex_type)
VAST_DEFINE_TYPE(record_type, complex_type)
VAST_DEFINE_TYPE(vector_type, container_type)
VAST_DEFINE_TYPE(set_type, container_type)
VAST_DEFINE_TYPE(table_type, container_type)

#define VAST_DEFINE_TYPE_TO_STRING_IMPL(type, str)      \
    std::string type::to_string_impl() const            \
    {                                                   \
        return str;                                     \
    }

VAST_DEFINE_TYPE_TO_STRING_IMPL(bool_type, "bool")
VAST_DEFINE_TYPE_TO_STRING_IMPL(int_type, "int")
VAST_DEFINE_TYPE_TO_STRING_IMPL(uint_type, "uint")
VAST_DEFINE_TYPE_TO_STRING_IMPL(double_type, "double")
VAST_DEFINE_TYPE_TO_STRING_IMPL(duration_type, "duration")
VAST_DEFINE_TYPE_TO_STRING_IMPL(timepoint_type, "timepoint")
VAST_DEFINE_TYPE_TO_STRING_IMPL(string_type, "string")
VAST_DEFINE_TYPE_TO_STRING_IMPL(regex_type, "pattern")
VAST_DEFINE_TYPE_TO_STRING_IMPL(address_type, "addr")
VAST_DEFINE_TYPE_TO_STRING_IMPL(prefix_type, "subnet")
VAST_DEFINE_TYPE_TO_STRING_IMPL(port_type, "port")

std::string enum_type::to_string_impl() const
{
    std::stringstream ss;
    ss << "enum {";
    auto first = fields.begin();
    auto last = fields.end();
    while (first != last)
    {
        ss << *first;
        if (++first != last)
            ss << ", ";
    }
    ss << '}';

    return ss.str();
}

std::string record_type::to_string_impl() const
{
    std::stringstream ss;
    ss << "record {";
    auto first = args.begin();
    auto last = args.end();
    while (first != last)
    {
        ss << **first;
        if (++first != last)
            ss << ", ";
    }
    ss << '}';

    return ss.str();
}

std::string vector_type::to_string_impl() const
{
    std::string str("vector of ");
    str += elem_type->to_string();

    return str;
}

std::string set_type::to_string_impl() const
{
    std::string str("set[");
    str += elem_type->to_string();
    str += ']';

    return str;
}

std::string table_type::to_string_impl() const
{
    std::string str("table[");
    str += key_type->to_string();
    str += "] of ";
    str += value_type->to_string();

    return str;
}

} // namespace meta
} // namespace vast
