#ifndef VAST_META_FORWARD_H
#define VAST_META_FORWARD_H

#include <memory>
#include <vector>
#include <unordered_map>
#include <boost/variant/variant_fwd.hpp>

namespace vast {
namespace meta {

class taxonomy;
typedef std::shared_ptr<taxonomy> taxonomy_ptr;

class type;
typedef std::shared_ptr<type> type_ptr;
typedef std::unordered_map<std::string, type_ptr> type_map;

class event;
typedef std::shared_ptr<event> event_ptr;
typedef std::unordered_map<std::string, event_ptr> event_map;

class argument;
typedef std::shared_ptr<argument> argument_ptr;

struct unknown_type;
struct addr_type;
struct bool_type;
struct count_type;
struct double_type;
struct int_type;
struct interval_type;
struct file_type;
struct port_type;
struct string_type;
struct subnet_type;
struct time_type;
struct enum_type;
struct vector_type;
struct set_type;
struct table_type;
struct attribute;
struct argument_declaration;
struct record_type;
struct type_declaration;
struct event_declaration;

} // namespace meta
} // namespace vast

#endif
