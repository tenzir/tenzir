#ifndef VAST_META_FORWARD_H
#define VAST_META_FORWARD_H

#include <ze/forward.h>

namespace vast {
namespace meta {

class argument;
class event;
class taxonomy;
class type;

typedef ze::intrusive_ptr<argument> argument_ptr;
typedef ze::intrusive_ptr<event> event_ptr;
typedef ze::intrusive_ptr<type> type_ptr;

} // namespace meta
} // namespace vast

#endif
