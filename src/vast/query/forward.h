#ifndef VAST_QUERY_FORWARD_H
#define VAST_QUERY_FORWARD_H

#include <ze/forward.h>

namespace vast {
namespace query {

class query;
class meta_query;
class type_query;
class taxonomy_query;
typedef ze::intrusive_ptr<query> query_ptr;

} // namespace query
} // namespace vast

#endif
