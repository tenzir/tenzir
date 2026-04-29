#pragma once

// nats.c's libevent adapter includes <event.h>. Some static dependency
// closures also contain libev's compatibility header with the same name, so
// keep this include path ahead of dependency include paths for the NATS plugin.
//
// Folly still relies on libevent's deprecated compatibility declarations, so
// this shim mirrors the compatibility surface instead of only forwarding to
// <event2/event.h>.
#include <event2/buffer.h>
#include <event2/buffer_compat.h>
#include <event2/bufferevent.h>
#include <event2/bufferevent_compat.h>
#include <event2/bufferevent_struct.h>
#include <event2/event.h>
#include <event2/event_compat.h>
#include <event2/event_struct.h>
#include <event2/tag.h>
#include <event2/tag_compat.h>
#include <event2/util.h>

#include <stdarg.h>
