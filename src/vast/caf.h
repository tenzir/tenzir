#ifndef VAST_CAF_H
#define VAST_CAF_H

#include <ostream>

#include <caf/all.hpp>
#include <caf/experimental/whereis.hpp>
#include <caf/scheduler/profiled_coordinator.hpp>

#include "vast/logger.h"
#include "vast/util/assert.h"

namespace caf {

template <typename Char, typename Traits>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& out, actor_addr const& a) {
  out << '#' << a.id();
  return out;
}

template <typename Char, typename Traits>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& out, actor const& a) {
  out << a.address();
  return out;
}

template <typename Char, typename Traits>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& out, abstract_actor const& a) {
  out << a.address();
  return out;
}

template <typename Char, typename Traits, typename T, typename Base>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& out,
           stateful_actor<T, Base> const& a) {
  out << a.name() << a.address();
  return out;
}

template <typename Stream, typename T, typename Base>
Stream& operator<<(Stream& out, stateful_actor<T, Base> const* a) {
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

template <typename Stream>
Stream& operator<<(Stream& out, actor const* a) {
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

template <typename Stream>
Stream& operator<<(Stream& out, abstract_actor const* a) {
  VAST_ASSERT(a != nullptr);
  out << *a;
  return out;
}

} // namespace caf

namespace vast {

// Hoist various CAF types that we consider family in VAST. <3

using caf::abstract_uniform_type_info;
using caf::actor;
using caf::actor_addr;
using caf::actor_cast;
using caf::after;
using caf::arg_match;
using caf::anon_send;
using caf::any_vals;
using caf::arg_match;
using caf::atom;
using caf::behavior;
using caf::detached;
using caf::down_msg;
using caf::either;
using caf::event_based_actor;
using caf::exit_msg;
using caf::invalid_actor;
using caf::keep_behavior;
using caf::linked;
using caf::local_actor;
using caf::make_message;
using caf::message;
using caf::message_builder;
using caf::message_handler;
using caf::message_priority;
using caf::monitored;
using caf::on;
using caf::optional;
using caf::others;
using caf::priority_aware;
using caf::reacts_to;
using caf::replies_to;
using caf::response_promise;
using caf::scoped_actor;
using caf::set_scheduler;
using caf::skip_message;
using caf::spawn;
using caf::to_string;
using caf::typed_actor;
using caf::typed_response_promise;
using caf::unit;
using caf::uniform_type_info;
using caf::val;

using caf::detail::make_scope_guard;

using caf::stateful_actor;
using caf::experimental::whereis;

namespace exit_reason = caf::exit_reason;
namespace scheduler = caf::scheduler;

namespace exit {

constexpr auto done  = exit_reason::user_defined;
constexpr auto stop  = exit_reason::user_defined + 1;
constexpr auto error = exit_reason::user_defined + 2;
constexpr auto kill  = exit_reason::user_defined + 3;

} // namespace exit

inline char const* render_exit_reason(uint32_t reason) {
  switch (reason) {
    default:
      return "unknown";
    case exit::done:
      return "done";
    case exit::stop:
      return "stop";
    case exit::error:
      return "error";
    case exit::kill:
      return "kill";
    case exit_reason::normal:
      return "normal";
    case exit_reason::unhandled_exception:
      return "unhandled exception";
    case exit_reason::unhandled_sync_failure:
      return "unhandled sync failure";
    case exit_reason::user_shutdown:
      return "user shutdown";
    case exit_reason::remote_link_unreachable:
      return "remote link unreachable";
  }
}

/// Helper to inject a catch-all match expression that logs unexpected
/// messages.
/// @param self The actor context.
auto log_others = [](auto self) {
  return others >> [=] {
    VAST_ERROR_AT(self, "got unexpected message from",
                  '#' << self->current_sender()->id() << ':',
                  to_string(self->current_message()));
  };
};

auto quit_on_others = [](auto self) {
  return others >> [=] {
    VAST_ERROR_AT(self, "got unexpected message from",
                  '#' << self->current_sender()->id() << ':',
                  to_string(self->current_message()));
    self->quit(exit::error);
  };
};

} // namespace vast

#endif
