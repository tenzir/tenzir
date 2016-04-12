#ifndef VAST_CAF_HPP
#define VAST_CAF_HPP

#include <ostream>

#include <caf/all.hpp>

#include "vast/logger.hpp"
#include "vast/util/assert.hpp"

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

using caf::actor;
using caf::actor_addr;
using caf::actor_cast;
using caf::after;
using caf::anon_send;
using caf::atom;
using caf::behavior;
using caf::detached;
using caf::down_msg;
using caf::event_based_actor;
using caf::exit_msg;
using caf::keep_behavior;
using caf::linked;
using caf::local_actor;
using caf::make_message;
using caf::message;
using caf::message_builder;
using caf::message_handler;
using caf::message_priority;
using caf::monitored;
using caf::priority_aware;
using caf::reacts_to;
using caf::replies_to;
using caf::response_promise;
using caf::scoped_actor;
using caf::to_string;
using caf::typed_actor;
using caf::typed_response_promise;
using caf::unit;
using caf::detail::make_scope_guard;
using caf::stateful_actor;

/// A catch-all match expression that logs unexpected messages.
/// @param self The actor context.
/// @relates quit_on_others
//auto log_others = [](auto self) {
//  return others >> [=] {
//    VAST_ERROR_AT(self, "got unexpected message from",
//                  self->current_sender() << ':',
//                  to_string(self->current_message()));
//  };
//};

/// A catch-all match expression that logs unexpected messages and terminates.
/// @param self The actor context.
/// @relates log_others
//auto quit_on_others = [](auto self) {
//  return others >> [=] {
//    VAST_ERROR_AT(self, "got unexpected message from",
//                  self->current_sender() << ':',
//                  to_string(self->current_message()));
//    self->quit(exit::error);
//  };
//};

/// Delays processing of an EXIT message. If an actor is spawned with the
/// `priority_aware` flag, it will process CAF high-priority message before
/// those with normal priority. Since CAF sends exit messages with high
/// priority, a priority-aware actor will terminate even if it still has
/// received other messages earlier. This conflicts when actor termination
/// semantics should be that all messages are processed up to the point of
/// receiving an EXIT message. In such scenarios, one can downgrade the
/// priority of the EXIT signal by re-inserting it with normal priority at the
/// end of the mailbox.
/// @param self The actor context.
//auto downgrade_exit_msg = [](auto self) {
//  return [=](exit_msg const& msg) {
//    if (self->current_mailbox_element()->mid.is_high_priority()) {
//      VAST_DEBUG_AT(self, "delays EXIT from", msg.source);
//      self->send(message_priority::normal, self, self->current_message());
//    } else {
//      self->quit(msg.reason);
//    }
//  };
//};

} // namespace vast

#endif
