//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "fixtures/dummy_index.hpp"

#include "vast/system/instrumentation.hpp"

#include <caf/exit_reason.hpp>
#include <caf/send.hpp>

using void_fun = std::function<void()>;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(void_fun)

using namespace vast;

using vast::system::index_state;

namespace fixtures {

namespace {

caf::behavior
dummy_indexer(caf::stateful_actor<dummy_index::dummy_indexer_state>*) {
  return {[](atom::ok) {
    // nop
  }};
}

caf::actor
spawn_dummy_indexer(caf::local_actor* self, system::accountant_actor, path,
                    type, caf::settings, caf::actor, uuid, std::string) {
  return self->spawn(dummy_indexer);
}

caf::behavior
dummy_index_actor(caf::stateful_actor<index_state>* self, path dir) {
  self->state.init(std::move(dir), std::numeric_limits<size_t>::max(), 10, 5,
                   true);
  self->state.factory = spawn_dummy_indexer;
  return {[](std::function<void()> f) { f(); }};
}

} // namespace

dummy_index::dummy_index() {
  idx_handle = sys.spawn(dummy_index_actor, directory);
  run();
  idx_state = &deref<caf::stateful_actor<index_state>>(idx_handle).state;
}

dummy_index::~dummy_index() {
  anon_send_exit(idx_handle, caf::exit_reason::kill);
  run();
}

void dummy_index::run_in_index(std::function<void()> f) {
  caf::mailbox_element_vals<void_fun> tmp{
    nullptr, caf::make_message_id(), {}, std::move(f)};
  auto ctx = sys.dummy_execution_unit();
  deref<caf::stateful_actor<index_state>>(idx_handle).activate(ctx, tmp);
}

} // namespace fixtures
