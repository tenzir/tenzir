/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "fixtures/dummy_index.hpp"

#include <caf/exit_reason.hpp>
#include <caf/send.hpp>

using void_fun = std::function<void()>;

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(void_fun)

using namespace caf;
using namespace vast;

using vast::system::index_state;

namespace fixtures {

namespace {

behavior dummy_indexer(stateful_actor<dummy_index::dummy_indexer_state>*) {
  return {[](ok_atom) {
    // nop
  }};
}

actor spawn_dummy_indexer(local_actor* self, path, type, size_t) {
  return self->spawn(dummy_indexer);
}

behavior dummy_index_actor(stateful_actor<index_state>* self,
                           path dir) {
  self->state.init(std::move(dir), std::numeric_limits<size_t>::max(), 10, 5);
  self->state.factory = spawn_dummy_indexer;
  return {[](std::function<void()> f) { f(); }};
}

} // namespace <anonymous>

dummy_index::dummy_index() {
  idx_handle = sys.spawn(dummy_index_actor, directory);
  run();
  idx_state = &deref<stateful_actor<index_state>>(idx_handle).state;
}

dummy_index::~dummy_index() {
  anon_send_exit(idx_handle, exit_reason::kill);
  run();
}

void dummy_index::run_in_index(std::function<void()> f) {
  mailbox_element_vals<void_fun> tmp{nullptr,
                                     make_message_id(),
                                     {},
                                     std::move(f)};
  auto ctx = sys.dummy_execution_unit();
  deref<stateful_actor<index_state>>(idx_handle).activate(ctx, tmp);
}

} // namespace fixtures
