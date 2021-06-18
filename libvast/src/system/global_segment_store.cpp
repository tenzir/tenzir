//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/global_segment_store.hpp"

#include "vast/detail/framed.hpp"
#include "vast/plugin.hpp"
#include "vast/si_literals.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/posix_filesystem.hpp"
#include "vast/table_slice.hpp"

using namespace vast::binary_byte_literals;

namespace vast::system {

// A custom stream manager that is able to notify when all data has been
// processed. It relies on `Self->state` being a struct containing a function
// `notify_flush_listeners()` and a vector `flush_listeners`, which means that
// it is currently only usable in combination with the `index` or the
// `active_partition` actor.
template <class Self, class Driver>
class injecting_stream_manager : public caf::detail::stream_stage_impl<Driver> {
public:
  using super = caf::detail::stream_stage_impl<Driver>;

  template <class... Ts>
  injecting_stream_manager(Self* self, Ts&&... xs)
    : caf::stream_manager(self),
      super(self, std::forward<Ts>(xs)...),
      self_(self) {
    // nop
  }

  void input_closed(caf::error reason) override {
    super::input_closed(std::move(reason));
    super::super::out().push(detail::framed<table_slice>::make_eof());
  }

private:
  Self* self_;

  auto self() {
    return self_;
  }

  auto& state() {
    return self()->state;
  }
};

/// Create a `notifying_stream_stage` and attaches it to the given actor.
// This is essentially a copy of `caf::attach_continous_stream_stage()`, but
// since the construction of the `stream_stage_impl` is buried quite deep there
// it is necesssary to duplicate the code.
template <class Self, class Init, class Fun, class Finalize = caf::unit_t,
          class DownstreamManager = caf::default_downstream_manager_t<Fun>,
          class Trait = caf::stream_stage_trait_t<Fun>>
caf::stream_stage_ptr<typename Trait::input, DownstreamManager>
attach_injecting_stream_stage(
  Self* self, bool continuous, Init init, Fun fun, Finalize fin = {},
  [[maybe_unused]] caf::policy::arg<DownstreamManager> token = {}) {
  using input_type = typename Trait::input;
  using output_type = typename Trait::output;
  using state_type = typename Trait::state;
  static_assert(
    std::is_same<void(state_type&),
                 typename caf::detail::get_callable_trait<Init>::fun_sig>::value,
    "Expected signature `void (State&)` for init function");
  static_assert(
    std::is_same<void(state_type&, caf::downstream<output_type>&, input_type),
                 typename caf::detail::get_callable_trait<Fun>::fun_sig>::value,
    "Expected signature `void (State&, downstream<Out>&, In)` "
    "for consume function");
  using caf::detail::stream_stage_driver_impl;
  using driver = stream_stage_driver_impl<typename Trait::input,
                                          DownstreamManager, Fun, Finalize>;
  using impl = injecting_stream_manager<Self, driver>;
  auto ptr = caf::make_counted<impl>(self, std::move(init), std::move(fun),
                                     std::move(fin));
  if (continuous)
    ptr->continuous(true);
  return ptr;
}

// This store plugin wraps the global "archive" so we can
// use a unified API in the transition period.

// plugin API
caf::error global_store_plugin::initialize(data) {
  namespace sd = defaults::system;
  // TODO: Read settings from config.
  capacity_ = sd::segments;
  max_segment_size_ = 1_MiB * sd::max_segment_size;
  return {};
}

[[nodiscard]] const char* global_store_plugin::name() const {
  return "global_segment_store";
}

struct archive_adapter_state {
  bool shutdown_on_next_stream_disconnect;
  caf::stream_slot stream_slot;
  archive_actor archive;
  using archive_adapter_stream_stage_ptr
    = caf::stream_stage_ptr<detail::framed<table_slice>,
                            caf::broadcast_downstream_manager<table_slice>>;
  archive_adapter_stream_stage_ptr stage;
};

shutdownable_store_builder_actor::behavior_type archive_adapter(
  shutdownable_store_builder_actor::stateful_pointer<archive_adapter_state> self,
  archive_actor archive) {
  self->state.archive = archive;
  self->set_exit_handler([self](const caf::exit_msg&) {
    self->state.stage->out().close(self->state.stream_slot);
    self->quit();
  });
  self->state.stage = attach_injecting_stream_stage(
    self, true, [](caf::unit_t&) {},
    [self](caf::unit_t&, caf::downstream<table_slice>& out,
           detail::framed<table_slice>& slice) {
      if (slice.header == detail::stream_control_header::eof) {
        if (self->state.shutdown_on_next_stream_disconnect)
          self->send_exit(self, caf::exit_reason::normal);
        return;
      }
      return out.push(slice.body);
    },
    [self](caf::unit_t&, const caf::error&) {
      self->state.stage->shutdown();
    });
  self->state.stream_slot = self->state.stage->add_outbound_path(archive);
  return {[self](atom::shutdown) {
            if (self->state.stage->inbound_paths().empty()) {
              self->send_exit(self, caf::exit_reason::user_shutdown);
            }
            self->state.shutdown_on_next_stream_disconnect = true;
          },
          [self](query q, ids i) {
            return self->delegate(self->state.archive, q, i);
          },
          [self](atom::erase, ids i) {
            return self->delegate(self->state.archive, atom::erase_v, i);
          },
          [self](caf::stream<detail::framed<table_slice>> in) {
            self->state.stage->add_inbound_path(in);
          }};
}

[[nodiscard]] auto
global_store_plugin::make_store_builder(filesystem_actor fs,
                                        const vast::uuid&) const
  -> caf::expected<builder_and_header> {
  if (!archive_) {
    caf::scoped_actor self{fs.home_system()};
    caf::expected<std::filesystem::path> dir = std::filesystem::path{};
    self->request(fs, caf::infinite, atom::root_v)
      .receive(
        [&](std::filesystem::path root) {
          dir = root;
        },
        [&](caf::error& err) {
          dir = std::move(err);
        });
    if (!dir)
      return dir.error();
    archive_ = self->spawn(system::archive, *dir / "archive", capacity_,
                           max_segment_size_);
    adapter_ = self->spawn(archive_adapter, archive_);
  }
  return builder_and_header{adapter_, vast::chunk::empty()};
}

[[nodiscard]] caf::expected<system::store_actor>
global_store_plugin::make_store(filesystem_actor, span<const std::byte>) const {
  return archive_;
}

[[nodiscard]] archive_actor global_store_plugin::archive() const {
  return archive_;
}

VAST_REGISTER_PLUGIN(vast::system::global_store_plugin)

} // namespace vast::system
