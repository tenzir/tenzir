#include <caf/detail/stream_source_driver_impl.hpp>

#include <vast/system/indexer_downstream_manager.hpp>
#include <vast/system/indexer_stage_driver.hpp>

#define SUITE idm
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

caf::behavior dummy_actor(caf::event_based_actor* self) {
  return {
    [=](caf::stream<vast::table_slice_column> in) {
      std::cout << "got new stream\n";
      // Create a stream manager for implementing a stream sink. Once more, we
      // have to provide three functions: Initializer, Consumer, Finalizer.
      return attach_stream_sink(
        self,
        // Our input source.
        in,
        // Initializer. Here, we store all values we receive. Note that streams
        // are potentially unbound, so this is usually a bad idea outside small
        // examples like this one.
        [](caf::unit_t&) {
          std::cout << "init stream\n";
          // nop
        },
        // Consumer. Takes individual input elements as `val` and stores them
        // in our history.
        [](caf::unit_t&, vast::table_slice_column) {},
        // Finalizer. Allows us to run cleanup code once the stream terminates.
        [=](caf::unit_t& xs, const caf::error& err) {
          if (err) {
            aout(self) << "int_sink aborted with error: " << err << std::endl;
          } else {
            aout(self) << "int_sink finalized after receiving: " << xs
                       << std::endl;
          }
        });
    },
  };
}

// We use functors here instead of the recommended lambda interface so we're
// able to write out the actual type of the stream source below.
struct source_state {};

struct source_pull {
  // The `indexer_downstream_manager` has hard-coded sink type
  // `vast::table_slice_column`.
  void operator()(source_state&, caf::downstream<vast::table_slice_column>&,
                  size_t) {
    return;
  }
};

struct source_done {
  bool operator()(const source_state&) const {
    return false;
  }
};

struct source_finalize {
  void operator()(const source_state&, const caf::error&) {
    return;
  }
};

FIXTURE_SCOPE(foo, fixtures::deterministic_actor_system)

TEST(path overflow) {
  auto actor = self->spawn(dummy_actor);

  // caf::stream_stage<int, vast::system::indexer_downstream_manager>
  // stg(caf::actor_cast<caf::scheduled_actor*>(actor));

  // Note: The `state` type is deduced from the first argument of the `Pull`
  // functor. using source_driver =
  // caf::detail::stream_source_driver_impl<vast::system::indexer_downstream_manager,
  // source_pull, source_done, source_finalize>;
  // caf::detail::stream_source_impl<source_driver> src(
  // 	caf::actor_cast<caf::scheduled_actor*>(actor),
  // 	[](source_state&) { /* init */},
  // 	source_pull{},
  // 	source_done{},
  // 	source_finalize{});

  using source_driver = caf::detail::stream_source_driver_impl<
    caf::broadcast_downstream_manager<vast::table_slice_column>, source_pull,
    source_done, source_finalize>;
  caf::detail::stream_source_impl<source_driver> src(
    caf::actor_cast<caf::scheduled_actor*>(actor),
    [](source_state&) { /* init */ }, source_pull{}, source_done{},
    source_finalize{});

  run();

  std::vector<caf::actor> sinks;
  for (int i = 0; i < 2; ++i) {
    sinks.push_back(self->spawn(dummy_actor));
    run();
  }

  for (auto& snk : sinks) {
    auto slot = src.add_outbound_path();
    CHECK_NOT_EQUAL(slot, caf::invalid_stream_slot);
    run();
  }

  std::cout << "calling stop...\n" << std::flush;

  src.stop();

  std::cout << "finished; destructoring...\n";
}

FIXTURE_SCOPE_END()