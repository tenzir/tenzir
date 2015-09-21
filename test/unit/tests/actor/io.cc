#include "vast/caf.h"
#include "vast/actor/atoms.h"
#include "vast/io/actor_stream.h"
#include "vast/io/buffered_stream.h"

#define SUITE actors
#include "test.h"

using namespace vast;

TEST(actor_input_stream)
{
  auto producer = spawn([]() -> behavior {
    std::vector<std::vector<uint8_t>> blocks = {
      {4, 5, 6},
      {1, 2, 3}
    };
    return {
      [=](get_atom) mutable {
        if (blocks.empty())
          return make_message(done_atom::value);
        auto msg = make_message(std::move(blocks.back()));
        blocks.pop_back();
        return msg;
      }
    };
  });

  // At the end of the scope, the scoped_actor inside the input stream
  // terminates.
  {
    vast::io::actor_input_stream ais{producer, std::chrono::milliseconds(100)};
    uint8_t const* data;
    size_t size;
    MESSAGE("getting 1st chunk");
    REQUIRE(ais.next(reinterpret_cast<void const**>(&data), &size));
    for (auto i = 0u; i < size; ++i)
      CHECK(data[i] == i + 1);
    MESSAGE("getting 2nd chunk");
    REQUIRE(ais.next(reinterpret_cast<void const**>(&data), &size));
    for (auto i = 0u; i < size; ++i)
      CHECK(data[i] == i + 4);
    MESSAGE("rewinding 1 byte");
    ais.rewind(1);
    MESSAGE("getting 2nd chunk again");
    REQUIRE(ais.next(reinterpret_cast<void const**>(&data), &size));
    CHECK(size == 1);
    CHECK(*data == 6);
    CHECK(! ais.next(reinterpret_cast<void const**>(&data), &size));
  }

  MESSAGE("terminating producer");
  scoped_actor self;
  self->send_exit(producer, exit_reason::kill);
  self->await_all_other_actors_done();
}

TEST(actor_output_stream)
{
  auto block_size = 512u;
  auto consumer = spawn([=]() -> behavior {
    auto i = std::make_shared<int>(0);
    return {
      [=](std::vector<uint8_t>& data) {
        CHECK(data.size() == block_size);
        for (auto x : data)
          CHECK(x == (*i)++);
      },
      others() >> [=] {
        CHECK(*i == block_size);
      }
    };
  });

  vast::io::actor_output_stream os{consumer, block_size};
  uint8_t* data;
  size_t size;
  REQUIRE(os.next(reinterpret_cast<void**>(&data), &size));
  for (auto i = 0u; i < size; ++i)
    data[i] = i;
  REQUIRE(os.flush());

  MESSAGE("terminating consumer");
  scoped_actor self;
  self->send(consumer, "final check");
  self->send_exit(consumer, exit::done);
  self->await_all_other_actors_done();
}
