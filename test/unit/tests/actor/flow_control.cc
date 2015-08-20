#include "vast/actor/flow_controller.h"

#define SUITE actors
#include "test.h"

using namespace vast;

namespace {

behavior worker(event_based_actor* self, actor controller, actor supervisor) {
  auto overloads = std::make_shared<int>(0);
  auto underloads = std::make_shared<int>(0);
  return {
    [=](overload_atom, actor const&) {
      self->send(supervisor, ++*overloads);
    },
    [=](underload_atom, actor const&) {
      self->send(supervisor, ++*underloads);
    },
    [=](enable_atom, overload_atom) {
      self->send(message_priority::high, controller, overload_atom::value);
    },
    [=](enable_atom, underload_atom) {
      self->send(message_priority::high, controller, underload_atom::value);
    }
  };
}

} // namespace <anonymous>

TEST(single-path flow-control) {
  scoped_actor self;
  MESSAGE("constructing data flow path A -> B -> C -> D");
  auto fc = self->spawn(flow_controller::actor);
  auto a = self->spawn<priority_aware>(worker, fc, self);
  auto b = self->spawn<priority_aware>(worker, fc, self);
  auto c = self->spawn<priority_aware>(worker, fc, self);
  auto d = self->spawn<priority_aware>(worker, fc, self);
  MESSAGE("registering edges with flow controller");
  self->send(fc, add_atom::value, a, b);
  self->send(fc, add_atom::value, b, c);
  self->send(fc, add_atom::value, c, d);
  MESSAGE("overloading C");
  self->send(c, enable_atom::value, overload_atom::value);
  self->send(c, enable_atom::value, overload_atom::value);
  MESSAGE("checking overload count on A");
  auto i = 1;
  self->receive_for(i, 3)(
    [&](int overloads) {
      CHECK(self->current_sender() == a);
      CHECK(overloads == i);
    }
  );
  MESSAGE("underloading D");
  self->send(d, enable_atom::value, underload_atom::value);
  MESSAGE("checking underload count on A");
  self->receive(
    [&](int underloads) {
      CHECK(self->current_sender() == a);
      CHECK(underloads == 1);
    }
  );
  self->send_exit(a, exit::done);
  self->send_exit(b, exit::done);
  self->send_exit(c, exit::done);
  self->send_exit(d, exit::done);
  self->send_exit(fc, exit::done);
  self->await_all_other_actors_done();
}

TEST(multi-path flow-control with deflectors) {
  scoped_actor self;
  MESSAGE("constructing data flow path A -> B -> C -> D");
  MESSAGE("constructing data flow path E -> F -> C");
  auto fc = self->spawn(flow_controller::actor);
  auto a = self->spawn<priority_aware>(worker, fc, self);
  auto b = self->spawn<priority_aware>(worker, fc, self);
  auto c = self->spawn<priority_aware>(worker, fc, self);
  auto d = self->spawn<priority_aware>(worker, fc, self);
  auto e = self->spawn<priority_aware>(worker, fc, self);
  auto f = self->spawn<priority_aware>(worker, fc, self);
  MESSAGE("registering edges with flow controller");
  self->send(fc, add_atom::value, a, b);
  self->send(fc, add_atom::value, b, c);
  self->send(fc, add_atom::value, c, d);
  self->send(fc, add_atom::value, e, f);
  self->send(fc, add_atom::value, f, c);
  MESSAGE("overloading D");
  self->send(d, enable_atom::value, overload_atom::value);
  MESSAGE("checking overload count on A & E");
  auto i = 0;
  self->receive_for(i, 2)(
    [&](int overloads) {
      CHECK((self->current_sender() == a || self->current_sender() == e));
      CHECK(overloads == 1);
    }
  );
  MESSAGE("adding deflector F");
  self->send(fc, add_atom::value, deflector_atom::value, f);
  MESSAGE("overloading D");
  self->send(d, enable_atom::value, overload_atom::value);
  MESSAGE("checking overload count on A & F");
  self->receive(
    [&](int overloads) {
      CHECK(self->current_sender() == f);
      CHECK(overloads == 1);
    }
  );
  self->receive(
    [&](int overloads) {
      CHECK(self->current_sender() == a);
      CHECK(overloads == 2);
    }
  );
  self->send_exit(a, exit::done);
  self->send_exit(b, exit::done);
  self->send_exit(c, exit::done);
  self->send_exit(d, exit::done);
  self->send_exit(e, exit::done);
  self->send_exit(f, exit::done);
  self->send_exit(fc, exit::done);
  self->await_all_other_actors_done();
}
