#include "vast/individual.hpp"

namespace vast {

individual::individual(uuid id) : id_(id) {
}

bool operator<(individual const& x, individual const& y) {
  return x.id() < y.id();
}

bool operator==(individual const& x, individual const& y) {
  return x.id() == y.id();
}

uuid const& individual::id() const {
  return id_;
}

void individual::id(uuid id) {
  id_ = std::move(id);
}

} // namespace vast
