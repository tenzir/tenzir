#ifndef VAST_ACTOR_SINK_BRO_H
#define VAST_ACTOR_SINK_BRO_H

#include <iosfwd>
#include <memory>
#include <unordered_map>

#include "vast/filesystem.h"
#include "vast/actor/sink/base.h"

namespace vast {
namespace sink {

/// A sink generating Bro logs.
struct bro_state : state {
  static constexpr char sep = '\x09';
  static constexpr auto set_separator = ",";
  static constexpr auto empty_field = "(empty)";
  static constexpr auto unset_field = "-";
  static constexpr auto format = "%Y-%m-%d-%H-%M-%S";

  static std::string make_header(type const& t);
  static std::string make_footer();

  bro_state(local_actor* self);
  ~bro_state();

  bool process(event const& e) override;

  using map_type =
    std::unordered_map<std::string, std::unique_ptr<std::ostream>>;

  path dir;
  map_type streams;
};

/// Spawns a Bro sink.
/// @param self The actor handle.
/// @param p The output path.
behavior bro(stateful_actor<bro_state>* self, path p);

} // namespace sink
} // namespace vast

#endif
