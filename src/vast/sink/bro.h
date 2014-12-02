#ifndef VAST_SINK_BRO_H
#define VAST_SINK_BRO_H

#include <memory>
#include <unordered_map>

#include "vast/file_system.h"
#include "vast/sink/base.h"
#include "vast/sink/stream.h"

namespace vast {
namespace sink {

/// A sink generating Bro logs.
class bro : public base<bro>
{
public:
  static constexpr char sep = '\x09';
  static constexpr auto set_separator = ",";
  static constexpr auto empty_field = "(empty)";
  static constexpr auto unset_field = "-";
  static constexpr auto format = "%Y-%m-%d-%H-%M-%S";

  static std::string make_header(type const& t);
  static std::string make_footer();

  /// Spawns a Bro sink.
  /// @param p The output path.
  bro(path p);

  bool process(event const& e);
  std::string name() const;

private:
  path dir_;
  std::unordered_map<std::string, std::unique_ptr<stream>> streams_;
};

} // namespace sink
} // namespace vast

#endif
