#include "vast/store/ingestor.h"

namespace vast {
namespace store {

ingestor::ingestor(ze::io& io)
  : ze::component(io)
  , source(*this)
{
}

} // namespace store
} // namespace vast
