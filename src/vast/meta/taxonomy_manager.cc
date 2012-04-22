#include "vast/meta/taxonomy_manager.h"

#include "vast/meta/taxonomy.h"
#include "vast/meta/event.h"
#include "vast/meta/type.h"

namespace vast {
namespace meta {

void taxonomy_manager::init(fs::path const& tax_file)
{
    tax_.reset(new taxonomy);
    tax_->load(tax_file);
}

taxonomy& taxonomy_manager::get()
{
    assert(tax_);
    return *tax_;
}

} // namespace meta
} // namespace vast
