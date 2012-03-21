#include "vast/meta/taxonomy_manager.h"

#include "vast/meta/taxonomy.h"

namespace vast {
namespace meta {

void taxonomy_manager::init(const fs::path& tax_file)
{
    tax_.reset(new taxonomy);
    tax_->load(tax_file);
}

taxonomy_ptr taxonomy_manager::get() const
{
    assert (tax_);
    return tax_;
}

} // namespace meta
} // namespace vast
