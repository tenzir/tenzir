/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/system/partition.hpp"

#include "vast/aliases.hpp"
#include "vast/concept/hashable/xxhash.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/expression_visitors.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/save.hpp"
#include "vast/system/index.hpp"
#include "vast/system/indexer_stage_driver.hpp"
#include "vast/system/spawn_indexer.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/time.hpp"
#include "vast/type.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/local_actor.hpp>
#include <caf/make_counted.hpp>
#include <caf/stateful_actor.hpp>

using namespace std::chrono;
using namespace caf;

namespace vast::system {

partition::partition(index_state* state, uuid id, size_t max_capacity)
  : state_(state),
    id_(std::move(id)),
    capacity_(max_capacity) {
  // If the directory already exists, we must have some state from the past and
  // are pre-loading all INDEXER types we are aware of.
  VAST_ASSERT(state != nullptr);
}

partition::~partition() noexcept {
  flush_to_disk();
}

// -- persistence --------------------------------------------------------------

indexer_downstream_manager& partition::out() const {
  return state_->stage->out();
}

caf::error partition::init() {
  VAST_TRACE("");
  auto file_path = meta_file();
  if (!exists(file_path))
    return ec::no_such_file;
  auto partition_type = record_type{};
  if (auto err = load(nullptr, file_path, meta_data_, partition_type))
    return err;
  VAST_DEBUG(state_->self, "loaded partition", id_, "from disk with",
             meta_data_.layouts.size(), "layouts and",
             partition_type.fields.size(), "columns");
  for (auto& layout : meta_data_.layouts)
    for (auto& field : layout.fields) {
      qualified_record_field fqf{layout.name(), field};
      indexers_.emplace(std::move(fqf), wrapped_indexer{});
    }
  return caf::none;
}

caf::error partition::flush_to_disk() {
  if (meta_data_.dirty) {
    // Write all layouts to disk.
    if (auto err = save(nullptr, meta_file(), meta_data_, combined_type()))
      return err;
    meta_data_.dirty = false;
  }
  return caf::none;
}

caf::actor& partition::indexer_at(size_t position) {
  VAST_ASSERT(position < indexers_.size());
  auto& [fqf, ip] = as_vector(indexers_)[position];
  if (!ip.indexer) {
    ip.indexer
      = state().make_indexer(column_file(fqf), fqf.type, id(), fqf.fqn());
    VAST_ASSERT(ip.indexer != nullptr);
  }
  return ip.indexer;
}

// -- properties ---------------------------------------------------------------

void partition::add(table_slice_ptr slice) {
  VAST_ASSERT(slice != nullptr);
  VAST_TRACE(VAST_ARG(slice));
  meta_data_.dirty = true;
  auto first = slice->offset();
  auto last = slice->offset() + slice->rows();
  auto& layout = slice->layout();
  bool is_new = meta_data_.layouts.emplace(layout).second;
  auto it = meta_data_.type_ids.emplace(layout.name(), vast::ids{}).first;
  auto& ids = it->second;
  VAST_ASSERT(first >= ids.size());
  ids.append_bits(false, first - ids.size());
  ids.append_bits(true, last - first);
  if (is_new) {
    // Insert new type.
    for (auto& field : layout.fields) {
      auto fqf = qualified_record_field{layout.name(), field};
      auto j = indexers_.find(fqf);
      if (j == indexers_.end()) {
        // Spawn a new indexer.
        auto k = indexers_.emplace(fqf, wrapped_indexer{});
        auto& ip = k.first->second;
        ip.indexer
          = state().make_indexer(column_file(fqf), fqf.type, id(), fqf.fqn());
        state_->active_partition_indexers++;
        ip.slot
          = out().parent()->add_unchecked_outbound_path<table_slice_column>(
            ip.indexer);
        ip.outbound = out().paths().rbegin()->second.get();
      }
    }
  }
  // Make sure the capacity does not wrap around.
  if (slice->rows() > capacity_)
    capacity_ = 0;
  else
    capacity_ -= slice->rows();
  inbound_.push_back(std::move(slice));
}

record_type partition::combined_type() const {
  record_type result;
  for (auto& kvp : indexers_) {
    result.fields.push_back(as_record_field(kvp.first));
  }
  return result;
}

caf::actor partition::fetch_indexer(const data_extractor& dx,
                                    [[maybe_unused]] relational_operator op,
                                    [[maybe_unused]] const data& x) {
  VAST_TRACE(VAST_ARG(dx), VAST_ARG(op), VAST_ARG(x));
  // Sanity check.
  if (dx.offset.empty())
    return nullptr;
  auto& r = caf::get<record_type>(dx.type);
  auto k = r.resolve(dx.offset);
  VAST_ASSERT(k);
  auto index = r.flat_index_at(dx.offset);
  if (!index) {
    VAST_DEBUG(state_->self, "got invalid offset for record type", dx.type);
    return nullptr;
  }
  return indexer_at(*index);
}

caf::actor partition::fetch_indexer(const attribute_extractor& ex,
                                    relational_operator op, const data& x) {
  VAST_TRACE(VAST_ARG(ex), VAST_ARG(op), VAST_ARG(x));
  if (ex.attr == atom::type_v) {
    // We know the answer immediately: all IDs that are part of the table.
    // However, we still have to "lift" this result into an actor for the
    // EVALUATOR.
    ids row_ids;
    for (auto& [name, ids] : meta_data_.type_ids)
      if (evaluate(name, op, x))
        row_ids |= ids;
    // TODO: Spawning a one-shot actor is quite expensive. Maybe the
    //       partition could instead maintain this actor lazily.
    return state_->self->spawn([row_ids]() -> caf::behavior {
      return [=](const curried_predicate&) { return row_ids; };
    });
  }
  VAST_WARNING(state_->self, "got unsupported attribute:", ex.attr);
  return nullptr;
}

evaluation_triples partition::eval(const expression& expr) {
  evaluation_triples result;
  // Pretend the partition is a table, and return fitted predicates for the
  // partitions layout.
  auto resolved = resolve(expr, combined_type());
  for (auto& kvp : resolved) {
    // For each fitted predicate, look up the corresponding INDEXER according
    // to the specified type of extractor.
    auto& pred = kvp.second;
    auto get_indexer_handle = [&](const auto& ext, const data& x) {
      return fetch_indexer(ext, pred.op, x);
    };
    auto v = detail::overload(
      [&](const attribute_extractor& ex, const data& x) {
        return get_indexer_handle(ex, x);
      },
      [&](const data_extractor& dx, const data& x) {
        return get_indexer_handle(dx, x);
      },
      [](const auto&, const auto&) {
        return caf::actor{}; // clang-format fix
      });
    // Package the predicate, its position in the query and the required
    // INDEXER as a "job description".
    if (auto hdl = caf::visit(v, pred.lhs, pred.rhs))
      result.emplace_back(kvp.first, curried(pred), std::move(hdl));
  }
  // Return the list of jobs, to be used by the EVALUATOR.
  return result;
}

path partition::base_dir() const {
  return state_->dir / to_string(id_);
}

path partition::meta_file() const {
  return base_dir() / "meta";
}

path partition::column_file(const qualified_record_field& field) const {
  return base_dir() / (field.fqn() + "-" + to_digest(field.type));
}

} // namespace vast::system

namespace std {

namespace {

using pptr = vast::system::partition_ptr;

} // namespace <anonymous>

size_t hash<pptr>::operator()(const pptr& ptr) const {
  hash<vast::uuid> f;
  return ptr != nullptr ? f(ptr->id()) : 0u;
}

} // namespace std
