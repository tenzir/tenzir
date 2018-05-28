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

#include "vast/system/column_index.hpp"

#include "vast/expression_visitors.hpp"
#include "vast/load.hpp"
#include "vast/logger.hpp"
#include "vast/save.hpp"

namespace vast::system {

// -- free functions -----------------------------------------------------------

namespace {

/// Tries to initialize `res` and returns it on success, otherwise returns the
/// initialization error.
caf::expected<column_index_ptr> init_res(column_index_ptr res) {
  auto err = res->init();
  if (err)
    return err;
  return res;
}

} // namespace <anonymous>

caf::expected<column_index_ptr> make_time_index(path filename) {
  struct impl : column_index {
    impl(path&& fname) : column_index(timespan_type{}, std::move(fname)) {
      // nop
    }

    void add(const event& x) override {
      idx_->push_back(x.timestamp());
    }
  };
  return init_res(std::make_unique<impl>(std::move(filename)));
}

caf::expected<column_index_ptr> make_type_index(path filename) {
  struct impl : column_index {
    impl(path&& fname) : column_index(string_type{}, std::move(fname)) {
      // nop
    }

    void add(const event& x) override {
      idx_->push_back(x.type().name());
    }
  };
  return init_res(std::make_unique<impl>(std::move(filename)));
}

caf::expected<column_index_ptr> make_flat_data_index(path filename,
                                                     type event_type) {
  struct impl : column_index {
    impl(type&& etype, path&& fname)
      : column_index(std::move(etype), std::move(fname)) {
        // nop
    }

    void add(const event& x) override {
      if (x.type() == column_type_)
        idx_->push_back(x.data());
    }
  };
  return init_res(std::make_unique<impl>(std::move(event_type),
                                         std::move(filename)));
}

caf::expected<column_index_ptr> make_field_data_index(path filename,
                                                      type field_type,
                                                      offset off) {
  struct impl : column_index {
    impl(type&& ftype, path&& fname, offset o)
      : column_index(std::move(ftype), std::move(fname)),
        o_(o) {
      // nop
    }

    void add(const event& x) override {
      auto v = get_if<vector>(x.data());
      if (!v)
        return;
      if (auto y = get(*v, o_))
        idx_->push_back(*y);
      // If there is no data at a given offset, it means that an intermediate
      // record is nil but we're trying to access a deeper field.
      static const auto nil_data = data{nil};
      idx_->push_back(nil_data);
    }

    offset o_;
  };
  auto res = std::make_unique<impl>(std::move(field_type), std::move(filename),
                                    off);
  return init_res(std::move(res));
}

/*
caf::expected<bitmap> lookup(column_index::owning_pointer_vec columns,
                             const type& event_type,
                             const predicate& x) {
  // For now, we require that the predicate is part of a normalized expression,
  // i.e., LHS an extractor type and RHS of type data.
  auto rhs = get_if<data>(pred.rhs);
  if (!rhs)
    return ec::invalid_query;
  // Specialize the predicate for the type.
  auto resolved = type_resolver{event_type}(pred);
  if (!resolved)
    return std::move(resolved.error());
  auto indexers = visit(loader{self}, *resolved);
  // Forward predicate to all available indexers.
  if (indexers.empty()) {
    VAST_DEBUG(self, "did not find matching indexers for", pred);
    rp.deliver(bitmap{});
    return;
  }
  VAST_DEBUG(self, "asks", indexers.size(), "indexers");
  // Manual map-reduce over the indexers.
  auto n = std::make_shared<size_t>(indexers.size());
  auto reducer = self->system().spawn([=]() mutable -> behavior {
    auto result = std::make_shared<bitmap>();
    return {
      [=](const bitmap& bm) mutable {
        if (!bm.empty())
          *result |= bm;
        if (--*n == 0)
          rp.deliver(std::move(*result));
      },
      [=](error& e) mutable {
        rp.deliver(std::move(e));
      }
    };
  });
  auto msg = self->current_mailbox_element()->move_content_to_message();
  for (auto& x : indexers)
    send_as(reducer, x, msg);
}
*/

// -- constructors, destructors, and assignment operators ----------------------

column_index::~column_index() {
  // nop
}

// -- persistency --------------------------------------------------------------

caf::error column_index::init() {
  if (exists(filename_)) {
    // Materialize the index when encountering persistent state.
    detail::value_index_inspect_helper tmp{index_type_, idx_};
    auto result = load(filename_, last_flush_, tmp);
    if (!result) {
      return result.error();
    } else {
      VAST_DEBUG("loaded value index with offset", idx_->offset());
    }
  } else {
    // Otherwise construct a new one.
    idx_ = value_index::make(index_type_);
    if (!idx_)
      return make_error(ec::unspecified, "failed to construct index");
  }
  return caf::none;
}

caf::error column_index::flush_index_to_disk() {
  // Flush index to disk.
  auto offset = idx_->offset();
  if (offset == last_flush_) {
    // Nothing to write.
    return caf::none;
  }
  // Create parent directory if it doesn't exist.
  auto dir = filename_.parent();
  if (!exists(dir)) {
    auto result = mkdir(dir);
    if (!result)
      return result.error();
  }
  VAST_DEBUG("flush index (" << (offset - last_flush_) << '/' << offset,
             "new/total bits)");
  last_flush_ = offset;
  detail::value_index_inspect_helper tmp{index_type_, idx_};
  auto result = save(filename_, last_flush_, tmp);
  if (!result)
    return result.error();
  return caf::none;
}

// -- properties -------------------------------------------------------------

caf::expected<bitmap> column_index::lookup(const predicate& pred) {
  VAST_ASSERT(idx_ != nullptr);
  return idx_->lookup(pred.op, get<data>(pred.rhs));
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::column_index(type index_type, path filename)
  : index_type_(std::move(index_type)),
    filename_(std::move(filename)) {
}

} // namespace vast::system
