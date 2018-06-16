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
      VAST_TRACE(VAST_ARG(x));
      idx_->push_back(x.timestamp(), x.id());
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
      VAST_TRACE(VAST_ARG(x));
      idx_->push_back(x.type().name(), x.id());
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
      VAST_TRACE(VAST_ARG(x));
      if (x.type() == index_type_)
        idx_->push_back(x.data(), x.id());
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
      VAST_TRACE(VAST_ARG(x));
      VAST_ASSERT(x.id() != invalid_id);
      auto v = get_if<vector>(x.data());
      if (!v)
        return;
      if (auto y = get(*v, o_)) {
        idx_->push_back(*y, x.id());
        return;
      }
      // If there is no data at a given offset, it means that an intermediate
      // record is nil but we're trying to access a deeper field.
      static const auto nil_data = data{nil};
      idx_->push_back(nil_data, x.id());
    }

    offset o_;
  };
  auto res = std::make_unique<impl>(std::move(field_type), std::move(filename),
                                    off);
  return init_res(std::move(res));
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::~column_index() {
  // nop
}

// -- persistency --------------------------------------------------------------

caf::error column_index::init() {
  VAST_TRACE("");
  // Materialize the index when encountering persistent state.
  if (exists(filename_)) {
    detail::value_index_inspect_helper tmp{index_type_, idx_};
    auto result = load(filename_, last_flush_, tmp);
    if (!result) {
      VAST_ERROR("unable to load value index from disk", result.error());
      return std::move(result.error());
    } else {
      VAST_DEBUG("loaded value index with offset", idx_->offset());
    }
    return caf::none;
  }
  // Otherwise construct a new one.
  idx_ = value_index::make(index_type_);
  if (idx_ == nullptr) {
    VAST_ERROR("failed to construct index");
    return make_error(ec::unspecified, "failed to construct index");
  }
  VAST_DEBUG("constructed new value index");
  return caf::none;
}

caf::error column_index::flush_to_disk() {
  VAST_TRACE("");
  // Check whether there's something to write.
  auto offset = idx_->offset();
  if (offset == last_flush_)
    return caf::none;
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
  VAST_TRACE(VAST_ARG(pred));
  VAST_ASSERT(idx_ != nullptr);
  auto result = idx_->lookup(pred.op, get<data>(pred.rhs));
  VAST_DEBUG(VAST_ARG(result));
  return result;
}

// -- constructors, destructors, and assignment operators ----------------------

column_index::column_index(type index_type, path filename)
  : index_type_(std::move(index_type)),
    filename_(std::move(filename)) {
}

} // namespace vast::system
