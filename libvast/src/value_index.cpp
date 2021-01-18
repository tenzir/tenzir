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

#include "vast/value_index.hpp"

#include "vast/value_index_factory.hpp"

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

namespace vast {

value_index::value_index(vast::type t, caf::settings opts)
  : type_{std::move(t)}, opts_{std::move(opts)} {
  // nop
}

value_index::~value_index() {
  // nop
}

caf::expected<void> value_index::append(data_view x) {
  return append(x, offset());
}

caf::expected<void> value_index::append(data_view x, id pos) {
  auto off = offset();
  if (pos < off)
    // Can only append at the end
    return caf::make_error(ec::unspecified, pos, '<', off);
  if (caf::holds_alternative<caf::none_t>(x)) {
    none_.append_bits(false, pos - none_.size());
    none_.append_bit(true);
    return caf::no_error;
  }
  // TODO: let append_impl return caf::error
  if (!append_impl(x, pos))
    return caf::make_error(ec::unspecified, "append_impl");
  mask_.append_bits(false, pos - mask_.size());
  mask_.append_bit(true);
  return caf::no_error;
}

caf::expected<ids>
value_index::lookup(relational_operator op, data_view x) const {
  // When x is nil, we can answer the query right here.
  if (caf::holds_alternative<caf::none_t>(x)) {
    if (!(op == equal || op == not_equal))
      return caf::make_error(ec::unsupported_operator, op);
    auto is_equal = op == equal;
    auto result = is_equal ? none_ : ~none_;
    if (result.size() < mask_.size())
      result.append_bits(!is_equal, mask_.size() - result.size());
    return result;
  }
  // If x is not nil, we dispatch to the concrete implementation.
  auto result = lookup_impl(op, x);
  if (!result)
    return result;
  // The result can only have mass (i.e., 1-bits) where actual IDs exist.
  *result &= mask_;
  // Because the value index implementations never see nil values, they need
  // to be handled here. If we have a predicate with a non-nil RHS and `!=` as
  // operator, then we need to add the nils to the result, because the
  // expression `nil != RHS` is true when RHS is not nil.
  auto is_negation = op == not_equal;
  if (is_negation)
    *result |= none_;
  // Finally, the concrete result may be too short, e.g., when the last values
  // have been nils. In this case we need to fill it up. For any operator other
  // than !=, the result of comparing with nil is undefined.
  if (result->size() < offset())
    result->append_bits(is_negation, offset() - result->size());
  return std::move(*result);
}

value_index::size_type value_index::offset() const {
  return std::max(none_.size(), mask_.size());
}

const type& value_index::type() const {
  return type_;
}

const caf::settings& value_index::options() const {
  return opts_;
}

caf::error value_index::serialize(caf::serializer& sink) const {
  return sink(mask_, none_);
}

caf::error value_index::deserialize(caf::deserializer& source) {
  return source(mask_, none_);
}

const ewah_bitmap& value_index::mask() const {
  return mask_;
}

const ewah_bitmap& value_index::none() const {
  return none_;
}

caf::error inspect(caf::serializer& sink, const value_index& x) {
  return x.serialize(sink);
}

caf::error inspect(caf::deserializer& source, value_index& x) {
  return x.deserialize(source);
}

caf::error inspect(caf::serializer& sink, const value_index_ptr& x) {
  static auto nullptr_type = type{};
  if (x == nullptr)
    return sink(nullptr_type);
  return caf::error::eval([&] { return sink(x->type(), x->options()); },
                          [&] { return x->serialize(sink); });
}

caf::error inspect(caf::deserializer& source, value_index_ptr& x) {
  type t;
  if (auto err = source(t))
    return err;
  if (caf::holds_alternative<none_type>(t)) {
    x = nullptr;
    return caf::none;
  }
  caf::settings opts;
  if (auto err = source(opts))
    return err;
  x = factory<value_index>::make(std::move(t), std::move(opts));
  if (x == nullptr)
    return caf::make_error(ec::unspecified, "failed to construct value index");
  return x->deserialize(source);
}

} // namespace vast
