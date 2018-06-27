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

#include <cmath>

#include "vast/base.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/value_index.hpp"

namespace vast {
namespace {

optional<std::string> extract_attribute(const type& t, const std::string& key) {
  for (auto& attr : t.attributes())
    if (attr.key == key && attr.value)
      return attr.value;
  return {};
}

optional<base> parse_base(const type& t) {
  if (auto a = extract_attribute(t, "base")) {
    if (auto b = to<base>(*a))
      return *b;
    return {};
  }
  return base::uniform<64>(10);
}

} // namespace <anonymous>

value_index::~value_index() {
  // nop
}

std::unique_ptr<value_index> value_index::make(const type& t) {
  struct factory {
    using result_type = std::unique_ptr<value_index>;
    result_type operator()(const none_type&) const {
      return nullptr;
    }
    result_type operator()(const boolean_type&) const {
      return std::make_unique<arithmetic_index<boolean>>();
    }
    result_type operator()(const integer_type& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<integer>>(std::move(*b));
    }
    result_type operator()(const count_type& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<count>>(std::move(*b));
    }
    result_type operator()(const real_type& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<real>>(std::move(*b));
    }
    result_type operator()(const timespan_type& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<timespan>>(std::move(*b));
    }
    result_type operator()(const timestamp_type& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<timestamp>>(std::move(*b));
    }
    result_type operator()(const string_type& t) const {
      auto max_length = size_t{1024};
      if (auto a = extract_attribute(t, "max_length")) {
        if (auto x = to<size_t>(*a))
          max_length = *x;
        else
          return nullptr;
      }
      return std::make_unique<string_index>(max_length);
    }
    result_type operator()(const pattern_type&) const {
      return nullptr;
    }
    result_type operator()(const address_type&) const {
      return std::make_unique<address_index>();
    }
    result_type operator()(const subnet_type&) const {
      return std::make_unique<subnet_index>();
    }
    result_type operator()(const port_type&) const {
      return std::make_unique<port_index>();
    }
    result_type operator()(const enumeration_type&) const {
      return nullptr;
    }
    result_type operator()(const vector_type& t) const {
      auto max_size = size_t{1024};
      if (auto a = extract_attribute(t, "max_size")) {
        if (auto x = to<size_t>(*a))
          max_size = *x;
        else
          return nullptr;
      }
      return std::make_unique<sequence_index>(t.value_type, max_size);
    }
    result_type operator()(const set_type& t) const {
      auto max_size = size_t{1024};
      if (auto a = extract_attribute(t, "max_size")) {
        if (auto x = to<size_t>(*a))
          max_size = *x;
        else
          return nullptr;
      }
      return std::make_unique<sequence_index>(t.value_type, max_size);
    }
    result_type operator()(const map_type&) const {
      return nullptr;
    }
    result_type operator()(const record_type&) const {
      return nullptr;
    }
    result_type operator()(const alias_type& t) const {
      return caf::visit(*this, t.value_type);
    }
  };
  return caf::visit(factory{}, t);
}

expected<void> value_index::push_back(const data& x) {
  if (caf::holds_alternative<none>(x)) {
    none_.append_bit(true);
    ++nils_;
  } else {
    if (!push_back_impl(x, nils_))
      return make_error(ec::unspecified, "push_back_impl");
    nils_ = 0;
    none_.append_bit(false);
  }
  mask_.append_bit(true);
  return {};
}

expected<void> value_index::push_back(const data& x, id pos) {
  auto off = offset();
  if (pos < off)
    // Can only append at the end
    return make_error(ec::unspecified, pos, '<', off);
  if (pos == off)
    return push_back(x);
  auto skip = pos - off;
  if (caf::holds_alternative<none>(x)) {
    none_.append_bits(false, skip);
    none_.append_bit(true);
    ++nils_;
  } else {
    if (!push_back_impl(x, skip + nils_))
      return make_error(ec::unspecified, "push_back_impl");
    nils_ = 0;
    none_.append_bits(false, skip + 1);
  }
  mask_.append_bits(false, skip);
  mask_.append_bit(true);
  return {};
}

expected<ids> value_index::lookup(relational_operator op, const data& x) const {
  if (caf::holds_alternative<none>(x)) {
    if (!(op == equal || op == not_equal))
      return make_error(ec::unsupported_operator, op);
    return op == equal ? none_ & mask_ : ~none_ & mask_;
  }
  auto result = lookup_impl(op, x);
  if (!result)
    return result;
  return (*result - none_) & mask_;
}

value_index::size_type value_index::offset() const {
  return mask_.size(); // none_ would work just as well.
}


string_index::string_index(size_t max_length) : max_length_{max_length} {
}

void string_index::init() {
  if (length_.coder().storage().empty()) {
    size_t components = std::log10(max_length_);
    if (max_length_ % 10 != 0)
      ++components;
    length_ = length_bitmap_index{base::uniform(10, components)};
  }
}

bool string_index::push_back_impl(const data& x, size_type skip) {
  auto str = caf::get_if<std::string>(&x);
  if (!str)
    return false;
  init();
  auto length = str->size();
  if (length > max_length_)
    length = max_length_;
  if (length > chars_.size())
    chars_.resize(length, char_bitmap_index{8});
  for (auto i = 0u; i < length; ++i) {
    auto gap = length_.size() - chars_[i].size();
    chars_[i].push_back(static_cast<uint8_t>((*str)[i]), gap + skip);
  }
  length_.push_back(length, skip);
  return true;
}

expected<ids>
string_index::lookup_impl(relational_operator op, const data& x) const {
  return caf::visit(detail::overload(
    [&](const auto& x) -> expected<ids> {
      return make_error(ec::type_clash, x);
    },
    [&](const std::string& str) -> expected<ids> {
      auto str_size = str.size();
      if (str_size > max_length_)
        str_size = max_length_;
      switch (op) {
        default:
          return make_error(ec::unsupported_operator, op);
        case equal:
        case not_equal: {
          if (str_size == 0) {
            auto result = length_.lookup(equal, 0);
            if (op == not_equal)
              result.flip();
            return result;
          }
          if (str_size > chars_.size())
            return bitmap{length_.size(), op == not_equal};
          auto result = length_.lookup(less_equal, str_size);
          if (all<0>(result))
            return bitmap{length_.size(), op == not_equal};
          for (auto i = 0u; i < str_size; ++i) {
            auto b = chars_[i].lookup(equal, static_cast<uint8_t>(str[i]));
            result &= b;
            if (all<0>(result))
              return bitmap{length_.size(), op == not_equal};
          }
          if (op == not_equal)
            result.flip();
          return result;
        }
        case ni:
        case not_ni: {
          if (str_size == 0)
            return bitmap{length_.size(), op == ni};
          if (str_size > chars_.size())
            return bitmap{length_.size(), op == not_ni};
          // TODO: Be more clever than iterating over all k-grams (#45).
          bitmap result{length_.size(), false};
          for (auto i = 0u; i < chars_.size() - str_size + 1; ++i) {
            bitmap substr{length_.size(), true};
            auto skip = false;
            for (auto j = 0u; j < str_size; ++j) {
              auto bm = chars_[i + j].lookup(equal, str[j]);
              if (all<0>(bm)) {
                skip = true;
                break;
              }
              substr &= bm;
            }
            if (!skip)
              result |= substr;
          }
          if (op == not_ni)
            result.flip();
          return result;
        }
      }
    },
    [&](const vector& xs) { return detail::container_lookup(*this, op, xs); },
    [&](const set& xs) { return detail::container_lookup(*this, op, xs); }
  ), x);
}

void address_index::init() {
  if (bytes_[0].coder().storage().empty())
    // Initialize on first to make deserialization feasible.
    bytes_.fill(byte_index{8});
}

bool address_index::push_back_impl(const data& x, size_type skip) {
  init();
  auto addr = caf::get_if<address>(&x);
  if (!addr)
    return false;
  auto& bytes = addr->data();
  if (addr->is_v6())
    for (auto i = 0u; i < 12; ++i) {
      auto gap = v4_.size() - bytes_[i].size();
      bytes_[i].push_back(bytes[i], gap + skip);
    }
  for (auto i = 12u; i < 16; ++i)
    bytes_[i].push_back(bytes[i], skip);
  v4_.push_back(addr->is_v4(), skip);
  return true;
}

expected<ids>
address_index::lookup_impl(relational_operator op, const data& d) const {
  return caf::visit(detail::overload(
    [&](const auto& x) -> expected<ids> {
      return make_error(ec::type_clash, x);
    },
    [&](const address& x) -> expected<ids> {
      if (!(op == equal || op == not_equal))
        return make_error(ec::unsupported_operator, op);
      auto result = x.is_v4() ? v4_.coder().storage() : bitmap{v4_.size(), true};
      for (auto i = x.is_v4() ? 12u : 0u; i < 16; ++i) {
        auto bm = bytes_[i].lookup(equal, x.data()[i]);
        result &= bm;
        if (all<0>(result))
          return bitmap{v4_.size(), op == not_equal};
      }
      if (op == not_equal)
        result.flip();
      return result;
    },
    [&](const subnet& x) -> expected<ids> {
      if (!(op == in || op == not_in))
        return make_error(ec::unsupported_operator, op);
      auto topk = x.length();
      if (topk == 0)
        return make_error(ec::unspecified, "invalid IP subnet length: ", topk);
      auto is_v4 = x.network().is_v4();
      if ((is_v4 ? topk + 96 : topk) == 128)
        // Asking for /32 or /128 membership is equivalent to an equality lookup.
        return lookup_impl(op == in ? equal : not_equal, x.network());
      auto result = is_v4 ? v4_.coder().storage() : bitmap{v4_.size(), true};
      auto& bytes = x.network().data();
      size_t i = is_v4 ? 12 : 0;
      for ( ; i < 16 && topk >= 8; ++i, topk -= 8)
        result &= bytes_[i].lookup(equal, bytes[i]);
      for (auto j = 0u; j < topk; ++j) {
        auto bit = 7 - j;
        auto& bm = bytes_[i].coder().storage()[bit];
        result &= (bytes[i] >> bit) & 1 ? ~bm : bm;
      }
      if (op == not_in)
        result.flip();
      return result;
    },
    [&](const vector& xs) { return detail::container_lookup(*this, op, xs); },
    [&](const set& xs) { return detail::container_lookup(*this, op, xs); }
  ), d);
}

void subnet_index::init() {
  if (length_.coder().storage().empty())
    length_ = prefix_index{128 + 1}; // Valid prefixes range from /0 to /128.
}

bool subnet_index::push_back_impl(const data& x, size_type skip) {
  if (auto sn = caf::get_if<subnet>(&x)) {
    init();
    auto id = length_.size() + skip;
    length_.push_back(sn->length(), skip);
    return !!network_.push_back(sn->network(), id);
  }
  return false;
}

expected<ids>
subnet_index::lookup_impl(relational_operator op, const data& d) const {
  return caf::visit(detail::overload(
    [&](const auto& x) -> expected<ids> {
      return make_error(ec::type_clash, x);
    },
    [&](const subnet& x) -> expected<ids> {
      switch (op) {
        default:
          return make_error(ec::unsupported_operator, op);
        case equal:
        case not_equal: {
          auto result = network_.lookup(equal, x.network());
          if (!result)
            return result;
          auto n = length_.lookup(equal, x.length());
          *result &= n;
          if (op == not_equal)
            result->flip();
          return result;
        }
        case in:
        case not_in: {
          // For a subnet index U and subnet x, the in operator signifies a
          // subset relationship such that `U in x` translates to U ⊆ x, i.e.,
          // the lookup returns all subnets in U that are a subset of x.
          auto result = network_.lookup(in, x);
          if (!result)
            return result;
          *result &= length_.lookup(greater_equal, x.length());
          if (op == not_in)
            result->flip();
          return result;
        }
        case ni:
        case not_ni: {
          // For a subnet index U and subnet x, the ni operator signifies a
          // subset relationship such that `U ni x` translates to U ⊇ x, i.e.,
          // the lookup returns all subnets in U that include x.
          bitmap result;
          for (auto i = uint8_t{1}; i <= x.length(); ++i) {
            auto xs = network_.lookup(in, subnet{x.network(), i});
            if (!xs)
              return xs;
            *xs &= length_.lookup(equal, i);
            result |= *xs;
          }
          if (op == not_ni)
            result.flip();
          return result;
        }
      }
    },
    [&](const vector& xs) { return detail::container_lookup(*this, op, xs); },
    [&](const set& xs) { return detail::container_lookup(*this, op, xs); }
  ), d);
}


void port_index::init() {
  if (num_.coder().storage().empty()) {
    num_ = number_index{base::uniform(10, 5)}; // [0, 2^16)
    proto_ = protocol_index{4}; // unknown, tcp, udp, icmp
  }
}

bool port_index::push_back_impl(const data& x, size_type skip) {
  if (auto p = caf::get_if<port>(&x)) {
    init();
    num_.push_back(p->number(), skip);
    proto_.push_back(p->type(), skip);
    return true;
  }
  return false;
}

expected<ids>
port_index::lookup_impl(relational_operator op, const data& d) const {
  if (offset() == 0) // FIXME: why do we need this check again?
    return ids{};
  return caf::visit(detail::overload(
    [&](const auto& x) -> expected<ids> {
      return make_error(ec::type_clash, x);
    },
    [&](const port& x) -> expected<ids> {
      if (op == in || op == not_in)
        return make_error(ec::unsupported_operator, op);
      auto n = num_.lookup(op, x.number());
      if (all<0>(n))
        return bitmap{offset(), false};
      if (x.type() != port::unknown)
        n &= proto_.lookup(equal, x.type());
      return n;
    },
    [&](const vector& xs) { return detail::container_lookup(*this, op, xs); },
    [&](const set& xs) { return detail::container_lookup(*this, op, xs); }
  ), d);
}


sequence_index::sequence_index(vast::type t, size_t max_size)
  : max_size_{max_size},
    value_type_{std::move(t)} {
}

void sequence_index::init() {
  if (size_.coder().storage().empty()) {
    size_t components = std::log10(max_size_);
    if (max_size_ % 10 != 0)
      ++components;
    size_ = size_bitmap_index{base::uniform(10, components)};
  }
}

bool sequence_index::push_back_impl(const data& x, size_type skip) {
  if (auto v = caf::get_if<vector>(&x))
    return push_back_ctnr(*v, skip);
  if (auto s = caf::get_if<set>(&x))
    return push_back_ctnr(*s, skip);
  return false;
}

expected<ids>
sequence_index::lookup_impl(relational_operator op, const data& x) const {
  if (!(op == ni || op == not_ni))
    return make_error(ec::unsupported_operator, op);
  if (elements_.empty())
    return bitmap{};
  auto result = elements_[0]->lookup(equal, x);
  if (!result)
    return result;
  for (auto i = 1u; i < elements_.size(); ++i) {
    auto mbm = elements_[i]->lookup(equal, x);
    if (mbm)
      *result |= *mbm;
    else
      return mbm;
  }
  if (op == not_ni)
    result->flip();
  return result;
}

void serialize(caf::serializer& sink, const sequence_index& idx) {
  sink & static_cast<const value_index&>(idx);
  sink & idx.value_type_;
  sink & idx.max_size_;
  sink & idx.size_;
  // Polymorphic indexes.
  std::vector<detail::value_index_inspect_helper> xs;
  xs.reserve(idx.elements_.size());
  std::transform(idx.elements_.begin(),
                 idx.elements_.end(),
                 std::back_inserter(xs),
                 [&](auto& vi) {
                   auto& t = const_cast<type&>(idx.value_type_);
                   auto& x = const_cast<std::unique_ptr<value_index>&>(vi);
                   return detail::value_index_inspect_helper{t, x};
                 });
  sink & xs;
}

void serialize(caf::deserializer& source, sequence_index& idx) {
  source & static_cast<value_index&>(idx);
  source & idx.value_type_;
  source & idx.max_size_;
  source & idx.size_;
  // Polymorphic indexes.
  size_t n;
  auto construct = [&] {
    idx.elements_.resize(n);
    std::vector<detail::value_index_inspect_helper> xs;
    xs.reserve(n);
    std::transform(idx.elements_.begin(),
                   idx.elements_.end(),
                   std::back_inserter(xs),
                   [&](auto& vi) {
                     auto& t = idx.value_type_;
                     auto& x = vi;
                     return detail::value_index_inspect_helper{t, x};
                   });
    for (auto& x : xs)
      source & x;
    return error{};
  };
  auto e = error::eval(
    [&] { return source.begin_sequence(n); },
    [&] { return construct(); },
    [&] { return source.end_sequence(); }
  );
  if (e)
    throw std::runtime_error{to_string(e)};
}

} // namespace vast
