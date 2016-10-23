#include <cmath>

#include "vast/value_index.hpp"

namespace vast {

// TODO: parse type attributes to figure base, binner, etc.
std::unique_ptr<value_index> value_index::make(vast::type const& t) {
  struct factory {
    using result_type = std::unique_ptr<value_index>;
    result_type operator()(none_type const&) const {
      return nullptr;
    }
    result_type operator()(boolean_type const&) const {
      return std::make_unique<arithmetic_index<boolean>>();
    }
    result_type operator()(integer_type const&) const {
      auto b = base::uniform<64>(10);
      return std::make_unique<arithmetic_index<integer>>(std::move(b));
    }
    result_type operator()(count_type const&) const {
      auto b = base::uniform<64>(10);
      return std::make_unique<arithmetic_index<count>>(std::move(b));
    }
    result_type operator()(real_type const&) const {
      auto b = base::uniform<64>(10);
      return std::make_unique<arithmetic_index<real>>(std::move(b));
    }
    result_type operator()(interval_type const&) const {
      auto b = base::uniform<64>(10);
      return std::make_unique<arithmetic_index<interval>>(std::move(b));
    }
    result_type operator()(timestamp_type const&) const {
      auto b = base::uniform<64>(10);
      return std::make_unique<arithmetic_index<timestamp>>(std::move(b));
    }
    result_type operator()(string_type const&) const {
      return nullptr;
    }
    result_type operator()(pattern_type const&) const {
      return nullptr;
    }
    result_type operator()(address_type const&) const {
      return nullptr;
    }
    result_type operator()(subnet_type const&) const {
      return nullptr;
    }
    result_type operator()(port_type const&) const {
      return nullptr;
    }
    result_type operator()(enumeration_type const&) const {
      return nullptr;
    }
    result_type operator()(vector_type const&) const {
      return nullptr;
    }
    result_type operator()(set_type const&) const {
      return nullptr;
    }
    result_type operator()(table_type const&) const {
      return nullptr;
    }
    result_type operator()(record_type const&) const {
      return nullptr;
    }
    result_type operator()(alias_type const&) const {
      return nullptr;
    }
  };
  auto result = visit(factory{}, t);
  result->type_ = t;
  return result;
}

bool value_index::push_back(data const& x) {
  mask_.append_bit(true);
  if (is<none>(x)) {
    none_.append_bit(true);
    return true;
  }
  none_.append_bit(false);
  return push_back_impl(x, 0);
}

bool value_index::push_back(data const& x, event_id id) {
  auto size = none_.size();
  if (id < size)
    return false; // Can only append at the end.
  auto skip = id - size;
  none_.append_bits(false, skip);
  mask_.append_bits(false, skip);
  mask_.append_bit(true);
  if (is<none>(x)) {
    none_.append_bit(true);
    return true;
  }
  none_.append_bit(false);
  return push_back_impl(x, skip);
}

maybe<bitmap> value_index::lookup(relational_operator op, data const& x) const {
  if (is<none>(x)) {
    if (!(op == equal || op == not_equal))
      return fail<ec::unsupported_operator>(op);
    return op == equal ? none_ & mask_ : ~none_ & mask_;
  }
  auto result = lookup_impl(op, x);
  if (!result)
    return result;
  return *result & mask_;
}

type const& value_index::type() const {
  return type_;
}

value_index::value_index(vast::type t) : type_{std::move(t)} {
  // nop
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

bool string_index::push_back_impl(data const& x, size_type skip) {
  auto str = get_if<std::string>(x);
  if (!str)
    return false;
  init();
  auto length = str->size();
  if (length > max_length_)
    length = max_length_;
  if (length > chars_.size())
    chars_.resize(length, char_bitmap_index{8});
  for (auto i = 0u; i < length; ++i) {
    VAST_ASSERT(length_.size() >= chars_[i].size());
    auto gap = length_.size() - chars_[i].size();
    chars_[i].push_back(static_cast<uint8_t>((*str)[i]), gap + skip);
  }
  length_.push_back(length, skip);
  return true;
}

maybe<bitmap>
string_index::lookup_impl(relational_operator op, data const& x) const {
  auto str = get_if<std::string>(x);
  if (!str)
    return fail<ec::type_clash>(x);
  auto length = str->size();
  if (length > max_length_)
    length = max_length_;
  auto size = length_.size();
  switch (op) {
    default:
      return fail<ec::unsupported_operator>(op);
    case equal:
    case not_equal: {
      if (length == 0) {
        auto result = length_.lookup(equal, 0);
        if (op == not_equal)
          result.flip();
        return result;
      }
      if (length > chars_.size())
        return bitmap{size, op == not_equal};
      auto result = length_.lookup(less_equal, length);
      if (all<0>(result))
        return bitmap{size, op == not_equal};
      for (auto i = 0u; i < length; ++i) {
        auto b = chars_[i].lookup(equal, static_cast<uint8_t>((*str)[i]));
        result &= b;
        if (all<0>(result))
          return bitmap{size, op == not_equal};
      }
      if (op == not_equal)
        result.flip();
      return result;
    }
    case ni:
    case not_ni: {
      if (length == 0)
        return bitmap{size, op == ni};
      if (length > chars_.size())
        return bitmap{size, op == not_ni};
      // TODO: Be more clever than iterating over all k-grams (#45).
      bitmap result{size, false};
      for (auto i = 0u; i < chars_.size() - length + 1; ++i) {
        bitmap substr{size, 1};
        auto skip = false;
        for (auto j = 0u; j < length; ++j) {
          auto bm = chars_[i + j].lookup(equal, (*str)[j]);
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
}

void address_index::init() {
  if (bytes_[0].coder().storage().empty())
    // Initialize on first to make deserialization feasible.
    bytes_.fill(byte_index{8});
}

bool address_index::push_back_impl(data const& x, size_type skip) {
  init();
  auto addr = get_if<address>(x);
  if (!addr)
    return false;
  auto& bytes = addr->data();
  if (addr->is_v4()) {
    for (auto i = 12u; i < 16; ++i)
      bytes_[i].push_back(bytes[i], skip);
    v4_.push_back(true, skip);
  } else {
    for (auto i = 0; i < 16; ++i) {
      auto gap = v4_.size() - bytes_[i].size();
      bytes_[i].push_back(bytes[i], gap + skip);
    }
    v4_.push_back(false, skip);
  }
  return true;
}

maybe<bitmap>
address_index::lookup_impl(relational_operator op, data const& x) const {
  auto size = v4_.size();
  if (auto addr = get_if<address>(x)) {
    if (!(op == equal || op == not_equal))
      return fail<ec::unsupported_operator>(op);
    auto& bytes = addr->data();
    auto result = addr->is_v4() ? v4_.coder().storage() : bitmap{size, true};
    for (auto i = addr->is_v4() ? 12u : 0u; i < 16; ++i) {
      auto bm = bytes_[i].lookup(equal, bytes[i]);
      result &= bm;
      if (all<0>(result))
        return bitmap{size, op == not_equal};
    }
    if (op == not_equal)
      result.flip();
    return result;
  } else if (auto sn = get_if<subnet>(x)) {
    if (!(op == in || op == not_in))
      return fail<ec::unsupported_operator>(op);
    auto topk = sn->length();
    if (topk == 0)
      return fail("invalid IP subnet length: ", topk);
    auto& net = sn->network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      // Asking for /32 or /128 membership is equivalent to an equality lookup.
      return lookup_impl(op == in ? equal : not_equal, sn->network());
    auto result = is_v4 ? v4_.coder().storage() : bitmap{size, true};
    auto& bytes = net.data();
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
  }
  return fail<ec::type_clash>(x);
}

void subnet_index::init() {
  if (length_.coder().storage().empty())
    length_ = prefix_index{128 + 1}; // Valid prefixes range from /0 to /128.
}

bool subnet_index::push_back_impl(data const& x, size_type skip) {
  if (auto sn = get_if<subnet>(x)) {
    init();
    auto id = length_.size() + skip;
    length_.push_back(sn->length(), skip);
    return network_.push_back(sn->network(), id);
  }
  return false;
}

maybe<bitmap>
subnet_index::lookup_impl(relational_operator op, data const& x) const {
  if (!(op == equal || op == not_equal))
    return fail<ec::unsupported_operator>(op);
  auto sn = get_if<subnet>(x);
  if (!sn)
    return fail<ec::type_clash>(x);
  auto result = network_.lookup(equal, sn->network());
  if (!result)
    return result;
  auto n = length_.lookup(equal, sn->length());
  *result &= n;
  if (op == not_equal)
    result->flip();
  return result;
}


void port_index::init() {
  if (num_.coder().storage().empty()) {
    num_ = number_index{base::uniform(10, 5)}; // [0, 2^16)
    proto_ = protocol_index{4}; // unknown, tcp, udp, icmp
  }
}

bool port_index::push_back_impl(data const& x, size_type skip) {
  if (auto p = get_if<port>(x)) {
    init();
    num_.push_back(p->number(), skip);
    proto_.push_back(p->type(), skip);
    return true;
  }
  return false;
}

maybe<bitmap>
port_index::lookup_impl(relational_operator op, data const& x) const {
  if (op == in || op == not_in)
    return fail<ec::unsupported_operator>(op);
  if (num_.empty())
    return bitmap{};
  auto p = get_if<port>(x);
  if (!p)
    return fail<ec::type_clash>(x);
  auto n = num_.lookup(op, p->number());
  if (all<0>(n))
    return bitmap{proto_.size(), false};
  if (p->type() != port::unknown)
    n &= proto_.lookup(equal, p->type());
  return n;
}

} // namespace vast
