#include <cmath>

#include "vast/base.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/base.hpp"
#include "vast/value_index.hpp"

namespace vast {
namespace {

optional<std::string> extract_attribute(type const& t, std::string const& key) {
  for (auto& attr : t.attributes())
    if (attr.key == key && attr.value)
      return attr.value;
  return {};
}

optional<base> parse_base(type const& t) {
  if (auto a = extract_attribute(t, "base")) {
    if (auto b = to<base>(*a))
      return *b;
    return {};
  }
  return base::uniform<64>(10);
}

} // namespace <anonymous>

std::unique_ptr<value_index> value_index::make(type const& t) {
  struct factory {
    using result_type = std::unique_ptr<value_index>;
    result_type operator()(none_type const&) const {
      return nullptr;
    }
    result_type operator()(boolean_type const&) const {
      return std::make_unique<arithmetic_index<boolean>>();
    }
    result_type operator()(integer_type const& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<integer>>(std::move(*b));
    }
    result_type operator()(count_type const& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<count>>(std::move(*b));
    }
    result_type operator()(real_type const& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<real>>(std::move(*b));
    }
    result_type operator()(interval_type const& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<interval>>(std::move(*b));
    }
    result_type operator()(timestamp_type const& t) const {
      auto b = parse_base(t);
      if (!b)
        return nullptr;
      return std::make_unique<arithmetic_index<timestamp>>(std::move(*b));
    }
    result_type operator()(string_type const& t) const {
      auto max_length = size_t{1024};
      if (auto a = extract_attribute(t, "max_length")) {
        if (auto x = to<size_t>(*a))
          max_length = *x;
        else
          return nullptr;
      }
      return std::make_unique<string_index>(max_length);
    }
    result_type operator()(pattern_type const&) const {
      return nullptr;
    }
    result_type operator()(address_type const&) const {
      return std::make_unique<address_index>();
    }
    result_type operator()(subnet_type const&) const {
      return std::make_unique<subnet_index>();
    }
    result_type operator()(port_type const&) const {
      return std::make_unique<port_index>();
    }
    result_type operator()(enumeration_type const&) const {
      return nullptr;
    }
    result_type operator()(vector_type const& t) const {
      auto max_size = size_t{1024};
      if (auto a = extract_attribute(t, "max_size")) {
        if (auto x = to<size_t>(*a))
          max_size = *x;
        else
          return nullptr;
      }
      return std::make_unique<sequence_index>(t.value_type, max_size);
    }
    result_type operator()(set_type const& t) const {
      auto max_size = size_t{1024};
      if (auto a = extract_attribute(t, "max_size")) {
        if (auto x = to<size_t>(*a))
          max_size = *x;
        else
          return nullptr;
      }
      return std::make_unique<sequence_index>(t.value_type, max_size);
    }
    result_type operator()(table_type const&) const {
      return nullptr;
    }
    result_type operator()(record_type const&) const {
      return nullptr;
    }
    result_type operator()(alias_type const& t) const {
      return visit(*this, t.value_type);
    }
  };
  return visit(factory{}, t);
}

expected<void> value_index::push_back(data const& x) {
  if (is<none>(x)) {
    none_.append_bit(true);
  } else {
    if (!push_back_impl(x, 0))
      return make_error(ec::unspecified, "push_back_impl");
    none_.append_bit(false);
  }
  mask_.append_bit(true);
  return {};
}

expected<void> value_index::push_back(data const& x, event_id id) {
  auto off = offset();
  if (id < off)
    // Can only append at the end.
    return make_error(ec::unspecified, id, '<', off);
  if (id == off)
    return push_back(x);
  auto skip = id - off;
  if (is<none>(x)) {
    none_.append_bits(false, skip);
    none_.append_bit(true);
  } else {
    if (!push_back_impl(x, skip))
      return make_error(ec::unspecified, "push_back_impl");
    none_.append_bits(false, skip + 1);
  }
  mask_.append_bits(false, skip);
  mask_.append_bit(true);
  return {};
}

expected<bitmap>
value_index::lookup(relational_operator op, data const& x) const {
  if (is<none>(x)) {
    if (!(op == equal || op == not_equal))
      return make_error(ec::unsupported_operator, op);
    return op == equal ? none_ & mask_ : ~none_ & mask_;
  }
  auto result = lookup_impl(op, x);
  if (!result)
    return result;
  return *result & mask_;
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
    VAST_ASSERT(offset() >= chars_[i].size());
    auto gap = offset() - chars_[i].size();
    chars_[i].push_back(static_cast<uint8_t>((*str)[i]), gap + skip);
  }
  length_.push_back(length, skip);
  return true;
}

expected<bitmap>
string_index::lookup_impl(relational_operator op, data const& x) const {
  auto str = get_if<std::string>(x);
  if (!str)
    return make_error(ec::type_clash, x);
  auto str_size = str->size();
  if (str_size > max_length_)
    str_size = max_length_;
  auto off = offset();
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
        return bitmap{off, op == not_equal};
      auto result = length_.lookup(less_equal, str_size);
      if (all<0>(result))
        return bitmap{off, op == not_equal};
      for (auto i = 0u; i < str_size; ++i) {
        auto b = chars_[i].lookup(equal, static_cast<uint8_t>((*str)[i]));
        result &= b;
        if (all<0>(result))
          return bitmap{off, op == not_equal};
      }
      if (op == not_equal)
        result.flip();
      return result;
    }
    case ni:
    case not_ni: {
      if (str_size == 0)
        return bitmap{off, op == ni};
      if (str_size > chars_.size())
        return bitmap{off, op == not_ni};
      // TODO: Be more clever than iterating over all k-grams (#45).
      bitmap result{off, false};
      for (auto i = 0u; i < chars_.size() - str_size + 1; ++i) {
        bitmap substr{off, 1};
        auto skip = false;
        for (auto j = 0u; j < str_size; ++j) {
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
      auto gap = offset() - bytes_[i].size();
      bytes_[i].push_back(bytes[i], gap + skip);
    }
    v4_.push_back(false, skip);
  }
  return true;
}

expected<bitmap>
address_index::lookup_impl(relational_operator op, data const& x) const {
  auto off = offset();
  if (auto addr = get_if<address>(x)) {
    if (!(op == equal || op == not_equal))
      return make_error(ec::unsupported_operator, op);
    auto& bytes = addr->data();
    auto result = addr->is_v4() ? v4_.coder().storage() : bitmap{off, true};
    for (auto i = addr->is_v4() ? 12u : 0u; i < 16; ++i) {
      auto bm = bytes_[i].lookup(equal, bytes[i]);
      result &= bm;
      if (all<0>(result))
        return bitmap{off, op == not_equal};
    }
    if (op == not_equal)
      result.flip();
    return result;
  } else if (auto sn = get_if<subnet>(x)) {
    if (!(op == in || op == not_in))
      return make_error(ec::unsupported_operator, op);
    auto topk = sn->length();
    if (topk == 0)
      return make_error(ec::unspecified, "invalid IP subnet length: ", topk);
    auto& net = sn->network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      // Asking for /32 or /128 membership is equivalent to an equality lookup.
      return lookup_impl(op == in ? equal : not_equal, sn->network());
    auto result = is_v4 ? v4_.coder().storage() : bitmap{off, true};
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
  return make_error(ec::type_clash, x);
}

void subnet_index::init() {
  if (length_.coder().storage().empty())
    length_ = prefix_index{128 + 1}; // Valid prefixes range from /0 to /128.
}

bool subnet_index::push_back_impl(data const& x, size_type skip) {
  if (auto sn = get_if<subnet>(x)) {
    init();
    auto id = offset() + skip;
    length_.push_back(sn->length(), skip);
    return !!network_.push_back(sn->network(), id);
  }
  return false;
}

expected<bitmap>
subnet_index::lookup_impl(relational_operator op, data const& x) const {
  if (!(op == equal || op == not_equal))
    return make_error(ec::unsupported_operator, op);
  auto sn = get_if<subnet>(x);
  if (!sn)
    return make_error(ec::type_clash, x);
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

expected<bitmap>
port_index::lookup_impl(relational_operator op, data const& x) const {
  if (op == in || op == not_in)
    return make_error(ec::unsupported_operator, op);
  if (offset() == 0)
    return bitmap{};
  auto p = get_if<port>(x);
  if (!p)
    return make_error(ec::type_clash, x);
  auto n = num_.lookup(op, p->number());
  if (all<0>(n))
    return bitmap{offset(), false};
  if (p->type() != port::unknown)
    n &= proto_.lookup(equal, p->type());
  return n;
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

bool sequence_index::push_back_impl(data const& x, size_type skip) {
  auto v = get_if<vector>(x);
  if (v)
    return push_back_ctnr(*v, skip);
  auto s = get_if<set>(x);
  if (s)
    return push_back_ctnr(*s, skip);
  return false;
}

expected<bitmap>
sequence_index::lookup_impl(relational_operator op, data const& x) const {
  if (op == ni)
    op = in;
  else if (op == not_ni)
    op = not_in;
  if (!(op == in || op == not_in))
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
  if (op == not_in)
    result->flip();
  return result;
}

void serialize(caf::serializer& sink, sequence_index const& idx) {
  sink & static_cast<value_index const&>(idx);
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
