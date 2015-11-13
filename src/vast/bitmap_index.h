#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/operator.h"
#include "vast/maybe.h"
#include "vast/trial.h"
#include "vast/value.h"
#include "vast/concept/printable/vast/data.h"
#include "vast/concept/printable/vast/operator.h"
#include "vast/util/assert.h"
#include "vast/util/operators.h"

namespace vast {

struct access;
namespace detail {
template <typename>
struct bitmap_index_model;
}

/// The base class for bitmap indexes.
template <typename Derived, typename Bitstream>
class bitmap_index_base
  : util::equality_comparable<bitmap_index_base<Derived, Bitstream>> {
  friend access;

public:
  /// Appends a single value.
  /// @param x The value to append to the index.
  /// @param offset The position of *x* in the bitmap index.
  /// @returns `true` if appending succeeded.
  template <typename T>
  bool push_back(T const& x, uint64_t offset = 0) {
    return catch_up(offset) && derived()->push_back_impl(x)
           && nil_.push_back(false) && mask_.push_back(true);
  }

  bool push_back(none, uint64_t offset = 0) {
    return catch_up(offset) && stretch(1) && nil_.push_back(true)
           && mask_.push_back(true);
  }

  bool push_back(data const& d, uint64_t offset = 0) {
    return catch_up(offset)
           && (is<none>(d) ? stretch(1) : derived()->push_back_impl(d))
           && nil_.push_back(is<none>(d)) && mask_.push_back(true);
  }

  /// Appends 0-bits to the index.
  /// @param n The number of zeros to append.
  /// @returns `true` on success.
  bool stretch(size_t n) {
    return derived()->stretch_impl(n);
  }

  /// Appends a bitmap index.
  /// @param other The bitmap index to append.
  /// @returns `true` on success.
  bool append(Derived const& other) {
    return derived()->append_impl(other);
  }

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param x The value to lookup.
  template <typename T>
  trial<Bitstream> lookup(relational_operator op, T const& x) const {
    auto r = derived()->lookup_impl(op, x);
    if (r)
      *r &= mask_;
    return r;
  }

  trial<Bitstream> lookup(relational_operator op, none) const {
    if (!(op == equal || op == not_equal))
      return error{"invalid relational operator for nil data: ", op};
    return op == equal ? nil_ & mask_ : ~nil_ & mask_;
  }

  trial<Bitstream> lookup(relational_operator op, data const& d) const {
    if (is<none>(d))
      return lookup(op, nil);
    auto r = derived()->lookup_impl(op, d);
    if (r)
      *r &= mask_;
    return r;
  }

  /// Retrieves the number of elements in the bitmap index.
  /// @returns The number of rows, i.e., values in the bitmap.
  uint64_t size() const {
    return derived()->size_impl();
  }

  /// Checks whether the bitmap is empty.
  /// @returns `true` if `size() == 0`.
  bool empty() const {
    return size() == 0;
  }

  /// Appends invalid bits to bring the bitmap index up to a given size. Given
  /// an ID of *n*, the function stretches the index up to size *n* with
  /// invalid bits.
  /// @param n The ID to catch up to.
  /// @returns `true` on success.
  bool catch_up(uint64_t n) {
    if (n == 0)
      return true;
    if (n < size())
      return false;
    auto delta = n - size();
    if (delta == 0)
      return true;
    return stretch(delta) && nil_.append(delta, false)
           && mask_.append(delta, false);
  }

private:
  Derived* derived() {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const {
    return static_cast<Derived const*>(this);
  }

  Bitstream mask_;
  Bitstream nil_;
};

/// A bitmap index for arithmetic values.
template <typename Bitstream, typename T, typename Binner = void>
class arithmetic_bitmap_index
  : public bitmap_index_base<
             arithmetic_bitmap_index<Bitstream, T, Binner>,
             Bitstream
           >,
           util::equality_comparable<
             arithmetic_bitmap_index<Bitstream, T, Binner>
           > {
  using super =
    bitmap_index_base<arithmetic_bitmap_index<Bitstream, T, Binner>, Bitstream>;
  friend super;
  friend access;
  template <typename> friend struct detail::bitmap_index_model;

  using bitmap_value_type =
    std::conditional_t<
      std::is_same<T, time::point>{} || std::is_same<T, time::duration>{},
      time::duration::rep,
      std::conditional_t<
        std::is_same<T, boolean>{}
          || std::is_same<T, integer>{}
          || std::is_same<T, count>{}
          || std::is_same<T, real>{},
        T,
        std::false_type
      >
    >;

  using bitmap_coder =
    std::conditional_t<
      std::is_same<T, boolean>{},
      singleton_coder<Bitstream>,
      multi_level_coder<uniform_base<10, 20>, range_coder<Bitstream>>
    >;

  using bitmap_binner =
    std::conditional_t<
      std::is_same<Binner, void>{},
      std::conditional_t<
        std::is_same<T, time::point>{}
          || std::is_same<T, time::duration>{},
        decimal_binner<9>, // nanoseconds -> seconds
        std::conditional_t<
          std::is_same<T, real>{},
          precision_binner<10>, // no fractional part
          identity_binner
        >
      >,
      Binner
    >;

  using bitmap_type =
    bitmap<bitmap_value_type, bitmap_coder, bitmap_binner>;

public:
  using bitstream_type = Bitstream;

  arithmetic_bitmap_index() = default;

  friend bool operator==(arithmetic_bitmap_index const& x,
                         arithmetic_bitmap_index const& y) {
    return x.bitmap_ == y.bitmap_;
  }

private:
  struct pusher {
    pusher(bitmap_type& bm) : bm_{bm} {
    }

    template <typename U>
    bool operator()(U) const {
      return false;
    }

    bool operator()(bitmap_value_type x) const {
      return bm_.push_back(x);
    }

    bool operator()(time::point x) const {
      return (*this)(x.time_since_epoch().count());
    }

    bool operator()(time::duration x) const {
      return (*this)(x.count());
    }

    bitmap_type& bm_;
  };

  struct looker {
    looker(bitmap_type const& bm, relational_operator op) : bm_{bm}, op_{op} {
    }

    template <typename U>
    trial<Bitstream> operator()(U const& x) const {
      return error{"invalid type: ", x};
    }

    trial<Bitstream> operator()(bitmap_value_type x) const {
      return bm_.lookup(op_, x);
    }

    trial<Bitstream> operator()(time::point x) const {
      return (*this)(x.time_since_epoch().count());
    }

    trial<Bitstream> operator()(time::duration x) const {
      return (*this)(x.count());
    }

    bitmap_type const& bm_;
    relational_operator op_;
  };

  bool push_back_impl(data const& d) {
    return visit(pusher{bitmap_}, d);
  }

  bool push_back_impl(T x) {
    return pusher{bitmap_}(x);
  }

  bool stretch_impl(size_t n) {
    return bitmap_.stretch(n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};
    return visit(looker{bitmap_, op}, d);
  };

  trial<Bitstream> lookup_impl(relational_operator op, T x) const {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};
    return looker{bitmap_, op}(x);
  };

  uint64_t size_impl() const {
    return bitmap_.size();
  }

  bitmap_type bitmap_;
};

/// A bitmap index for strings.
template <typename Bitstream>
class string_bitmap_index
  : public bitmap_index_base<string_bitmap_index<Bitstream>, Bitstream> {
  using super = bitmap_index_base<string_bitmap_index<Bitstream>, Bitstream>;
  friend super;
  friend access;
  template <typename>
  friend struct detail::bitmap_index_model;

  static constexpr size_t max_string_length = 8192;

public:
  using bitstream_type = Bitstream;
  using char_bitmap_type = bitmap<uint8_t, bitslice_coder<Bitstream>>;
  using length_bitmap_type = bitmap<
    uint32_t, multi_level_coder<uniform_base<10, 4>, range_coder<Bitstream>>
  >;

  string_bitmap_index() = default;

  friend bool operator==(string_bitmap_index const& x,
                         string_bitmap_index const& y) {
    return x.bitmaps_ == y.bitmaps_;
  }

private:
  template <typename Iterator>
  bool push_back_string(Iterator begin, Iterator end) {
    auto length = static_cast<size_t>(end - begin);
    VAST_ASSERT(length < max_string_length);
    if (length > bitmaps_.size())
      bitmaps_.resize(length, char_bitmap_type{8});
    for (size_t i = 0; i < length; ++i) {
      VAST_ASSERT(this->size() >= bitmaps_[i].size());
      auto delta = this->size() - bitmaps_[i].size();
      if (delta > 0 && !bitmaps_[i].stretch(delta))
        return false;
      if (!bitmaps_[i].push_back(static_cast<uint8_t>(begin[i])))
        return false;
    }
    return length_.push_back(length);
  }

  bool push_back_impl(data const& d) {
    auto str = get<std::string>(d);
    return str && push_back_impl(*str);
  }

  bool push_back_impl(std::string const& str) {
    return push_back_string(str.begin(), str.end());
  }

  template <size_t N>
  bool push_back_impl(char const(&str)[N]) {
    return push_back_string(str, str + N - 1);
  }

  bool stretch_impl(size_t n) {
    return length_.stretch(n);
  }

  template <typename Iterator>
  trial<Bitstream> lookup_string(relational_operator op, Iterator begin,
                                 Iterator end) const {
    auto length = static_cast<size_t>(end - begin);
    VAST_ASSERT(length < max_string_length);
    switch (op) {
      default:
        return error{"unsupported relational operator: ", op};
      case equal:
      case not_equal: {
        if (length == 0) {
          auto l = length_.lookup(equal, 0);
          return std::move(op == equal ? l : l.flip());
        }
        if (length > bitmaps_.size())
          return Bitstream{this->size(), op == not_equal};
        auto r = length_.lookup(less_equal, length);
        if (r.all_zeros())
          return Bitstream{this->size(), op == not_equal};
        for (size_t i = 0; i < length; ++i) {
          auto b = bitmaps_[i].lookup(equal, static_cast<uint8_t>(begin[i]));
          if (!b.all_zeros())
            r &= b;
          else
            return Bitstream{this->size(), op == not_equal};
        }
        return std::move(op == equal ? r : r.flip());
      }
      case ni:
      case not_ni: {
        if (length == 0)
          return Bitstream{this->size(), op == ni};
        if (length > bitmaps_.size())
          return Bitstream{this->size(), op == not_ni};
        // TODO: Be more clever than iterating over all k-grams (#45).
        Bitstream r{this->size(), 0};
        for (size_t i = 0; i < bitmaps_.size() - length + 1; ++i) {
          Bitstream substr{this->size(), 1};
          auto skip = false;
          for (size_t j = 0; j < length; ++j) {
            auto bs = bitmaps_[i + j].lookup(equal, begin[j]);
            if (bs.all_zeros()) {
              skip = true;
              break;
            }
            substr &= bs;
          }
          if (!skip)
            r |= substr;
        }
        return std::move(op == ni ? r : r.flip());
      }
    }
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const {
    auto s = get<std::string>(d);
    if (s)
      return lookup_impl(op, *s);
    return error{"not string data: ", d};
  }

  trial<Bitstream> lookup_impl(relational_operator op,
                               std::string const& str) const {
    return lookup_string(op, str.begin(), str.end());
  }

  template <size_t N>
  trial<Bitstream> lookup_impl(relational_operator op,
                               char const(&str)[N]) const {
    return lookup_string(op, str, str + N - 1);
  }

  uint64_t size_impl() const {
    return length_.size();
  }

  std::vector<char_bitmap_type> bitmaps_;
  length_bitmap_type length_;
};

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index
  : public bitmap_index_base<address_bitmap_index<Bitstream>, Bitstream> {
  using super = bitmap_index_base<address_bitmap_index<Bitstream>, Bitstream>;
  friend super;
  friend access;
  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;
  using bitmap_type = bitmap<uint8_t, bitslice_coder<Bitstream>>;

  address_bitmap_index() {
    bitmaps_.fill(bitmap_type{8});
  }

  friend bool operator==(address_bitmap_index const& x,
                         address_bitmap_index const& y) {
    return x.bitmaps_ == y.bitmaps_;
  }

private:
  bool push_back_impl(address const& a) {
    auto& bytes = a.data();
    size_t start = a.is_v4() ? 12 : 0;
    if (!v4_.push_back(start == 12))
      return false;
    for (size_t i = 0; i < 16; ++i)
      // TODO: be lazy and push_back only where needed.
      if (!bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        return false;
    return true;
  }

  bool push_back_impl(data const& d) {
    if (auto a = get<address>(d))
      return push_back_impl(*a);
    else if (auto s = get<subnet>(d))
      return push_back_impl(s->network());
    else
      return false;
  }

  bool stretch_impl(size_t n) {
    for (size_t i = 0; i < 16; ++i)
      if (!bitmaps_[i].stretch(n))
        return false;
    return v4_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const {
    if (!(op == equal || op == not_equal || op == in || op == not_in))
      return error{"unsupported relational operator: ", op};
    if (v4_.empty())
      return Bitstream{};
    switch (which(d)) {
      default:
        return error{"invalid value"};
      case data::tag::address:
        return lookup_impl(op, *get<address>(d));
      case data::tag::subnet:
        return lookup_impl(op, *get<subnet>(d));
    }
  }

  trial<Bitstream> lookup_impl(relational_operator op, address const& a) const {
    if (!(op == equal || op == not_equal))
      return error{"unsupported relational operator: ", op};
    auto& bytes = a.data();
    auto is_v4 = a.is_v4();
    auto result = is_v4 ? v4_ : Bitstream{this->size(), true};
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++i) {
      auto bs = bitmaps_[i].lookup(equal, bytes[i]);
      if (!bs.all_zeros())
        result &= bs;
      else
        return Bitstream{this->size(), op == not_equal};
    }
    return std::move(op == equal ? result : result.flip());
  }

  trial<Bitstream> lookup_impl(relational_operator op, subnet const& s) const {
    if (!(op == in || op == not_in))
      return error{"unsupported relational operator: ", op};
    auto topk = s.length();
    if (topk == 0)
      return error{"invalid IP subnet length: ", topk};
    auto net = s.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      // Asking for /32 or /128 membership is equivalent to equality.
      return lookup_impl(op == in ? equal : not_equal, s.network());
    auto result = is_v4 ? v4_ : Bitstream{this->size(), true};
    auto& bytes = net.data();
    size_t i = is_v4 ? 12 : 0;
    while (i < 16 && topk >= 8) {
      result &= bitmaps_[i].lookup(equal, bytes[i]);
      ++i;
      topk -= 8;
    }
    for (auto j = 0u; j < topk; ++j) {
      auto bit = 7 - j;
      auto& bs = bitmaps_[i].coder()[bit];
      result &= (bytes[i] >> bit) & 1 ? ~bs : bs;
    }
    if (op == not_in)
      result.flip();
    return result;
  }

  uint64_t size_impl() const {
    return v4_.size();
  }

  std::array<bitmap_type, 16> bitmaps_;
  Bitstream v4_;
};

/// A bitmap index for IP prefixes.
template <typename Bitstream>
class subnet_bitmap_index
  : public bitmap_index_base<subnet_bitmap_index<Bitstream>, Bitstream> {
  using super = bitmap_index_base<subnet_bitmap_index<Bitstream>, Bitstream>;
  friend super;
  friend access;
  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  subnet_bitmap_index()
    : length_{128 + 1} // Valid prefixes range from /0 to /128.
  {
  }

  friend bool operator==(subnet_bitmap_index const& x,
                         subnet_bitmap_index const& y) {
    return x.network_ == y.network_ && x.length_ == y.length_;
  }

private:
  bool push_back_impl(subnet const& s) {
    return network_.push_back(s.network()) && length_.push_back(s.length());
  }

  bool push_back_impl(data const& d) {
    auto s = get<subnet>(d);
    return s ? push_back_impl(*s) : false;
  }

  bool stretch_impl(size_t n) {
    return network_.stretch(n) && length_.stretch(n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, subnet const& s) const {
    if (!(op == equal || op == not_equal))
      return error{"unsupported relational operator: ", op};
    auto bs = network_.lookup(equal, s.network());
    if (!bs)
      return bs;
    auto n = length_.lookup(equal, s.length());
    auto r = Bitstream{*bs & n};
    return std::move(op == equal ? r : r.flip());
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const {
    auto s = get<subnet>(d);
    if (s)
      return lookup_impl(op, *s);
    return error{"not subnet data: ", d};
  }

  uint64_t size_impl() const {
    return length_.size();
  }

  address_bitmap_index<Bitstream> network_;
  bitmap<uint8_t, equality_coder<Bitstream>> length_;
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index
  : public bitmap_index_base<port_bitmap_index<Bitstream>, Bitstream> {
  using super = bitmap_index_base<port_bitmap_index<Bitstream>, Bitstream>;
  friend super;
  friend access;
  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  port_bitmap_index()
    : proto_{4} // unknown, tcp, udp, icmp
  {
  }

  friend bool operator==(port_bitmap_index const& x,
                         port_bitmap_index const& y) {
    return x.num_ == y.num_ && x.proto_ == y.proto_;
  }

private:
  bool push_back_impl(port const& p) {
    return num_.push_back(p.number()) && proto_.push_back(p.type());
  }

  bool push_back_impl(data const& d) {
    auto p = get<port>(d);
    return p ? push_back_impl(*p) : false;
  }

  bool stretch_impl(size_t n) {
    return num_.stretch(n) && proto_.stretch(n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, port const& p) const {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};
    if (num_.empty())
      return Bitstream{};
    auto n = num_.lookup(op, p.number());
    if (n.all_zeros())
      return Bitstream{this->size(), false};
    if (p.type() != port::unknown)
      n &= proto_.lookup(equal, p.type());
    return std::move(n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const {
    auto p = get<port>(d);
    if (p)
      return lookup_impl(op, *p);
    return error{"not port data: ", d};
  }

  uint64_t size_impl() const {
    return proto_.size();
  }

  bitmap<
    port::number_type,
    multi_level_coder<
      make_uniform_base<10, port::number_type>,
      range_coder<Bitstream>
    >
  > num_;
  bitmap<
    std::underlying_type<port::port_type>::type,
    equality_coder<Bitstream>
  > proto_;
};

} // namespace vast

#endif
