#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/operator.h"
#include "vast/optional.h"
#include "vast/logger.h"
#include "vast/value.h"
#include "vast/util/operators.h"
#include "vast/util/trial.h"

namespace vast {

/// The base class for bitmap indexes.
template <typename Derived, typename Bitstream>
class bitmap_index_base
  : util::equality_comparable<bitmap_index_base<Derived, Bitstream>>
{
public:
  /// Appends a single value.
  /// @param x The value to append to the index.
  /// @param offset The position of *x* in the bitmap index.
  /// @returns `true` if appending succeeded.
  template <typename T>
  bool push_back(T const& x, uint64_t offset = 0)
  {
    return catch_up(offset)
        && derived()->push_back_impl(x)
        && nil_.push_back(false)
        && mask_.push_back(true);
  }

  bool push_back(none, uint64_t offset = 0)
  {
    return catch_up(offset)
        && stretch(1)
        && nil_.push_back(true)
        && mask_.push_back(true);
  }

  bool push_back(data const& d, uint64_t offset = 0)
  {
    return catch_up(offset)
        && (is<none>(d) ? stretch(1) : derived()->push_back_impl(d))
        && nil_.push_back(is<none>(d))
        && mask_.push_back(true);
  }

  /// Appends 0-bits to the index.
  /// @param n The number of zeros to append.
  /// @returns `true` on success.
  bool stretch(size_t n)
  {
    return derived()->stretch_impl(n);
  }

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param x The value to lookup.
  template <typename T>
  trial<Bitstream> lookup(relational_operator op, T const& x) const
  {
    auto r = derived()->lookup_impl(op, x);
    if (r)
      *r &= mask_;

    return r;
  }

  trial<Bitstream> lookup(relational_operator op, none) const
  {
    if (! (op == equal || op == not_equal))
      return error{"unsupported relational operator: ", op};

    return op == equal ? nil_ & mask_ : ~nil_ & mask_;
  }

  /// Retrieves the number of elements in the bitmap index.
  /// @returns The number of rows, i.e., values in the bitmap.
  uint64_t size() const
  {
    return derived()->size_impl();
  }

  /// Checks whether the bitmap is empty.
  /// @returns `true` if `size() == 0`.
  bool empty() const
  {
    return size() == 0;
  }

  /// Appends invalid bits to bring the bitmap index up to a given size. Given
  /// an ID of *n*, the function stretches the index up to size *n* with
  /// invalid bits. Afterwards it appends to the mask a single true bit
  /// for the next value to push back.
  /// @param n The ID to catch up to.
  /// @returns `true` on success.
  bool catch_up(uint64_t n)
  {
    if (n == 0)
      return true;

    if (n < size())
    {
      VAST_ERROR("lower n than required:", n, '<', size());
      return false;
    }

    auto delta = n - size();
    if (delta == 0)
      return true;

    return stretch(delta)
        && nil_.append(delta, false)
        && mask_.append(delta, false);
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << mask_ << nil_;
  }

  void deserialize(deserializer& source)
  {
    source >> mask_ >> nil_;
  }

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }

  Bitstream mask_;
  Bitstream nil_;
};

namespace detail {

/// The concept for bitmap indexes.
template <typename Bitstream>
struct bitmap_index_concept
{
  bitmap_index_concept() = default;
  virtual ~bitmap_index_concept() = default;

  virtual bool push_back(data const& d, uint64_t offset) = 0;
  virtual bool stretch(size_t n) = 0;
  virtual trial<Bitstream> lookup(relational_operator op,
                                  data const& d) const = 0;
  virtual uint64_t size() const = 0;

  virtual std::unique_ptr<bitmap_index_concept> copy() const = 0;
  virtual bool equals(bitmap_index_concept const& other) const = 0;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
};

/// A concrete bitmap index model.
template <typename BitmapIndex>
struct bitmap_index_model
  : bitmap_index_concept<typename BitmapIndex::bitstream_type>,
    util::equality_comparable<bitmap_index_model<BitmapIndex>>
{
  using bitstream_type = typename BitmapIndex::bitstream_type;
  using bmi_concept = bitmap_index_concept<bitstream_type>;

  bitmap_index_model() = default;

  bitmap_index_model(BitmapIndex bmi)
    : bmi_{std::move(bmi)}
  {
  }

  virtual bool push_back(data const& d, uint64_t offset) final
  {
    return bmi_.push_back(d, offset);
  }

  virtual bool stretch(size_t n) final
  {
    return bmi_.stretch(n);
  }

  virtual trial<bitstream_type>
  lookup(relational_operator op, data const& d) const final
  {
    return bmi_.lookup(op, d);
  }

  virtual uint64_t size() const final
  {
    return bmi_.size();
  }

  BitmapIndex const& cast(bmi_concept const& c) const
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model const&>(c).bmi_;
  }

  BitmapIndex& cast(bmi_concept& c)
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model&>(c).bmi_;
  }

  friend bool operator==(bitmap_index_model const& x, bitmap_index_model const& y)
  {
    return x.bmi_ == y.bmi_;
  }

  virtual std::unique_ptr<bmi_concept> copy() const final
  {
    return std::make_unique<bitmap_index_model>(*this);
  }

  virtual bool equals(bmi_concept const& other) const final
  {
    return bmi_ == cast(other);
  }

  virtual void serialize(serializer& sink) const final
  {
    sink << bmi_;
  }

  virtual void deserialize(deserializer& source) final
  {
    source >> bmi_;
  }

  BitmapIndex bmi_;
};

} // namespace detail

/// A polymorphic bitmap index with value semantics.
template <typename Bitstream>
class bitmap_index : util::equality_comparable<bitmap_index<Bitstream>>
{
public:
  bitmap_index() = default;

  bitmap_index(bitmap_index const& other)
    : concept_{other.concept_ ? other.concept_->copy() : nullptr}
  {
  }

  bitmap_index(bitmap_index&& other)
    : concept_{std::move(other.concept_)}
  {
  }

  template <
    typename BitmapIndex,
    typename = util::disable_if_same_or_derived_t<bitmap_index, BitmapIndex>
  >
  bitmap_index(BitmapIndex&& bmi)
    : concept_{
        new detail::bitmap_index_model<std::decay_t<BitmapIndex>>{
            std::forward<BitmapIndex>(bmi)}}
  {
  }

  bitmap_index& operator=(bitmap_index const& other)
  {
    concept_ = other.concept_ ? other.concept_->copy() : nullptr;
    return *this;
  }

  bitmap_index& operator=(bitmap_index&& other)
  {
    concept_ = std::move(other.concept_);
    return *this;
  }

  explicit operator bool() const
  {
    return concept_ != nullptr;
  }

  bool push_back(data const& d, uint64_t offset = 0)
  {
    assert(concept_);
    return concept_->push_back(d, offset);
  }

  bool stretch(size_t n)
  {
    assert(concept_);
    return concept_->stretch(n);
  }

  trial<Bitstream> lookup(relational_operator op, data const& d) const
  {
    assert(concept_);
    return concept_->lookup(op, d);
  }

  uint64_t size() const
  {
    assert(concept_);
    return concept_->size();
  }

  uint64_t empty() const
  {
    assert(concept_);
    return concept_->empty();
  }

  bool catch_up(uint64_t n)
  {
    assert(concept_);
    return concept_->catchup(n);
  }

private:
  std::unique_ptr<detail::bitmap_index_concept<Bitstream>> concept_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    if (concept_)
      sink << true << concept_;
    else
      sink << false;
  }

  void deserialize(deserializer& source)
  {
    bool valid;
    source >> valid;
    if (valid)
      source >> concept_;
  }

  friend bool operator==(bitmap_index const& x, bitmap_index const& y)
  {
    assert(x.concept_);
    assert(y.concept_);
    return x.concept_->equals(*y.concept_);
  }
};

/// A bitmap index for arithmetic values.
template <typename Bitstream, typename T>
class arithmetic_bitmap_index
  : public bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>, Bitstream>,
    util::equality_comparable<arithmetic_bitmap_index<Bitstream, T>>
{
  using super =
    bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>, Bitstream>;

  friend super;

  template <typename>
  friend struct detail::bitmap_index_model;

  using bitmap_value_type =
    std::conditional_t<
      std::is_same<T, time_point>::value
      || std::is_same<T, time_duration>::value,
      time_duration::rep,
      std::conditional_t<
        std::is_same<T, boolean>::value
        || std::is_same<T, integer>::value
        || std::is_same<T, count>::value
        || std::is_same<T, real>::value,
        T,
        std::false_type
      >
    >;

  template <typename U>
  using bitmap_binner =
    std::conditional_t<
      std::is_same<T, real>::value
      || std::is_same<T, time_point>::value
      || std::is_same<T, time_duration>::value,
      precision_binner<U>,
      null_binner<U>
    >;

  template <typename B, typename U>
  using bitmap_coder =
    std::conditional_t<
      std::is_same<T, boolean>::value,
      equality_coder<B, U>,
      std::conditional_t<
        std::is_arithmetic<bitmap_value_type>::value,
        range_bitslice_coder<B, U>,
        std::false_type
      >
    >;

  using bitmap_type =
    bitmap<bitmap_value_type, Bitstream, bitmap_coder, bitmap_binner>;

public:
  using bitstream_type = Bitstream;

  arithmetic_bitmap_index() = default;

  template <typename... Ts>
  void binner(Ts&&... xs)
  {
    bitmap_.binner(std::forward<Ts>(xs)...);
  }

private:
  struct pusher
  {
    pusher(bitmap_type& bm)
      : bm_{bm}
    {
    }

    template <typename U>
    bool operator()(U) const
    {
      return false;
    }

    bool operator()(bitmap_value_type x) const
    {
      return bm_.push_back(x);
    }

    bool operator()(time_point x) const
    {
      return (*this)(x.since_epoch().count());
    }

    bool operator()(time_duration x) const
    {
      return (*this)(x.count());
    }

    bitmap_type& bm_;
  };

  struct looker
  {
    looker(bitmap_type const& bm, relational_operator op)
      : bm_{bm},
        op_{op}
    {
    }

    template <typename U>
    trial<Bitstream> operator()(U const& x) const
    {
      return error{"invalid type: ", x};
    }

    trial<Bitstream> operator()(bitmap_value_type x) const
    {
      return bm_.lookup(op_, x);
    }

    trial<Bitstream> operator()(time_point x) const
    {
      return (*this)(x.since_epoch().count());
    }

    trial<Bitstream> operator()(time_duration x) const
    {
      return (*this)(x.count());
    }

    bitmap_type const& bm_;
    relational_operator op_;
  };

  bool push_back_impl(data const& d)
  {
    return visit(pusher{bitmap_}, d);
  }

  bool push_back_impl(T x)
  {
    return pusher{bitmap_}(x);
  }

  bool stretch_impl(size_t n)
  {
    return bitmap_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};

    return visit(looker{bitmap_, op}, d);
  };

  trial<Bitstream> lookup_impl(relational_operator op, T x) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};

    return looker{bitmap_, op}(x);
  };

  uint64_t size_impl() const
  {
    return bitmap_.size();
  }

  bitmap_type bitmap_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << bitmap_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> bitmap_;
  }

  friend bool operator==(arithmetic_bitmap_index const& x,
                         arithmetic_bitmap_index const& y)
  {
    return x.bitmap_ == y.bitmap_;
  }
};

/// A bitmap index for strings.
template <typename Bitstream>
class string_bitmap_index
  : public bitmap_index_base<string_bitmap_index<Bitstream>, Bitstream>
{
  using super = bitmap_index_base<string_bitmap_index<Bitstream>, Bitstream>;
  friend super;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  string_bitmap_index() = default;

private:
  template <typename Iterator>
  bool push_back_string(Iterator begin, Iterator end)
  {
    auto size = static_cast<size_t>(end - begin);
    if (size > bitmaps_.size())
      bitmaps_.resize(size);

    for (size_t i = 0; i < size; ++i)
    {
      assert(this->size() >= bitmaps_[i].size());
      auto delta = this->size() - bitmaps_[i].size();
      if (delta > 0)
        if (! bitmaps_[i].append(delta, false))
          return false;

      if (! bitmaps_[i].push_back(static_cast<uint8_t>(begin[i])))
        return false;
    }

    return size_.push_back(size);
  }

  bool push_back_impl(data const& d)
  {
    auto str = get<std::string>(d);
    return str && push_back_impl(*str);
  }

  bool push_back_impl(std::string const& str)
  {
    return push_back_string(str.begin(), str.end());
  }

  template <size_t N>
  bool push_back_impl(char const (&str)[N])
  {
    return push_back_string(str, str + N - 1);
  }

  bool stretch_impl(size_t n)
  {
    return size_.append(n, false);
  }

  template <typename Iterator>
  trial<Bitstream> lookup_string(relational_operator op,
                                 Iterator begin, Iterator end) const
  {
    auto size = static_cast<size_t>(end - begin);

    switch (op)
    {
      default:
        return error{"unsupported relational operator: ", op};
      case equal:
      case not_equal:
        {
          if (size == 0)
          {
            if (auto s = size_.lookup(equal, 0))
              return std::move(op == equal ? *s : s->flip());
            else
              return s.error();
          }

          if (size > bitmaps_.size())
            return Bitstream{this->size(), op == not_equal};

          auto r = size_.lookup(less_equal, size);
          if (! r)
            return r.error();

          if (r->all_zero())
            return Bitstream{this->size(), op == not_equal};

          for (size_t i = 0; i < size; ++i)
          {
            auto b = bitmaps_[i].lookup(equal, static_cast<uint8_t>(begin[i]));
            if (! b)
              return b.error();

            if (! b->all_zero())
              *r &= *b;
            else
              return Bitstream{this->size(), op == not_equal};
          }

          return std::move(op == equal ? *r : r->flip());
        }
      case ni:
      case not_ni:
        {
          if (size == 0)
            return Bitstream{this->size(), op == ni};

          if (size > bitmaps_.size())
            return Bitstream{this->size(), op == not_ni};

          // TODO: Be more clever than iterating over all k-grams (#45).
          Bitstream r{this->size(), 0};
          for (size_t i = 0; i < bitmaps_.size() - size + 1; ++i)
          {
            Bitstream substr{this->size(), 1};;
            auto skip = false;
            for (size_t j = 0; j < size; ++j)
            {
              auto bs = bitmaps_[i + j].lookup(equal, begin[j]);
              if (! bs)
                return bs.error();

              if (bs->all_zero())
              {
                skip = true;
                break;
              }

              substr &= *bs;
            }

            if (! skip)
              r |= substr;
          }

          return std::move(op == ni ? r : r.flip());
        }
    }
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    auto s = get<std::string>(d);
    if (s)
      return lookup_impl(op, *s);

    return error{"not string data: ", d};
  }

  trial<Bitstream> lookup_impl(relational_operator op,
                               std::string const& str) const
  {
    return lookup_string(op, str.begin(), str.end());
  }

  template <size_t N>
  trial<Bitstream> lookup_impl(relational_operator op,
                               char const (&str)[N]) const
  {
    return lookup_string(op, str, str + N - 1);
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  std::vector<bitmap<uint8_t, Bitstream, binary_bitslice_coder>> bitmaps_;
  bitmap<std::string::size_type, Bitstream, range_bitslice_coder> size_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << size_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> size_ >> bitmaps_;
  }

  friend bool operator==(string_bitmap_index const& x,
                         string_bitmap_index const& y)
  {
    return x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for IP addresses.
template <typename Bitstream>
class address_bitmap_index
  : public bitmap_index_base<address_bitmap_index<Bitstream>, Bitstream>
{
  using super = bitmap_index_base<address_bitmap_index<Bitstream>, Bitstream>;
  friend super;

  template <typename>
    friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  address_bitmap_index() = default;

private:
  bool push_back_impl(address const& a)
  {
    auto& bytes = a.data();
    size_t start = a.is_v4() ? 12 : 0;

    if (! v4_.push_back(start == 12))
      return false;

    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        return false;

    return true;
  }

  bool push_back_impl(data const& d)
  {
    if (auto a = get<address>(d))
      return push_back_impl(*a);
    else if (auto s = get<subnet>(d))
      return push_back_impl(s->network());
    else
      return false;
  }

  bool stretch_impl(size_t n)
  {
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].append(n, false))
        return false;

    return v4_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      return error{"unsupported relational operator: ", op};

    if (v4_.empty())
      return Bitstream{};

    switch (which(d))
    {
      default:
        return error{"invalid value"};
      case data::tag::address:
        return lookup_impl(op, *get<address>(d));
      case data::tag::subnet:
        return lookup_impl(op, *get<subnet>(d));
    }
  }

  trial<Bitstream> lookup_impl(relational_operator op, address const& a) const
  {
    if (! (op == equal || op == not_equal))
      return error{"unsupported relational operator: ", op};

    auto& bytes = a.data();
    auto is_v4 = a.is_v4();
    auto r = is_v4 ? v4_ : Bitstream{this->size(), true};

    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    {
      auto bs = bitmaps_[i][bytes[i]];
      if (! bs)
        return bs.error();

      if (! bs->all_zero())
        r &= *bs;
      else
        return Bitstream{this->size(), op == not_equal};
    }

    return std::move(op == equal ? r : r.flip());
  }

  trial<Bitstream> lookup_impl(relational_operator op, subnet const& s) const
  {
    if (! (op == in || op == not_in))
      return error{"unsupported relational operator: ", op};

    auto topk = s.length();
    if (topk == 0)
      return error{"invalid IP subnet length: ", topk};

    auto net = s.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup_impl(op == in ? equal : not_equal, s.network());

    auto r = is_v4 ? v4_ : Bitstream{this->size(), true};
    auto bit = topk;
    auto& bytes = net.data();
    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
      for (size_t j = 8; j --> 0; )
      {
        auto& bs = bitmaps_[i].coder().get(j);
        r &= ((bytes[i] >> j) & 1) ? bs : ~bs;

        if (! --bit)
        {
          if (op == not_in)
            r.flip();
          return std::move(r);
        }
      }

    return Bitstream{this->size(), false};
  }

  uint64_t size_impl() const
  {
    return v4_.size();
  }

  std::array<bitmap<uint8_t, Bitstream, binary_bitslice_coder>, 16> bitmaps_;
  Bitstream v4_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << v4_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> v4_ >> bitmaps_;
  }

  friend bool operator==(address_bitmap_index const& x,
                         address_bitmap_index const& y)
  {
    return x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for IP prefixes.
template <typename Bitstream>
class subnet_bitmap_index
  : public bitmap_index_base<subnet_bitmap_index<Bitstream>, Bitstream>
{
  using super = bitmap_index_base<subnet_bitmap_index<Bitstream>, Bitstream>;
  friend super;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  subnet_bitmap_index() = default;

private:
  bool push_back_impl(subnet const& s)
  {
    return network_.push_back(s.network()) && length_.push_back(s.length());
  }

  bool push_back_impl(data const& d)
  {
    auto s = get<subnet>(d);
    return s ? push_back_impl(*s) : false;
  }

  bool stretch_impl(size_t n)
  {
    return network_.stretch(n) && length_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, subnet const& s) const
  {
    if (! (op == equal || op == not_equal))
      return error{"unsupported relational operator: ", op};

    auto bs = network_.lookup(equal, s.network());
    if (! bs)
      return bs;

    auto n = length_.lookup(equal, s.length());
    if (! n)
      return n;

    auto r = Bitstream{*bs & *n};
    return std::move(op == equal ? r : r.flip());
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    auto s = get<subnet>(d);
    if (s)
      return lookup_impl(op, *s);

    return error{"not subnet data: ", d};
  }

  uint64_t size_impl() const
  {
    return length_.size();
  }

  address_bitmap_index<Bitstream> network_;
  bitmap<uint8_t, Bitstream, range_bitslice_coder> length_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << network_ << length_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> network_ >> length_;
  }

  friend bool operator==(subnet_bitmap_index const& x,
                         subnet_bitmap_index const& y)
  {
    return x.network_ == y.network_ && x.length_ == y.length_;
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index
  : public bitmap_index_base<port_bitmap_index<Bitstream>, Bitstream>
{
  using super = bitmap_index_base<port_bitmap_index<Bitstream>, Bitstream>;
  friend super;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  port_bitmap_index() = default;

private:
  bool push_back_impl(port const& p)
  {
    return num_.push_back(p.number()) && proto_.push_back(p.type());
  }

  bool push_back_impl(data const& d)
  {
    auto p = get<port>(d);
    return p ? push_back_impl(*p) : false;
  }

  bool stretch_impl(size_t n)
  {
    return num_.append(n, false) && proto_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, port const& p) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: ", op};

    if (num_.empty())
      return Bitstream{};

    auto n = num_.lookup(op, p.number());
    if (! n)
      return n.error();

    if (n->all_zero())
      return Bitstream{this->size(), false};

    if (p.type() != port::unknown)
    {
      auto t = proto_[p.type()];
      if (! t)
        return t.error();

      *n &= *t;
    }

    return std::move(*n);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    auto p = get<port>(d);
    if (p)
      return lookup_impl(op, *p);

    return error{"not port data: ", d};
  }

  uint64_t size_impl() const
  {
    return proto_.size();
  }

  bitmap<port::number_type, Bitstream, range_bitslice_coder> num_;
  bitmap<std::underlying_type<port::port_type>::type, Bitstream> proto_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << num_ << proto_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> num_ >> proto_;
  }

  friend bool operator==(port_bitmap_index const& x, port_bitmap_index const& y)
  {
    return x.num_ == y.num_ && x.proto_ == y.proto_;
  }
};

template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(type_tag t, Args&&... args);

/// A bitmap index for sets, vectors, and tuples.
template <typename Bitstream>
class sequence_bitmap_index
  : public bitmap_index_base<sequence_bitmap_index<Bitstream>, Bitstream>
{
  using super = bitmap_index_base<sequence_bitmap_index<Bitstream>, Bitstream>;
  friend super;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  sequence_bitmap_index() = default;

  sequence_bitmap_index(type t)
    : elem_type_{std::move(t)}
  {
  }

private:
  bool push_back_impl(data const& d)
  {
    switch (which(d))
    {
      default:
        return false;
      case data::tag::vector:
        return push_back_impl(*get<vector>(d));
      case data::tag::set:
        return push_back_impl(*get<set>(d));
    }
  }

  template <typename Container>
  bool push_back_impl(Container const& c)
  {
    if (c.empty())
      return size_.append(1, false);

    if (bmis_.size() < c.size())
    {
      auto old = bmis_.size();
      bmis_.resize(c.size());
      for (auto i = old; i < bmis_.size(); ++i)
      {
        auto bmi = make_bitmap_index<Bitstream>(elem_type_);
        if (! bmi)
          return false;

        bmis_[i] = std::move(*bmi);
      }
    }

    for (size_t i = 0; i < c.size(); ++i)
      if (! bmis_[i].push_back(c[i], this->size()))
        return false;

    return size_.push_back(c.size());
  }

  bool stretch_impl(size_t n)
  {
    return size_.append(n, false);
  }

  trial<Bitstream> lookup_impl(relational_operator op, data const& d) const
  {
    if (op == ni)
      op = in;
    else if (op == not_ni)
      op = not_in;

    if (! (op == in || op == not_in))
      return error{"unsupported relational operator: ", op};

    if (this->empty())
      return Bitstream{};

    Bitstream r;
    auto t = bmis_.front().lookup(equal, d);
    if (t)
      r |= *t;
    else
      return t;

    for (size_t i = 1; i < bmis_.size(); ++i)
    {
      auto bs = bmis_[i].lookup(equal, d);
      if (bs)
        r |= *bs;
      else
        return bs;
    }

    if (r.size() < this->size())
      r.append(this->size() - r.size(), false);

    if (op == not_in)
      r.flip();

    return std::move(r);
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  type elem_type_;
  std::vector<bitmap_index<Bitstream>> bmis_;
  bitmap<uint32_t, Bitstream, range_bitslice_coder> size_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << static_cast<super const&>(*this) << elem_type_ << bmis_ << size_;
  }

  void deserialize(deserializer& source)
  {
    source >> static_cast<super&>(*this) >> elem_type_ >> bmis_ >> size_;
  }

  friend bool
  operator==(sequence_bitmap_index const& x, sequence_bitmap_index const& y)
  {
    return x.elem_type_ == y.elem_type_
        && x.bmis_ == y.bmis_
        && x.size_ == y.size_;
  }
};

/// Factory to construct a bitmap index based on a given type tag.
template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(type const& t, Args&&... args)
{
  // Can't use a visitor because of argument forwarding.
  switch (which(t))
  {
    default:
      return error{"unspported type: ", t};
    case type::tag::boolean:
      return {arithmetic_bitmap_index<Bitstream, boolean>(std::forward<Args>(args)...)};
    case type::tag::integer:
      return {arithmetic_bitmap_index<Bitstream, integer>(std::forward<Args>(args)...)};
    case type::tag::count:
      return {arithmetic_bitmap_index<Bitstream, count>(std::forward<Args>(args)...)};
    case type::tag::real:
      return {arithmetic_bitmap_index<Bitstream, real>(std::forward<Args>(args)...)};
    case type::tag::time_point:
      return {arithmetic_bitmap_index<Bitstream, time_point>(std::forward<Args>(args)...)};
    case type::tag::time_duration:
      return {arithmetic_bitmap_index<Bitstream, time_duration>(std::forward<Args>(args)...)};
    case type::tag::string:
      return {string_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case type::tag::address:
      return {address_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case type::tag::subnet:
      return {subnet_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case type::tag::port:
      return {port_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
  }
}

} // namespace vast

#endif
