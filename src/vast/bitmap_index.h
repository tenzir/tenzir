#ifndef VAST_BITMAP_INDEX_H
#define VAST_BITMAP_INDEX_H

#include "vast/bitmap.h"
#include "vast/container.h"
#include "vast/operator.h"
#include "vast/logger.h"
#include "vast/value.h"
#include "vast/util/dictionary.h"
#include "vast/util/operators.h"
#include "vast/util/trial.h"

namespace vast {

/// The base class for bitmap indexes.
template <typename Derived>
class bitmap_index_base : util::equality_comparable<bitmap_index_base<Derived>>
{
public:
  /// Appends a single value.
  /// @param val The value to add to the index.
  /// @param id The ID to associate with *val*.
  /// @returns `true` if appending succeeded.
  bool push_back(value const& val, event_id id = 0)
  {
    if (id > 0)
    {
      if (id < size())
      {
        VAST_LOG_ERROR("got value " << val << " incompatible ID " << id <<
                       " (required: ID > " << size() << ')');
        return false;
      }

      auto delta = id - size();
      if (delta > 1)
        if (! append(delta, false))
          return false;
    }

    return val ? derived()->push_back_impl(val) : append(1, false);
  }

  /// Appends a sequence of bits.
  /// @param n The number of elements to append.
  /// @param bit The value of the bits to append.
  /// @returns `true` on success.
  bool append(size_t n, bool bit)
  {
    return derived()->append_impl(n, bit);
  }

  /// Looks up a value given a relational operator.
  /// @param op The relation operator.
  /// @param val The value to lookup.
  trial<bitstream> lookup(relational_operator op, value const& val) const
  {
    return derived()->lookup_impl(op, val);
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

private:
  friend access;

  Derived* derived()
  {
    return static_cast<Derived*>(this);
  }

  Derived const* derived() const
  {
    return static_cast<Derived const*>(this);
  }
};

namespace detail {

/// The concept for bitmap indexes.
struct bitmap_index_concept
{
  bitmap_index_concept() = default;
  virtual ~bitmap_index_concept() = default;

  virtual bool push_back_impl(value const& val) = 0;
  virtual bool append_impl(size_t n, bool bit) = 0;
  virtual trial<bitstream> lookup_impl(relational_operator op,
                                       value const& val) const = 0;
  virtual uint64_t size_impl() const = 0;

  virtual std::unique_ptr<bitmap_index_concept> copy() const = 0;
  virtual bool equals(bitmap_index_concept const& other) const = 0;
  virtual void serialize(serializer& sink) const = 0;
  virtual void deserialize(deserializer& source) = 0;
};

/// A concrete bitmap index model.
template <typename BitmapIndex>
struct bitmap_index_model
  : bitmap_index_concept,
    util::equality_comparable<bitmap_index_model<BitmapIndex>>
{
  bitmap_index_model() = default;

  bitmap_index_model(BitmapIndex bmi)
    : bmi_{std::move(bmi)}
  {
  }

  virtual bool push_back_impl(value const& val) final
  {
    return bmi_.push_back_impl(val);
  }

  virtual bool append_impl(size_t n, bool bit) final
  {
    return bmi_.append_impl(n, bit);
  }

  virtual trial<bitstream> lookup_impl(relational_operator op,
                                       value const& val) const final
  {
    return bmi_.lookup_impl(op, val);
  }

  virtual uint64_t size_impl() const final
  {
    return bmi_.size_impl();
  }

  BitmapIndex const& cast(bitmap_index_concept const& c) const
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model const&>(c).bmi_;
  }

  BitmapIndex& cast(bitmap_index_concept& c)
  {
    if (typeid(c) != typeid(*this))
      throw std::bad_cast();

    return static_cast<bitmap_index_model&>(c).bmi_;
  }

  friend bool operator==(bitmap_index_model const& x, bitmap_index_model const& y)
  {
    return x.bmi_ == y.bmi_;
  }

  virtual std::unique_ptr<bitmap_index_concept> copy() const final
  {
    return make_unique<bitmap_index_model>(*this);
  }

  virtual bool equals(bitmap_index_concept const& other) const final
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
class bitmap_index : public bitmap_index_base<bitmap_index<Bitstream>>,
                     util::equality_comparable<bitmap_index<Bitstream>>
{
  using super = bitmap_index_base<bitmap_index<Bitstream>>;
  friend super;

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
    typename = DisableIfSameOrDerived<bitmap_index, BitmapIndex>
  >
  bitmap_index(BitmapIndex&& bmi)
    : concept_{
        new detail::bitmap_index_model<Unqualified<BitmapIndex>>{
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

private:
  bool push_back_impl(value const& val)
  {
    assert(concept_);
    return concept_->push_back_impl(val);
  }

  bool append_impl(size_t n, bool bit)
  {
    assert(concept_);
    return concept_->append_impl(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(concept_);
    return concept_->lookup_impl(op, val);
  }

  uint64_t size_impl() const
  {
    assert(concept_);
    return concept_->size_impl();
  }

private:
  std::unique_ptr<detail::bitmap_index_concept> concept_;

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

/// A bitmap index for arithmetic value types.
template <typename Bitstream, value_type T>
class arithmetic_bitmap_index
  : public bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>>,
    util::equality_comparable<arithmetic_bitmap_index<Bitstream, T>>
{
  friend bitmap_index_base<arithmetic_bitmap_index<Bitstream, T>>;

  template <typename>
  friend struct detail::bitmap_index_model;

  using underlying_value_type = underlying_value_type<T>;

  using bitmap_type =
    typename std::conditional<
      T == time_range_value || T == time_point_value,
      time_range::rep,
      typename std::conditional<
        T == bool_value || T == int_value || T == uint_value || T == double_value,
        underlying_value_type,
        std::false_type
      >::type
    >::type;

  template <typename U>
  using bitmap_binner =
    typename std::conditional<
      T == double_value || T == time_range_value || T == time_point_value,
      precision_binner<U>,
      null_binner<U>
    >::type;

  template <typename B, typename U>
  using bitmap_coder =
    typename std::conditional<
      T == bool_value,
      equality_coder<B, U>,
      typename std::conditional<
        std::is_arithmetic<bitmap_type>::value,
        range_bitslice_coder<B, U>,
        std::false_type
      >::type
    >::type;

public:
  arithmetic_bitmap_index() = default;

  template <
    typename U = bitmap_type,
    typename... Args,
    typename = EnableIf<
      std::is_same<bitmap_binner<U>, precision_binner<U>>>
  >
  explicit arithmetic_bitmap_index(Args&&... args)
    : bitmap_{{std::forward<Args>(args)...}}
  {
  }

private:
  static bitmap_type extract(value const& val)
  {
    switch (val.which())
    {
      default:
        return val.get<bitmap_type>();
      case time_range_value:
        return val.get<time_range>().count();
      case time_point_value:
        return val.get<time_point>().since_epoch().count();
    }
  }

  bool push_back_impl(value const& val)
  {
    return bitmap_.push_back(extract(val));
  }

  bool append_impl(size_t n, bool bit)
  {
    return bitmap_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (op == in || op == not_in)
      return error{"unsupported relational operator: " + to<std::string>(op)};

    if (bitmap_.empty())
      return {Bitstream{}};

    auto r = bitmap_.lookup(op, extract(val));
    if (r)
      return {std::move(*r)};
    else
      return r.failure();
  };

  uint64_t size_impl() const
  {
    return bitmap_.size();
  }

  bitmap<bitmap_type, Bitstream, bitmap_coder, bitmap_binner> bitmap_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << bitmap_;
  }

  void deserialize(deserializer& source)
  {
    source >> bitmap_;
  }

  friend bool operator==(arithmetic_bitmap_index const& x,
                         arithmetic_bitmap_index const& y)
  {
    return x.bitmap_ == y.bitmap_;
  }
};

/// A bitmap index for strings. It uses a @link dictionary
/// vast::util::dictionary@endlink to map each string to a unique numeric value
/// to be used by the bitmap.
template <typename Bitstream>
class string_bitmap_index
  : public bitmap_index_base<string_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<string_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  string_bitmap_index() = default;

private:
  static uint8_t byte_at(string const& s, size_t i)
  {
    return static_cast<uint8_t>(s[i]);
  }

  bool push_back_impl(value const& val)
  {
    auto& str = val.get<string>();

    if (! size_.push_back(str.size()))
      return false;

    if (str.empty())
    {
      for (auto& bm : bitmaps_)
        if (! bm.append(1, 0))
          return false;

      return true;
    }

    if (str.size() > bitmaps_.size())
    {
      auto current = this->size() - 1;
      auto fresh = str.size() - bitmaps_.size();
      bitmaps_.resize(str.size());
      if (current > 0)
        for (size_t i = bitmaps_.size() - fresh; i < bitmaps_.size(); ++i)
          if (! bitmaps_[i].append(current, 0))
            return false;
    }

    for (size_t i = 0; i < str.size(); ++i)
      if (! bitmaps_[i].push_back(byte_at(str, i)))
        return false;

    for (size_t i = str.size(); i < bitmaps_.size(); ++i)
      if (! bitmaps_[i].append(1, 0))
        return false;

    return true;
  }

  bool append_impl(size_t n, bool bit)
  {
    for (auto& bm : bitmaps_)
      if (! bm.append(n, bit))
        return false;

    return size_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == string_value);

    auto str = val.get<string>();

    switch (op)
    {
      default:
        return error{"unsupported relational operator " + to<std::string>(op)};
      case equal:
      case not_equal:
        {
          if (str.empty())
          {
            if (auto s = size_.lookup(equal, 0))
              return {std::move(op == equal ? *s : s->flip())};
            else
              return s.failure();
          }

          if (str.size() > bitmaps_.size())
            return {Bitstream{this->size(), op == not_equal}};

          auto r = size_.lookup(less_equal, str.size());
          if (! r)
            return r.failure();

          if (r->find_first() == Bitstream::npos)
            return {Bitstream{this->size(), op == not_equal}};

          for (size_t i = 0; i < str.size(); ++i)
          {
            auto b = bitmaps_[i].lookup(equal, byte_at(str, i));
            if (! b)
              return b.failure();

            if (b->find_first() != Bitstream::npos)
              *r &= *b;
            else
              return {Bitstream{this->size(), op == not_equal}};
          }

          return {std::move(op == equal ? *r : r->flip())};
        }
      case ni:
      case not_ni:
        {
          if (str.empty())
            return {Bitstream{this->size(), op == ni}};

          if (str.size() > bitmaps_.size())
            return {Bitstream{this->size(), op == not_ni}};

          // Iterate through all k-grams.
          Bitstream r{this->size(), 0};
          for (size_t i = 0; i < bitmaps_.size() - str.size() + 1; ++i)
          {
            Bitstream substr{this->size(), 1};;
            auto skip = false;
            for (size_t j = 0; j < str.size(); ++j)
            {
              auto bs = bitmaps_[i + j].lookup(equal, str[j]);
              if (! bs)
                return bs.failure();

              if (bs->find_first() == Bitstream::npos)
              {
                skip = true;
                break;
              }

              substr &= *bs;
            }

            if (! skip)
              r |= substr;
          }

          return {std::move(op == ni ? r : r.flip())};
        }
    }
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  std::vector<bitmap<uint8_t, Bitstream, binary_bitslice_coder>> bitmaps_;
  bitmap<string::size_type, Bitstream, range_bitslice_coder> size_;

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << size_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> size_ >> bitmaps_;
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
  : public bitmap_index_base<address_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<address_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  address_bitmap_index() = default;

private:
  bool push_back_impl(value const& val)
  {
    auto& addr = val.get<address>();
    auto& bytes = addr.data();
    size_t const start = addr.is_v4() ? 12 : 0;

    if (! v4_.push_back(start == 12))
      return false;

    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].push_back(i < start ? 0x00 : bytes[i]))
        return false;

    return true;
  }

  bool append_impl(size_t n, bool bit)
  {
    bool success = true;
    for (size_t i = 0; i < 16; ++i)
      if (! bitmaps_[i].append(n, bit))
        success = false;
    return v4_.append(n, bit) && success;
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (! (op == equal || op == not_equal || op == in || op == not_in))
      return error{"unsupported relational operator " + to<std::string>(op)};

    if (v4_.empty())
      return {Bitstream{}};

    switch (val.which())
    {
      default:
        return error{"invalid value type"};
      case address_value:
        return lookup_impl(op, val.get<address>());
      case prefix_value:
        return lookup_impl(op, val.get<prefix>());
    }
  }

  trial<bitstream> lookup_impl(relational_operator op, address const& addr) const
  {
    auto& bytes = addr.data();
    auto is_v4 = addr.is_v4();
    auto r = is_v4 ? v4_ : Bitstream{this->size(), true};

    for (size_t i = is_v4 ? 12 : 0; i < 16; ++ i)
    {
      auto bs = bitmaps_[i][bytes[i]];
      if (! bs)
        return bs.failure();

      if (bs->find_first() != Bitstream::npos)
        r &= *bs;
      else
        return {Bitstream{this->size(), op == not_equal}};
    }

    return {std::move(op == equal ? r : r.flip())};
  }

  trial<bitstream> lookup_impl(relational_operator op, prefix const& pfx) const
  {
    if (! (op == in || op == not_in))
      return error{"unsupported relational operator " + to<std::string>(op)};

    auto topk = pfx.length();
    if (topk == 0)
      return error{"invalid IP prefix length: " + to<std::string>(topk)};

    auto net = pfx.network();
    auto is_v4 = net.is_v4();
    if ((is_v4 ? topk + 96 : topk) == 128)
      return lookup_impl(op == in ? equal : not_equal, pfx.network());

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
          return {std::move(r)};
        }
      }

    return {Bitstream{this->size(), false}};
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
    sink << v4_ << bitmaps_;
  }

  void deserialize(deserializer& source)
  {
    source >> v4_ >> bitmaps_;
  }

  friend bool operator==(address_bitmap_index const& x,
                         address_bitmap_index const& y)
  {
    return x.bitmaps_ == y.bitmaps_;
  }
};

/// A bitmap index for transport-layer ports.
template <typename Bitstream>
class port_bitmap_index
  : public bitmap_index_base<port_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<port_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  port_bitmap_index() = default;

private:
  bool push_back_impl(value const& val)
  {
    auto& p = val.get<port>();
    return num_.push_back(p.number()) && proto_.push_back(p.type());
  }

  bool append_impl(size_t n, bool bit)
  {
    return num_.append(n, bit) && proto_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    assert(val.which() == port_value);

    if (op == in || op == not_in)
      return error{"unsupported relational operator " + to<std::string>(op)};

    if (num_.empty())
      return {Bitstream{}};

    auto& p = val.get<port>();
    auto n = num_.lookup(op, p.number());
    if (! n)
      return n.failure();

    if (n->find_first() == Bitstream::npos)
      return {Bitstream{this->size(), false}};

    if (p.type() != port::unknown)
    {
      auto t = proto_[p.type()];
      if (! t)
        return t.failure();

      *n &= *t;
    }

    return {std::move(*n)};
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
    sink << num_ << proto_;
  }

  void deserialize(deserializer& source)
  {
    source >> num_ >> proto_;
  }

  friend bool operator==(port_bitmap_index const& x, port_bitmap_index const& y)
  {
    return x.num_ == y.num_ && x.proto_ == y.proto_;
  }
};

template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(value_type t, Args&&... args);

/// A bitmap index for sets, vectors, and tuples.
template <typename Bitstream>
class set_bitmap_index : public bitmap_index_base<set_bitmap_index<Bitstream>>
{
  friend bitmap_index_base<set_bitmap_index<Bitstream>>;

  template <typename>
  friend struct detail::bitmap_index_model;

public:
  set_bitmap_index(value_type t)
    : elem_type_{t}
  {
  }

private:
  bool push_back_impl(value const& val)
  {
    if (val.which() == set_value)
      return push_back_container<set>(val);
    else if (val.which() == vector_value)
      return push_back_container<vector>(val);
    else
      return false;
  }

  bool append_impl(size_t n, bool bit)
  {
    for (auto& bmi : bmis_)
      if (! bmi.append(n, bit))
        return false;

    return size_.append(n, bit);
  }

  trial<bitstream> lookup_impl(relational_operator op, value const& val) const
  {
    if (! (op == in || op == not_in))
      return error{"unsupported relational operator " + to<std::string>(op)};

    if (this->empty())
      return {Bitstream{}};

    bitstream r;
    auto t = bmis_.front().lookup(equal, val);
    if (t)
      r |= *t;
    else
      return t;

    for (size_t i = 1; i < bmis_.size(); ++i)
    {
      auto bs = bmis_[i].lookup(equal, val);
      if (bs)
        r |= *bs;
      else
        return bs;
    }

    if (op == not_in)
      r.flip();

    return r;
  }

  uint64_t size_impl() const
  {
    return size_.size();
  }

  value_type elem_type_;
  std::vector<bitmap_index<Bitstream>> bmis_;
  bitmap<uint32_t, Bitstream, range_bitslice_coder> size_;

private:
  template <typename C>
  bool push_back_container(value const& val)
  {
    auto& c = val.get<C>();

    if (c.empty())
    {
      for (auto& bmi : bmis_)
        if (! bmi.append(1, false))
          return false;

      if (! size_.append(1, false))
        return false;

      return true;
    }

    if (bmis_.size() < c.size())
    {
      auto old = bmis_.size();
      bmis_.resize(c.size());
      for (auto i = old; i < bmis_.size(); ++i)
      {
        auto bmi = make_bitmap_index<Bitstream>(elem_type_);
        if (! bmi)
          return false;

        if (! this->empty() && ! bmi->append(this->size(), false))
          return false;

        bmis_[i] = std::move(*bmi);
      }
    }

    for (size_t i = 0; i < c.size(); ++i)
      if (! bmis_[i].push_back(c[i]))
        return false;

    for (size_t i = c.size(); i < bmis_.size(); ++i)
      if (! bmis_[i].append(1, false))
        return false;

    if (! size_.push_back(c.size()))
      return false;

    return true;
  }

private:
  friend access;

  void serialize(serializer& sink) const
  {
    sink << elem_type_ << bmis_ << size_;
  }

  void deserialize(deserializer& source)
  {
    source >> elem_type_ >> bmis_ >> size_;
  }

  friend bool operator==(set_bitmap_index const& x, set_bitmap_index const& y)
  {
    return x.elem_type_ == y.elem_type_
        && x.bmis_ == y.bmis_
        && x.size_ == y.size_;
  }
};

/// Factory to construct a bitmap index based on a given value type.
template <typename Bitstream, typename... Args>
trial<bitmap_index<Bitstream>> make_bitmap_index(value_type t, Args&&... args)
{
  switch (t)
  {
    default:
      return error{"unspported value type: " + to_string(t)};
    case bool_value:
      return {arithmetic_bitmap_index<Bitstream, bool_value>(std::forward<Args>(args)...)};
    case int_value:
      return {arithmetic_bitmap_index<Bitstream, int_value>(std::forward<Args>(args)...)};
    case uint_value:
      return {arithmetic_bitmap_index<Bitstream, uint_value>(std::forward<Args>(args)...)};
    case double_value:
      return {arithmetic_bitmap_index<Bitstream, double_value>(std::forward<Args>(args)...)};
    case time_range_value:
      return {arithmetic_bitmap_index<Bitstream, time_range_value>(std::forward<Args>(args)...)};
    case time_point_value:
      return {arithmetic_bitmap_index<Bitstream, time_point_value>(std::forward<Args>(args)...)};
    case string_value:
      return {string_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case address_value:
      return {address_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
    case port_value:
      return {port_bitmap_index<Bitstream>(std::forward<Args>(args)...)};
  }
}

} // namespace vast

#endif
