#ifndef VAST_BITMAP_INDEX_POLYMORPHIC_H
#define VAST_BITMAP_INDEX_POLYMORPHIC_H

#include "vast/bitmap_index.h"
#include "vast/util/assert.h"

namespace vast {
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

  friend bool operator==(bitmap_index_model const& x,
                         bitmap_index_model const& y)
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

  BitmapIndex bmi_;
};

} // namespace detail

/// A polymorphic bitmap index with value semantics.
template <typename Bitstream>
class bitmap_index : util::equality_comparable<bitmap_index<Bitstream>>
{
  friend access;

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
    VAST_ASSERT(concept_);
    return concept_->push_back(d, offset);
  }

  bool stretch(size_t n)
  {
    VAST_ASSERT(concept_);
    return concept_->stretch(n);
  }

  trial<Bitstream> lookup(relational_operator op, data const& d) const
  {
    VAST_ASSERT(concept_);
    return concept_->lookup(op, d);
  }

  uint64_t size() const
  {
    VAST_ASSERT(concept_);
    return concept_->size();
  }

  uint64_t empty() const
  {
    VAST_ASSERT(concept_);
    return concept_->empty();
  }

  bool catch_up(uint64_t n)
  {
    VAST_ASSERT(concept_);
    return concept_->catchup(n);
  }

  friend bool operator==(bitmap_index const& x, bitmap_index const& y)
  {
    VAST_ASSERT(x.concept_);
    VAST_ASSERT(y.concept_);
    return x.concept_->equals(*y.concept_);
  }

private:
  std::unique_ptr<detail::bitmap_index_concept<Bitstream>> concept_;
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
  friend access;
  template <typename> friend struct detail::bitmap_index_model;

public:
  using bitstream_type = Bitstream;

  sequence_bitmap_index() = default;

  sequence_bitmap_index(type t)
    : elem_type_{std::move(t)}
  {
  }

  friend bool
  operator==(sequence_bitmap_index const& x, sequence_bitmap_index const& y)
  {
    return x.elem_type_ == y.elem_type_
        && x.bmis_ == y.bmis_
        && x.size_ == y.size_;
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
      return size_.stretch(1);

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
    return size_.stretch(n);
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
      return {arithmetic_bitmap_index<Bitstream, time::point>(std::forward<Args>(args)...)};
    case type::tag::time_duration:
      return {arithmetic_bitmap_index<Bitstream, time::duration>(std::forward<Args>(args)...)};
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
