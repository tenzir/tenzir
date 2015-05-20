#include "vast/type.h"
#include "vast/value.h"
#include "vast/util/assert.h"
#include "vast/util/json.h"

namespace vast {

namespace {

struct deriver
{
  type operator()(none const&) const
  {
    return type{};
  }

  template <typename T>
  type operator()(T const&) const
  {
    static_assert(type::is_basic<type::from_data<T>>::value,
                  "only basic types allowed");
    return type::from_data<T>{};
  }

  type operator()(enumeration const&) const
  {
    // We can't derive the available fields from a single data instance.
    return type{};
  }

  type operator()(vector const& v) const
  {
    return type::vector{v.empty() ? type{} : type::derive(v[0])};
  }

  type operator()(set const& s) const
  {
    return type::set{s.empty() ? type{} : type::derive(*s.begin())};
  }

  type operator()(table const& x) const
  {
    if (x.empty())
      return type::table{type{}, type{}};

    auto front = x.begin();
    return type::table{type::derive(front->first), type::derive(front->second)};
  }

  type operator()(record const& r) const
  {
    std::vector<type::record::field> fs;
    for (size_t i = 0; i < r.size(); ++i)
      fs.emplace_back("", type::derive(r[i]));

    return type::record{std::move(fs)};
  }
};

} // namespace <anonymous>

type type::derive(data const& d)
{
  return visit(deriver{}, d);
}

type::type()
{
  static auto const default_info = util::make_intrusive<intrusive_info>();
  info_ = default_info;
}

namespace {

struct name_setter
{
  name_setter(std::string& name)
    : name_{name}
  {
  }

  bool operator()(none&)
  {
    return false;
  }

  template <typename T>
  bool operator()(T& x)
  {
    return x.name(std::move(name_));
  }

  std::string& name_;
};

struct name_getter
{
  // FIXME: there exists a bug (#50) with the variant ipmlementation preventing
  // references as return types of visitors, so we work around this using
  // poitners.
  std::string const* operator()(none) const
  {
    static std::string const no_name;
    return &no_name;
  }

  template <typename T>
  std::string const* operator()(T const& x) const
  {
    return &x.name();
  }
};

struct digester
{
  auto operator()(none) const
  {
    static auto const nil_digest = type::hash_type::digest(nil);
    return nil_digest;
  }

  template <typename T>
  auto operator()(T const& x) const
  {
    return x.digest();
  }
};

struct attribute_getter
{
  auto operator()(none) const
  {
    static auto const empty = std::vector<type::attribute>{};
    return &empty;
  }

  template <typename T>
  auto operator()(T const& x) const
  {
    return &x.attributes();
  }
};

struct attribute_finder
{
  attribute_finder(type::attribute::key_type k)
    : key_{k}
  {
  }

  type::attribute const* operator()(none) const
  {
    return nullptr;
  }

  template <typename T>
  type::attribute const* operator()(T const& x) const
  {
    return x.find_attribute(key_);
  }

  type::attribute::key_type key_;
};

} // namespace <anonymous>

bool type::name(std::string name)
{
  return visit(name_setter{name}, *info_);
}

std::string const& type::name() const
{
  return *visit(name_getter{}, *info_);
}

std::vector<type::attribute> const& type::attributes() const
{
  return *visit(attribute_getter{}, *info_);
}

type::attribute const* type::find_attribute(attribute::key_type key) const
{
  return visit(attribute_finder{key}, *info_);
}

type::hash_type::digest_type type::digest() const
{
  return visit(digester{}, *info_);
}

namespace {

struct data_checker
{
  data_checker(type const& t)
    : type_{t}
  {
  }

  bool operator()(none const&) const
  {
    return false;
  }

  template <typename T>
  bool operator()(T const&) const
  {
    static_assert(type::is_basic<type::from_data<T>>::value,
                  "only basic types allowed");

    return is<type::from_data<T>>(type_);
  }

  bool operator()(enumeration const& e) const
  {
    auto t = get<type::enumeration>(type_);
    return t && e < t->fields().size();
  }

  bool operator()(vector const& v) const
  {
    if (v.empty())
      return true;

    auto t = get<type::vector>(type_);
    return t && t->elem().check(*v.begin());
  }

  bool operator()(set const& s) const
  {
    if (s.empty())
      return true;

    auto t = get<type::set>(type_);
    return t && t->elem().check(*s.begin());
  }

  bool operator()(table const& x) const
  {
    if (x.empty())
      return true;

    auto t = get<type::table>(type_);
    if (! t)
      return false;

    auto front = x.begin();
    return t->key().check(front->first) && t->value().check(front->second);
  }

  bool operator()(record const& r) const
  {
    auto t = get<type::record>(type_);
    if (! t || t->fields().size() != r.size())
      return false;

    for (size_t i = 0; i < r.size(); ++i)
      if (! t->fields()[i].type.check(r[i]))
        return false;

    return true;
  }

  type const& type_;
};

struct data_maker
{
  data operator()(none) const
  {
    return nil;
  }

  template <typename T>
  data operator()(T const&) const
  {
    return type::to_data<T>{};
  }

  data operator()(type::alias const& a) const
  {
    return a.type().make();
  }
};

} // namespace <anonymous>

bool type::check(data const& d) const
{
  return which(d) == data::tag::none
      || which(*info_) == type::tag::none
      || visit(data_checker{*this}, d);
}

data type::make() const
{
  return visit(data_maker{}, *this);
}

bool type::basic() const
{
  auto t = which(*info_);
  return t == tag::boolean
      || t == tag::integer
      || t == tag::count
      || t == tag::real
      || t == tag::time_point
//      || t == tag::time_interval
      || t == tag::time_duration
//      || t == tag::time_period
      || t == tag::string
      || t == tag::pattern
      || t == tag::address
      || t == tag::subnet
      || t == tag::port;
}

bool type::container() const
{
  auto t = which(*info_);
  return t == tag::set
      || t == tag::vector
      || t == tag::table;
}

bool type::recursive() const
{
  auto t = which(*info_);
  return t == tag::set
      || t == tag::vector
      || t == tag::table;
}

type::info& expose(type& t)
{
  return *t.info_;
}

type::info const& expose(type const& t)
{
  return *t.info_;
}

bool operator==(type const& lhs, type const& rhs)
{
  return lhs.digest() == rhs.digest();
}

bool operator<(type const& lhs, type const& rhs)
{
  return lhs.digest() < rhs.digest();
}

namespace {

struct congruentor
{
  template <typename T>
  bool operator()(T const&, T const&) const
  {
    return true;
  }

  template <typename T, typename U>
  bool operator()(T const&, U const&) const
  {
    return false;
  }

  template <typename T>
  bool operator()(T const& x, type::alias const& a) const
  {
    using namespace std::placeholders;
    return visit(std::bind(std::cref(*this), std::cref(x), _1), a.type());
  }

  template <typename T>
  bool operator()(type::alias const& a, T const& x) const
  {
    return (*this)(x, a);
  }

  bool operator()(type::alias const& x, type::alias const& y) const
  {
    return visit(*this, x.type(), y.type());
  }

  bool operator()(type::enumeration const& x, type::enumeration const& y) const
  {
    return x.fields().size() == y.fields().size();
  }

  bool operator()(type::vector const& x, type::vector const& y) const
  {
    return visit(*this, x.elem(), y.elem());
  }

  bool operator()(type::set const& x, type::set const& y) const
  {
    return visit(*this, x.elem(), y.elem());
  }

  bool operator()(type::table const& x, type::table const& y) const
  {
    return visit(*this, x.key(), y.key()) && visit(*this, x.value(), y.value());
  }

  bool operator()(type::record const& x, type::record const& y) const
  {
    if (x.fields().size() != y.fields().size())
      return false;

    for (size_t i = 0; i < x.fields().size(); ++i)
      if (! visit(*this, x.fields()[i].type, y.fields()[i].type))
        return false;

    return true;
  }
};

} // namespace <anonymous>

bool congruent(type const& x, type const& y)
{
  return visit(congruentor{}, x, y);
}

bool compatible(type const& lhs, relational_operator op, type const& rhs)
{
  switch (op)
  {
    default:
      return false;
    case match:
    case not_match:
      return is<type::string>(lhs) && is<type::pattern>(rhs);
    case equal:
    case not_equal:
    case less:
    case less_equal:
    case greater:
    case greater_equal:
      return congruent(lhs, rhs);
    case in:
    case not_in:
      switch (which(lhs))
      {
        default:
          return rhs.container();
        case type::tag::string:
          return is<type::string>(rhs) || rhs.container();
        case type::tag::address:
          return is<type::subnet>(rhs) || rhs.container();
      }
    case ni:
      return compatible(rhs, in, lhs);
    case not_ni:
      return compatible(rhs, not_in, lhs);
  }
}

key type::record::each::range_state::key() const
{
  vast::key k(trace.size());
  for (size_t i = 0; i < trace.size(); ++i)
    k[i] = trace[i]->name;
  return k;
}

size_t type::record::each::range_state::depth() const
{
  return trace.size();
}

type::record::each::each(record const& r)
{
  if (r.fields_.empty())
    return;
  auto rec = &r;
  do
  {
    records_.push_back(rec);
    state_.trace.push_back(&rec->fields_[0]);
    state_.offset.push_back(0);
  }
  while ((rec = get<type::record>(state_.trace.back()->type)));
}

bool type::record::each::next()
{
  if (records_.empty())
    return false;

  while (++state_.offset.back() == records_.back()->fields_.size())
  {
    records_.pop_back();
    state_.trace.pop_back();
    state_.offset.pop_back();
    if (records_.empty())
      return false;
  }

  auto f = &records_.back()->fields_[state_.offset.back()];
  state_.trace.back() = f;

  while (auto r = get<type::record>(f->type))
  {
    f = &r->fields_[0];
    records_.emplace_back(r);
    state_.trace.push_back(f);
    state_.offset.push_back(0);
  }

  return true;
}

trial<offset> type::record::resolve(key const& k) const
{
  if (k.empty())
    return error{"empty symbol sequence"};

  offset off;
  auto found = true;
  auto rec = this;
  for (auto id = k.begin(); id != k.end() && found; ++id)
  {
    found = false;
    for (size_t i = 0; i < rec->fields_.size(); ++i)
    {
      if (rec->fields_[i].name == *id)
      {
        // If the name matches, we have to check whether we're continuing with
        // an intermediate record or have reached the last symbol.
        rec = get<record>(rec->fields_[i].type);
        if (! (rec || id + 1 == k.end()))
          return error{"intermediate fields must be records"};

        off.push_back(i);
        found = true;
        break;
      }
    }
  }

  if (! found)
    return error{"non-existant field name"};

  return std::move(off);
}

trial<key> type::record::resolve(offset const& o) const
{
  if (o.empty())
    return error{"empty offset sequence"};

  key k;
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i)
  {
    if (o[i] >= r->fields_.size())
      return error{"offset index ", i, " out of bounds"};

    k.push_back(r->fields_[o[i]].name);

    if (i != o.size() - 1)
    {
      r = get<record>(r->fields_[o[i]].type);
      if (! r)
        return error{"intermediate fields must be records"};
    }
  }

  return std::move(k);
}

namespace {

struct finder
{
  enum mode
  {
    prefix,
    suffix,
    exact,
    any
  };

  finder(key const& k, mode m, std::string const& init = "")
    : mode_{m},
      key_{k}
  {
    VAST_ASSERT(! key_.empty());
    if (! init.empty())
      trace_.push_back(init);
  }

  template <typename T>
  std::vector<std::pair<offset, key>> operator()(T const&) const
  {
    std::vector<std::pair<offset, key>> r;
    if (off_.empty() || key_.size() > trace_.size())
      return r;

    if (mode_ == prefix || mode_ == exact)
    {
      if (mode_ == exact && key_.size() != trace_.size())
        return r;

      for (size_t i = 0; i < key_.size(); ++i)
        if (! match(key_[i], trace_[i]))
          return r;
    }
    else if (mode_ == suffix)
    {
      for (size_t i = 0; i < key_.size(); ++i)
        if (! match(key_[i], trace_[i + trace_.size() - key_.size()]))
          return r;
    }
    else
    {
      for (size_t run = 0; run < trace_.size() - key_.size(); ++run)
      {
        auto found = true;
        for (size_t i = 0; i < key_.size(); ++i)
          if (! match(key_[i], trace_[i + run]))
          {
            found = false;
            break;
          }

        if (found)
          break;
      }

      return r;
    }

    r.emplace_back(off_, trace_);
    return r;
  }

  std::vector<std::pair<offset, key>> operator()(type::record const& r)
  {
    std::vector<std::pair<offset, key>> result;

    off_.push_back(0);
    for (auto& f : r.fields())
    {
      trace_.push_back(f.name);

      for (auto& p : visit(*this, f.type))
        result.push_back(std::move(p));

      trace_.pop_back();
      ++off_.back();
    }

    off_.pop_back();

    return result;
  }

  static bool match(std::string const& key, std::string const& trace)
  {
    return pattern::glob(key).match(trace);
  }

  mode mode_;
  key key_;
  key trace_;
  offset off_;
};

} // namespace <anonymous>

std::vector<std::pair<offset, key>> type::record::find(key const& k) const
{
  return finder{k, finder::exact, name()}(*this);
}

std::vector<std::pair<offset, key>> type::record::find_prefix(key const& k) const
{
  return finder{k, finder::prefix, name()}(*this);
}

std::vector<std::pair<offset, key>> type::record::find_suffix(key const& k) const
{
  return finder{k, finder::suffix, name()}(*this);
}

type::record type::record::flatten() const
{
  record result;
  for (auto& outer : fields_)
    if (auto r = get<record>(outer.type))
      for (auto& inner : r->flatten().fields_)
        result.fields_.emplace_back(outer.name + "." + inner.name, inner.type);
    else
      result.fields_.push_back(outer);

  result.initialize();

  return result;
}

type::record type::record::unflatten() const
{
  record result;
  for (auto& f : fields_)
  {
    auto names = util::to_strings(util::split(f.name, "."));
    VAST_ASSERT(! names.empty());
    record* r = &result;
    for (size_t i = 0; i < names.size() - 1; ++i)
    {
      if (r->fields_.empty() || r->fields_.back().name != names[i])
        r->fields_.emplace_back(std::move(names[i]), type{record{}});
      r = get<record>(r->fields_.back().type);
    }
    r->fields_.emplace_back(std::move(names.back()) , f.type);
  }
  std::vector<std::vector<record*>> rs(1);
  rs.back().push_back(&result);
  auto more = true;
  while (more)
  {
    std::vector<record*> next;
    for (auto& current : rs.back())
      for (auto& f : current->fields_)
        if (auto r = get<record>(f.type))
          next.push_back(r);
    if (next.empty())
      more = false;
    else
      rs.push_back(std::move(next));
  }
  for (auto r = rs.rbegin(); r != rs.rend(); ++r)
    for (auto i : *r)
      i->initialize();
  return result;
}

type const* type::record::at(key const& k) const
{
  auto r = this;
  for (size_t i = 0; i < k.size(); ++i)
  {
    auto& id = k[i];
    field const* f = nullptr;
    for (auto& a : r->fields_)
      if (a.name == id)
      {
        f = &a;
        break;
      }

    if (! f)
      return nullptr;

    if (i + 1 == k.size())
      return &f->type;

    r = get<type::record>(f->type);
    if (! r)
      return nullptr;
  }

  return nullptr;
}

type const* type::record::at(offset const& o) const
{
  auto r = this;
  for (size_t i = 0; i < o.size(); ++i)
  {
    auto& idx = o[i];
    if (idx >= r->fields_.size())
      return nullptr;

    auto t = &r->fields_[idx].type;
    if (i + 1 == o.size())
      return t;

    r = get<type::record>(*t);
    if (! r)
      return nullptr;
  }

  return nullptr;
}

void type::record::initialize()
{
  static constexpr auto desc = "record";
  update(desc, sizeof(desc));

  for (auto f : fields_)
  {
    update(f.name.data(), f.name.size());
    update(f.type.digest());
  }
}

} // namespace vast
