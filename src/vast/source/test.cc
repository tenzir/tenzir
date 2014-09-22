#include "vast/source/test.h"

#include "vast/event.h"
#include "vast/util/meta.h"
#include "vast/util/hash/murmur.h"

namespace vast {
namespace source {

namespace {

result<distribution> make_distribution(type const& t)
{
  auto a = t.find_attribute(type::attribute::default_);
  if (! a)
    return {};

  auto lparen = a->value.find('(');
  auto rparen = a->value.find(')', lparen);
  if (lparen == std::string::npos || rparen == std::string::npos)
    return error{"invalid parenthesis"};

  auto name = a->value.substr(0, lparen);
  auto parms = a->value.substr(lparen, rparen - lparen + 1);
  auto v = to<vector>(parms, type::real{}, ",", "(", ")");
  if (! v)
    return v.error();

  if (v->size() != 2)
    return error{"all distributions require two parameters"};

  auto p0 = *get<real>((*v)[0]);
  auto p1 = *get<real>((*v)[1]);

  if (name == "uniform")
  {
    if (is<type::integer>(t))
      return {std::uniform_int_distribution<integer>{
                static_cast<integer>(p0), static_cast<integer>(p1)}};
    else if (is<type::boolean>(t) || is<type::count>(t) || is<type::string>(t))
      return {std::uniform_int_distribution<count>{
                static_cast<count>(p0), static_cast<count>(p1)}};
    else
      return {std::uniform_real_distribution<long double>{p0, p1}};
  }

  if (name == "normal")
    return {std::normal_distribution<long double>{p0, p1}};

  if (name == "pareto")
    return {util::pareto_distribution<long double>{p0, p1}};

  return error{"unknown distribution: ", name};
}

struct blueprint_factory
{
  blueprint_factory(test::blueprint& bp)
    : blueprint_{bp}
  {
  }

  template <typename T>
  trial<void> operator()(T const& t)
  {
    auto dist = make_distribution(t);
    if (dist)
    {
      blueprint_.data.push_back(type{t}.make());
      blueprint_.dists.push_back(std::move(*dist));
      return nothing;
    }

    if (dist.empty())
    {
      blueprint_.data.push_back(nil);
      return nothing;
    }

    return dist.error();
  }

  trial<void> operator()(type::record const& r)
  {
    for (auto& f : r.fields())
    {
      auto okay = visit(*this, f.type);
      if (! okay)
        return okay;
    }

    return nothing;
  }

  test::blueprint& blueprint_;
};

template <typename RNG>
struct sampler
{
  sampler(RNG& gen)
    : gen_{gen}
  {
  }

  template <typename D>
  long double operator()(D& dist)
  {
    return static_cast<long double>(dist(gen_));
  }

  RNG& gen_;
};

// Randomizes data according to a list of distributions and a source of
// randomness.
template <typename RNG>
struct randomizer
{
  randomizer(std::vector<distribution>& dists, RNG& gen)
    : dists_{dists},
      gen_{gen}
  {
  }

  template <typename T>
  auto operator()(T&)
    -> util::disable_if_t<
         std::is_same<T, integer>::value
         || std::is_same<T, count>::value
         || std::is_same<T, real>::value
         || std::is_same<T, time_point>::value
         || std::is_same<T, time_duration>::value
       >
  {
    // For types we don't know how to randomize, we just crank the wheel.
    sample();
  }

  void operator()(none)
  {
    // Do nothing.
  }

  void operator()(boolean& b)
  {
    lcg64 gen{static_cast<count>(sample())};
    std::uniform_int_distribution<count> unif{0, 1};
    b = unif(gen);
  }

  template <typename T>
  auto operator()(T& x)
    -> std::enable_if_t<
         std::is_same<T, integer>::value
         || std::is_same<T, count>::value
         || std::is_same<T, real>::value
       >
  {
    x = static_cast<T>(sample());
  }

  template <typename T>
  auto operator()(T& x)
    -> std::enable_if_t<
         std::is_same<T, time_point>::value
         || std::is_same<T, time_duration>::value
       >
  {
    x = time_duration::fractional(sample());
  }

  void operator()(std::string& str)
  {
    auto x = static_cast<uint64_t>(sample());

    // Generate printable characters only.
    lcg64 gen{x};
    std::uniform_int_distribution<char> unif{32, 126};

    str.resize(x % 256);
    for (auto& c : str)
      c = unif(gen);
  }

  void operator()(address& addr)
  {
    // We hash the generated sample into a 128-bit digest to spread out the
    // bits over the entire domain of an IPv6 address.
    auto x = sample();
    uint32_t bytes[4];
    util::detail::murmur3<128>(&x, sizeof(x), 0, bytes);

    // P[ip == v6] = 0.5
    std::uniform_int_distribution<uint8_t> unif{0, 1};
    auto version = unif(gen_) ? address::ipv4 : address::ipv6;
    addr = {bytes, version, address::network};
  }

  void operator()(subnet& sn)
  {
    address addr;
    (*this)(addr);

    std::uniform_int_distribution<uint8_t> unif{0, 128};
    sn = {std::move(addr), unif(gen_)};
  }

  void operator()(port& p)
  {
    using port_type = std::underlying_type_t<port::port_type>;
    std::uniform_int_distribution<port_type> unif{0, 3};
    p.number(static_cast<port::number_type>(sample()));
    p.type(static_cast<port::port_type>(unif(gen_)));
  }

  void operator()(record& r)
  {
    for (auto& d : r)
      visit(*this, d);
  }

  auto sample()
  {
    return visit(sampler<RNG>{gen_}, dists_[i++]);
  }

  std::vector<distribution> dists_;
  size_t i = 0;
  RNG& gen_;
};

} // namespace <anonymous>

test::test(schema sch, event_id id, uint64_t events)
  : schema_{std::move(sch)},
    id_{id},
    events_{events},
    generator_{std::random_device{}()},
    next_{schema_.begin()}
{
  assert(events_ > 0);
}

result<event> test::extract()
{
  if (schema_.empty())
    return error{"must have at least one type in schema"};

  assert(next_ != schema_.end());

  if (blueprints_.empty())
  {
    for (auto& t : schema_)
    {
      blueprint bp;
      auto attempt = visit(blueprint_factory{bp}, t);
      if (! attempt)
        return attempt.error();

      if (auto r = get<type::record>(t))
      {
        auto u = bp.data.unflatten(*r);
        if (! u)
          return u.error();

        bp.data = std::move(*u);
      }

      assert(! bp.data.empty());
      blueprints_[t] = std::move(bp);
    }
  }

  auto& bp = blueprints_[*next_];
  auto d = is<type::record>(*next_) ? data{bp.data} : bp.data[0];
  visit(randomizer<std::mt19937_64>{bp.dists, generator_}, d);

  event e{{std::move(d), *next_}};
  e.timestamp(now());
  e.id(id_++);

  if (++next_ == schema_.end())
    next_ = schema_.begin();

  assert(events_ > 0);
  --events_;
  return std::move(e);
}

bool test::done() const
{
  return events_ == 0;
}

std::string test::describe() const
{
  return "test-source";
}

} // namespace source
} // namespace vast
