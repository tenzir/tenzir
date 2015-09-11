#include "vast/event.h"
#include "vast/actor/source/test.h"
#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/numeric/real.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/vast/detail/to_schema.h"
#include "vast/util/assert.h"
#include "vast/util/meta.h"
#include "vast/util/hash/murmur.h"

namespace vast {
namespace source {

namespace {

result<distribution> make_distribution(type const& t) {
  auto a = t.find_attribute(type::attribute::default_);
  if (!a)
    return {};
  static auto num = parsers::real_opt_dot;
  static auto param_parser = +parsers::alpha >> '(' >> num >> ',' >> num >> ')';
  std::string name;
  double p0, p1;
  auto tie = std::tie(name, p0, p1);
  if (!param_parser(a->value, tie))
    return error{"invalid distribution specification"};
  if (name == "uniform") {
    if (is<type::integer>(t))
      return {std::uniform_int_distribution<integer>{static_cast<integer>(p0),
                                                     static_cast<integer>(p1)}};
    else if (is<type::boolean>(t) || is<type::count>(t) || is<type::string>(t))
      return {std::uniform_int_distribution<count>{static_cast<count>(p0),
                                                   static_cast<count>(p1)}};
    else
      return {std::uniform_real_distribution<long double>{p0, p1}};
  }
  if (name == "normal")
    return {std::normal_distribution<long double>{p0, p1}};
  if (name == "pareto")
    return {util::pareto_distribution<long double>{p0, p1}};
  return error{"unknown distribution: ", name};
}

struct blueprint_factory {
  blueprint_factory(test_state::blueprint& bp) : blueprint_{bp} {
  }

  template <typename T>
  trial<void> operator()(T const& t) {
    auto dist = make_distribution(t);
    if (dist) {
      blueprint_.data.push_back(type{t}.make());
      blueprint_.dists.push_back(std::move(*dist));
      return nothing;
    }
    if (dist.empty()) {
      blueprint_.data.push_back(nil);
      return nothing;
    }
    return dist.error();
  }

  trial<void> operator()(type::record const& r) {
    for (auto& f : r.fields()) {
      auto okay = visit(*this, f.type);
      if (!okay)
        return okay;
    }
    return nothing;
  }

  test_state::blueprint& blueprint_;
};

template <typename RNG>
struct sampler {
  sampler(RNG& gen) : gen_{gen} {
  }

  template <typename D>
  long double operator()(D& dist) {
    return static_cast<long double>(dist(gen_));
  }

  RNG& gen_;
};

// Randomizes data according to a list of distributions and a source of
// randomness.
template <typename RNG>
struct randomizer {
  randomizer(std::vector<distribution>& dists, RNG& gen)
    : dists_{dists}, gen_{gen} {
  }

  template <typename T>
  auto operator()(T&)
    -> util::disable_if_t<
         std::is_same<T, integer>::value
           || std::is_same<T, count>::value
           || std::is_same<T, real>::value
           || std::is_same<T, time::point>::value
           || std::is_same<T, time::duration>::value
        > {
    // For types we don't know how to randomize, we just crank the wheel.
    sample();
  }

  void operator()(none) {
    // Do nothing.
  }

  void operator()(boolean& b) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<count> unif{0, 1};
    b = unif(gen);
  }

  template <typename T>
  auto operator()(T& x)
    -> std::enable_if_t<
         std::is_same<T, integer>::value
           || std::is_same<T, count>::value
           || std::is_same<T, real>::value
        > {
    x = static_cast<T>(sample());
  }

  template <typename T>
  auto operator()(T& x)
    -> std::enable_if_t<
         std::is_same<T, time::point>::value
           || std::is_same<T, time::duration>::value
       > {
    x += time::fractional(sample());
  }

  void operator()(std::string& str) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<size_t> unif_size{0, 256};
    std::uniform_int_distribution<char> unif_char{32, 126}; // Printable ASCII
    str.resize(unif_size(gen));
    for (auto& c : str)
      c = unif_char(gen);
  }

  void operator()(address& addr) {
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

  void operator()(subnet& sn) {
    address addr;
    (*this)(addr);
    std::uniform_int_distribution<uint8_t> unif{0, 128};
    sn = {std::move(addr), unif(gen_)};
  }

  void operator()(port& p) {
    using port_type = std::underlying_type_t<port::port_type>;
    std::uniform_int_distribution<port_type> unif{0, 3};
    p.number(static_cast<port::number_type>(sample()));
    p.type(static_cast<port::port_type>(unif(gen_)));
  }

  void operator()(record& r) {
    for (auto& d : r)
      visit(*this, d);
  }

  auto sample() {
    return visit(sampler<RNG>{gen_}, dists_[i++]);
  }

  std::vector<distribution>& dists_;
  size_t i = 0;
  RNG& gen_;
};

} // namespace <anonymous>

test_state::test_state(local_actor* self)
  : state{self, "test-source"},
    generator_{std::random_device{}()} {
  static auto builtin_schema = R"__(
    type test = record
    {
      b: bool &default="uniform(0,1)",
      i: int &default="uniform(-42000,1337)",
      c: count &default="pareto(0,1)",
      r: real &default="normal(0,1)",
      s: string &default="uniform(0,100)",
      t: time &default="uniform(0,10)",
      d: duration &default="uniform(100,200)",
      a: addr &default="uniform(0,2000000)",
      s: subnet &default="uniform(1000,2000)",
      p: port &default="uniform(1,65384)"
    }
  )__";
  auto t = detail::to_schema(builtin_schema);
  VAST_ASSERT(t);
  schema(*t);
}

schema test_state::schema() {
  return schema_;
}

void test_state::schema(vast::schema const& sch) {
  VAST_ASSERT(!sch.empty());
  schema_ = sch;
  next_ = schema_.begin();
  for (auto& t : schema_) {
    blueprint bp;
    auto attempt = visit(blueprint_factory{bp}, t);
    if (!attempt) {
      VAST_ERROR(this, "failed to generate blueprint:", attempt.error());
      self->quit(exit::error);
      return;
    }
    if (auto r = get<type::record>(t)) {
      auto u = bp.data.unflatten(*r);
      if (!u) {
        VAST_ERROR(this, "failed to unflatten record:", u.error());
        self->quit(exit::error);
        return;
      }
      bp.data = std::move(*u);
    }
    VAST_ASSERT(!bp.data.empty());
    blueprints_[t] = std::move(bp);
  }
}

result<event> test_state::extract() {
  VAST_ASSERT(next_ != schema_.end());
  // Generate random data.
  auto& bp = blueprints_[*next_];
  randomizer<std::mt19937_64>{bp.dists, generator_}(bp.data);
  auto d = is<type::record>(*next_) ? data{bp.data} : bp.data[0];
  // Fill a new event.
  event e{{std::move(d), *next_}};
  e.timestamp(time::now());
  e.id(id_++);
  // Advance to next type in schema.
  if (++next_ == schema_.end())
    next_ = schema_.begin();
  VAST_ASSERT(num_events_ > 0);
  if (--num_events_ == 0)
    done_ = true;
  return std::move(e);
}

behavior test(stateful_actor<test_state>* self, event_id id, uint64_t events) {
  VAST_ASSERT(events > 0);
  self->state.id_ = id;
  self->state.num_events_ = events;
  return make(self);
}

} // namespace source
} // namespace vast
