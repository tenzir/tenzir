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

#include "vast/format/test.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"

using caf::holds_alternative;
using caf::visit;

namespace vast {
namespace format {
namespace test {

namespace {

expected<distribution> make_distribution(const type& t) {
  using parsers::real_opt_dot;
  using parsers::alpha;
  auto i = std::find_if(t.attributes().begin(), t.attributes().end(),
                        [](auto& attr) { return attr.key == "default"; });
  if (i == t.attributes().end() || !i->value)
    return no_error;
  auto parser = +alpha >> '(' >> real_opt_dot >> ',' >> real_opt_dot >> ')';
  std::string name;
  double p0, p1;
  auto tie = std::tie(name, p0, p1);
  if (!parser(*i->value, tie))
    return make_error(ec::parse_error, "invalid distribution specification");
  if (name == "uniform") {
    if (holds_alternative<integer_type>(t))
      return {std::uniform_int_distribution<integer>{static_cast<integer>(p0),
                                                     static_cast<integer>(p1)}};
    else if (holds_alternative<boolean_type>(t)
             || holds_alternative<count_type>(t)
             || holds_alternative<string_type>(t))
      return {std::uniform_int_distribution<count>{static_cast<count>(p0),
                                                   static_cast<count>(p1)}};
    else
      return {std::uniform_real_distribution<long double>{p0, p1}};
  }
  if (name == "normal")
    return {std::normal_distribution<long double>{p0, p1}};
  if (name == "pareto")
    return {detail::pareto_distribution<long double>{p0, p1}};
  return make_error(ec::parse_error, "unknown distribution", name);
}

struct initializer {
  initializer(blueprint& bp)
    : distributions_{bp.distributions},
      data_{&bp.data} {
  }

  template <class T>
  expected<void> operator()(const T& t) {
    auto dist = make_distribution(t);
    if (dist)
      distributions_.push_back(std::move(*dist));
    else if (!dist.error())
      *data_ = caf::none;
    else
      return dist.error();
    return no_error;
  }

  expected<void> operator()(const record_type& r) {
    auto& v = caf::get<vector>(*data_);
    VAST_ASSERT(v.size() == r.fields.size());
    for (auto i = 0u; i < r.fields.size(); ++i) {
      data_ = &v[i];
      auto result = visit(*this, r.fields[i].type);
      if (!result)
        return result;
    }
    return no_error;
  }

  std::vector<distribution>& distributions_;
  data* data_ = nullptr;
};

expected<blueprint> make_blueprint(const type& t) {
  blueprint bp;
  bp.data = construct(t);
  auto result = visit(initializer{bp}, t);
  if (!result)
    return result.error();
  return bp;
}

template <class Generator>
struct sampler {
  sampler(Generator& gen) : gen_{gen} {
  }

  template <class Distribution>
  long double operator()(Distribution& dist) {
    return static_cast<long double>(dist(gen_));
  }

  Generator& gen_;
};

// Randomizes data according to a list of distributions and a source of
// randomness.
template <class Generator>
struct randomizer {
  randomizer(std::vector<distribution>& dists, Generator& gen)
    : dists_{dists}, gen_{gen} {
  }

  template <class T, class U>
  auto operator()(const T&, U&) {
    // Do nothing.
  }

  void operator()(const integer_type&, integer& x) {
    x = static_cast<integer>(sample());
  }

  void operator()(const count_type&, count& x) {
    x = static_cast<count>(sample());
  }

  void operator()(const real_type&, real& x) {
    x = static_cast<real>(sample());
  }

  auto operator()(const timestamp_type&, timestamp& x) {
    x += std::chrono::duration_cast<timespan>(double_seconds(sample()));
  }

  auto operator()(const timespan_type&, timespan& x) {
    x += std::chrono::duration_cast<timespan>(double_seconds(sample()));
  }

  void operator()(const boolean_type&, boolean& b) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<count> unif{0, 1};
    b = unif(gen);
  }

  void operator()(const string_type&, std::string& str) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<size_t> unif_size{0, 256};
    std::uniform_int_distribution<char> unif_char{32, 126}; // Printable ASCII
    str.resize(unif_size(gen));
    for (auto& c : str)
      c = unif_char(gen);
  }

  void operator()(const address_type&, address& addr) {
    // We hash the generated sample into a 128-bit digest to spread out the
    // bits over the entire domain of an IPv6 address.
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<uint32_t> unif0;
    uint32_t bytes[4];
    for (auto& byte : bytes)
      byte = unif0(gen);
    // P[ip == v6] = 0.5
    std::uniform_int_distribution<uint8_t> unif1{0, 1};
    auto version = unif1(gen_) == 0 ? address::ipv4 : address::ipv6;
    addr = {bytes, version, address::network};
  }

  void operator()(const subnet_type&, subnet& sn) {
    static type addr_type = address_type{};
    address addr;
    (*this)(addr_type, addr);
    std::uniform_int_distribution<uint8_t> unif{0, 128};
    sn = {std::move(addr), unif(gen_)};
  }

  void operator()(const port_type&, port& p) {
    using port_type = std::underlying_type_t<port::port_type>;
    std::uniform_int_distribution<port_type> unif{0, 3};
    p.number(static_cast<port::number_type>(sample()));
    p.type(static_cast<port::port_type>(unif(gen_)));
  }

  // Can only be a record, because we don't support randomizing containers.
  void operator()(const record_type& r, vector& v) {
    for (auto i = 0u; i < v.size(); ++i)
      visit(*this, r.fields[i].type, v[i]);
  }

  auto sample() {
    return visit(sampler<Generator>{gen_}, dists_[i++]);
  }

  std::vector<distribution>& dists_;
  size_t i = 0;
  Generator& gen_;
};

std::string_view builtin_schema = R"__(
  type test = record{
    n: set<int>,
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

auto default_schema() {
  schema result;
  auto success = parsers::schema(builtin_schema, result);
  VAST_ASSERT(success);
  return result;
}

using default_randomizer = randomizer<std::mt19937_64>;

} // namespace <anonymous>

// reader::reader(size_t seed, uint64_t n, vast::schema sch)
//   : generator_{seed},
//     num_events_{n} {
//   auto result = schema(std::move(sch));
//   VAST_ASSERT(result);
// }

reader::reader(caf::atom_value slice_type, size_t seed, size_t n)
  : super{slice_type}, generator_{seed}, num_events_{n} {
  // nop
}

caf::error reader::schema(vast::schema sch) {
  if (sch.empty())
    return make_error(ec::format_error, "empty schema");
  std::unordered_map<type, blueprint> blueprints;
  for (auto& t : sch) {
    if (auto bp = make_blueprint(t))
      blueprints.emplace(t, std::move(*bp));
    else
      return make_error(ec::format_error, "failed to create blueprint", t);
    if (auto ptr = builder(t); ptr == nullptr)
      return make_error(ec::format_error,
                        "failed to create table slize builder", t);
  }
  schema_ = std::move(sch);
  blueprints_ = std::move(blueprints);
  next_ = schema_.begin();
  return caf::none;
}

schema reader::schema() const {
  return schema_;
}

const char* reader::name() const {
  return "test-reader";
}

caf::error reader::read_impl(size_t max_events, size_t max_slice_size,
                             consumer& f) {
  // Sanity checks.
  if (schema_.empty())
    schema(default_schema());
  VAST_ASSERT(next_ != schema_.end());
  if (num_events_ == 0)
    return make_error(ec::end_of_input, "completed generation of events");
  // Loop until we reach the `max_events` limit or exhaust the configured
  // `num_events_` threshold.
  size_t produced = 0;
  while (produced < max_events) {
    // Generate random data.
    auto& t = *next_;
    auto& bp = blueprints_[t];
    auto ptr = builder(t);
    VAST_ASSERT(ptr != nullptr);
    auto rows = std::min({num_events_, max_events - produced, max_slice_size});
    VAST_ASSERT(rows > 0);
    for (size_t i = 0; i < rows; ++i) {
      visit(default_randomizer{bp.distributions, generator_}, t, bp.data);
      if (!ptr->recursive_add(bp.data, t)) {
        VAST_ERROR(this, "failed to add blueprint data to slice builder");
        return make_error(ec::format_error,
                          "failed to add blueprint data to slice builder");
      }
    }
    // Emit table slice.
    if (auto err = finish(f, ptr))
      return err;
    // Check for EOF and prepare for next iteration.
    if (num_events_ == rows)
      return make_error(ec::end_of_input, "completed generation of events");
    num_events_ -= rows;
    produced += rows;
    if (schema_.size() > 1) {
     if (++next_ == schema_.end())
       next_ = schema_.begin();
    }
  }
  finish(f);
  return caf::none;
}

} // namespace test
} // namespace format
} // namespace vast
