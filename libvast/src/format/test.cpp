//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/test.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/schema.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/detail/type_traits.hpp"
#include "vast/error.hpp"
#include "vast/factory.hpp"
#include "vast/logger.hpp"
#include "vast/table_slice_builder.hpp"

#include <caf/settings.hpp>

using caf::holds_alternative;
using caf::visit;

namespace vast::format::test {

namespace {

caf::expected<distribution> make_distribution(const type& t) {
  using parsers::alpha;
  using parsers::real;
  auto tag = t.attribute("default");
  if (!tag || tag->empty())
    return {caf::error{}};
  auto parser = +alpha >> '(' >> real >> ',' >> real >> ')';
  std::string name;
  double p0 = {};
  double p1 = {};
  auto tie = std::tie(name, p0, p1);
  if (!parser(*tag, tie))
    return caf::make_error(ec::parse_error, "invalid distribution "
                                            "specification");
  VAST_DEBUG("generating distribution {} in [{}, {})", name, p0, p1);
  if (name == "uniform") {
    if (caf::holds_alternative<int64_type>(t))
      return distribution{std::uniform_int_distribution<int64_t>{
        static_cast<int64_t>(p0), static_cast<int64_t>(p1)}};
    if (caf::holds_alternative<bool_type>(t)
        || caf::holds_alternative<uint64_type>(t)
        || caf::holds_alternative<string_type>(t))
      return distribution{std::uniform_int_distribution<uint64_t>{
        static_cast<uint64_t>(p0), static_cast<uint64_t>(p1)}};
    return distribution{std::uniform_real_distribution<long double>{p0, p1}};
  }
  if (name == "normal")
    return distribution{std::normal_distribution<long double>{p0, p1}};
  if (name == "pareto")
    return distribution{detail::pareto_distribution<long double>{p0, p1}};
  return caf::make_error(ec::parse_error, "unknown distribution", name);
}

caf::expected<blueprint> make_blueprint(const type& t) {
  blueprint bp;
  auto initialize = detail::overload{
    [&]<basic_type T>(const T& x) -> caf::error {
      bp.data = std::make_unique<data>(x.construct());
      // It's important that we pass t rather than x here as this needs access
      // to the tags, which are part of type but not the concrete type.
      auto dist = make_distribution(t);
      if (!dist)
        return dist.error();
      bp.distributions.push_back(std::move(*dist));
      return caf::none;
    },
    [&](const record_type& x) -> caf::error {
      auto l = list{};
      l.reserve(x.num_fields());
      for (const auto& [_, ft] : x.fields()) {
        l.push_back(ft.construct());
        auto dist = make_distribution(ft);
        if (!dist) {
          // TODO: This may cause a mismatch between data and distributions. We
          // probably need to make bp.distributions a list of optional
          // distributions so we can add a nullopt in this case, and then handle
          // that case when visiting the distributions by printing a warning and
          // just adding null. As-is, we just skip the field, which may result
          // in errors for schemas that such fields anywhere but at the end.
          if (!dist.error())
            continue;
          return dist.error();
        }
        bp.distributions.push_back(std::move(*dist));
      }
      l.reserve(x.num_fields());
      bp.data = std::make_unique<data>(std::move(l));
      return caf::none;
    },
    []<complex_type T>(const T& x) -> caf::error {
      // Complex types other than record_type are not supported.
      return caf::make_error(
        ec::unimplemented,
        fmt::format("test generator does not support complex type {}", x));
    },
  };
  if (auto err = visit(initialize, t))
    return err;
  VAST_DEBUG("created blueprint for type {} with stub data {}", t, *bp.data);
  return bp;
}

template <class Generator>
struct sampler {
  sampler(Generator& gen) : gen_{gen} {
  }

  template <class Distribution>
  auto operator()(Distribution& dist) {
    return dist(gen_);
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

  template <basic_type T, class U>
    requires(!std::is_same_v<type_to_data_t<T>, U>)
  auto operator()(const T&, U&) {
    // Do nothing.
  }

  template <complex_type T, class U>
  auto operator()(const T&, U&) {
    // Do nothing.
  }

  void operator()(const int64_type&, int64_t& x) {
    x = sample();
  }

  void operator()(const uint64_type&, uint64_t& x) {
    x = sample();
  }

  void operator()(const double_type&, double& x) {
    x = sample();
  }

  auto operator()(const time_type&, time& x) {
    x += std::chrono::duration_cast<duration>(double_seconds(sample()));
  }

  auto operator()(const duration_type&, duration& x) {
    x += std::chrono::duration_cast<duration>(double_seconds(sample()));
  }

  void operator()(const bool_type&, bool& b) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<uint64_t> unif{0, 1};
    b = unif(gen);
  }

  void operator()(const string_type&, std::string& str) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<unsigned int> unif_size{0, 256};
    std::uniform_int_distribution<int> unif_char{32, 126}; // Printable ASCII
    str.resize(static_cast<size_t>(unif_size(gen)));
    for (auto& c : str)
      c = static_cast<char>(unif_char(gen));
  }

  void operator()(const ip_type&, ip& addr) {
    // We hash the generated sample into a 128-bit digest to spread out the
    // bits over the entire domain of an IPv6 address.
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<uint32_t> unif0;
    uint32_t bytes[4];
    for (auto& byte : bytes)
      byte = unif0(gen);
    auto ptr = reinterpret_cast<const uint8_t*>(bytes);
    auto span = std::span<const uint8_t, 16>{ptr, 16};
    addr = ip{span};
  }

  void operator()(const subnet_type&, subnet& sn) {
    ip addr;
    (*this)(ip_type{}, addr);
    std::uniform_int_distribution<uint8_t> unif{0, 128};
    sn = {addr, unif(gen_)};
  }

  // Can only be a record, because we don't support randomizing containers.
  void operator()(const record_type& r, list& xs) {
    for (auto i = 0u; i < xs.size(); ++i)
      visit(*this, r.field(i).type, xs[i]);
  }

  void operator()(const record_type& r, record& xs) {
    for (auto i = 0u; i < xs.size(); ++i)
      visit(*this, r.field(i).type, as_vector(xs)[i].second);
  }

  auto sample() {
    return visit(sampler<Generator>{gen_}, dists_[i++]);
  }

  std::vector<distribution>& dists_;
  size_t i = 0;
  Generator& gen_;
};

std::string_view builtin_module = R"__(
  type test.full = record{
    n: list<int>,
    b: bool #default="uniform(0,1)",
    i: int64 #default="uniform(-5158,1337)",
    c: uint64 #default="pareto(0,1)",
    r: double #default="normal(0,1)",
    s: string #default="uniform(0,100)",
    t: time #default="uniform(0,10)",
    d: duration #default="uniform(100,200)",
    a: ip #default="uniform(0,2000000)",
    u: subnet #default="uniform(1000,2000)",
  }
)__";

auto default_module() {
  module result;
  auto success = parsers::module(builtin_module, result);
  VAST_ASSERT(success);
  return result;
}

using default_randomizer = randomizer<std::mt19937_64>;

} // namespace

reader::reader(const caf::settings& options, std::unique_ptr<std::istream>)
  : super{options},
    generator_{vast::defaults::import::test::seed(options)},
    num_events_{caf::get_or(options, "vast.import.max-events",
                            vast::defaults::import::max_events)} {
  if (num_events_ == 0)
    num_events_ = std::numeric_limits<size_t>::max();
  if (caf::holds_alternative<std::string>(options, "vast.import.read-timeout"))
    VAST_VERBOSE("{} ingnores the unsupported read timeout option",
                 detail::pretty_type_name(this));
}

void reader::reset(std::unique_ptr<std::istream>) {
  // This function intentionally does nothing, as the test reader generates data
  // instead of reading from an input stream. It only exists for compatibility
  // with our reader abstraction.
}

caf::error reader::module(vast::module mod) {
  if (mod.empty())
    return caf::make_error(ec::format_error, "empty schema");
  std::unordered_map<type, blueprint> blueprints;
  auto subset = vast::module{};
  for (const auto& t : mod) {
    auto sn = detail::split(t.name(), ".");
    if (sn.size() != 2 || sn[0] != "test")
      continue;
    subset.add(t);
    if (auto bp = make_blueprint(t))
      blueprints.emplace(t, std::move(*bp));
    else
      return caf::make_error(ec::format_error, "failed to create blueprint", t);
    if (auto ptr = builder(t); ptr == nullptr)
      return caf::make_error(ec::format_error,
                             "failed to create table slice builder", t);
  }
  if (subset.empty())
    return caf::make_error(ec::format_error, "no test type in schema");
  module_ = std::move(subset);
  blueprints_ = std::move(blueprints);
  next_ = module_.begin();
  return caf::none;
}

module reader::module() const {
  return module_;
}

const char* reader::name() const {
  return "test-reader";
}

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(max_events), VAST_ARG(max_slice_size),
                   VAST_ARG(num_events_));
  // Sanity checks.
  if (module_.empty())
    if (auto err = module(default_module()))
      return err;
  VAST_ASSERT(next_ != module_.end());
  if (num_events_ == 0)
    return caf::make_error(ec::end_of_input, "completed generation of events");
  // Loop until we reach the `max_events` limit or exhaust the configured
  // `num_events_` threshold.
  size_t produced = 0;
  while (produced < max_events) {
    // Generate random data.
    const auto& t = *next_;
    auto bp = blueprints_.find(t);
    VAST_ASSERT(bp != blueprints_.end());
    auto ptr = builder(t);
    VAST_ASSERT(ptr != nullptr);
    auto rows = std::min({num_events_, max_events - produced, max_slice_size});
    VAST_ASSERT(rows > 0);
    for (size_t i = 0; i < rows; ++i) {
      auto randomizer
        = default_randomizer{bp->second.distributions, generator_};
      visit(randomizer, t, *bp->second.data);
      if (!ptr->recursive_add(*bp->second.data, t)) {
        VAST_ERROR("{} failed to add blueprint data to slice builder",
                   detail::pretty_type_name(this));
        return caf::make_error(ec::format_error, "failed to add blueprint data "
                                                 "to slice builder");
      }
      ++batch_events_;
    }
    // Emit table slice.
    if (auto err = finish(f, ptr))
      return err;
    // Check for EOF and prepare for next iteration.
    if (num_events_ == rows)
      return caf::make_error(ec::end_of_input, "completed generation of "
                                               "events");
    num_events_ -= rows;
    produced += rows;
    if (module_.size() > 1) {
      if (++next_ == module_.end())
        next_ = module_.begin();
    }
  }
  if (auto err = finish(f))
    return err;
  return caf::none;
}

} // namespace vast::format::test
