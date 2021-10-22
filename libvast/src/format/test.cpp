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
#include "vast/table_slice_builder_factory.hpp"

#include <caf/settings.hpp>

using caf::holds_alternative;
using caf::visit;

namespace vast::format::test {

namespace {

caf::expected<distribution> make_distribution(const legacy_type& t) {
  using parsers::alpha;
  using parsers::real_opt_dot;
  auto i = std::find_if(t.attributes().begin(), t.attributes().end(),
                        [](auto& attr) { return attr.key == "default"; });
  if (i == t.attributes().end() || !i->value)
    return caf::no_error;
  auto parser = +alpha >> '(' >> real_opt_dot >> ',' >> real_opt_dot >> ')';
  std::string name;
  double p0, p1;
  auto tie = std::tie(name, p0, p1);
  if (!parser(*i->value, tie))
    return caf::make_error(ec::parse_error, "invalid distribution "
                                            "specification");
  if (name == "uniform") {
    if (holds_alternative<legacy_integer_type>(t))
      return {std::uniform_int_distribution<vast::integer::value_type>{
        static_cast<vast::integer::value_type>(p0),
        static_cast<vast::integer::value_type>(p1)}};
    else if (holds_alternative<legacy_bool_type>(t)
             || holds_alternative<legacy_count_type>(t)
             || holds_alternative<legacy_string_type>(t))
      return {std::uniform_int_distribution<count>{static_cast<count>(p0),
                                                   static_cast<count>(p1)}};
    else
      return {std::uniform_real_distribution<long double>{p0, p1}};
  }
  if (name == "normal")
    return {std::normal_distribution<long double>{p0, p1}};
  if (name == "pareto")
    return {detail::pareto_distribution<long double>{p0, p1}};
  return caf::make_error(ec::parse_error, "unknown distribution", name);
}

struct initializer {
  initializer(blueprint& bp)
    : distributions_{bp.distributions}, data_{&bp.data} {
  }

  template <class T>
  caf::expected<void> operator()(const T& t) {
    auto dist = make_distribution(t);
    if (dist)
      distributions_.push_back(std::move(*dist));
    else if (!dist.error())
      *data_ = caf::none;
    else
      return dist.error();
    return caf::no_error;
  }

  caf::expected<void> operator()(const legacy_record_type& r) {
    auto& xs = caf::get<list>(*data_);
    VAST_ASSERT(xs.size() == r.fields.size());
    for (auto i = 0u; i < r.fields.size(); ++i) {
      data_ = &xs[i];
      auto result = visit(*this, r.fields[i].type);
      if (!result)
        return result;
    }
    return caf::no_error;
  }

  std::vector<distribution>& distributions_;
  data* data_ = nullptr;
};

caf::expected<blueprint> make_blueprint(const legacy_type& t) {
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

  template <class T, class U>
  auto operator()(const T&, U&) {
    // Do nothing.
  }

  void operator()(const legacy_integer_type&, integer& x) {
    x.value = static_cast<integer::value_type>(sample());
  }

  void operator()(const legacy_count_type&, count& x) {
    x = sample();
  }

  void operator()(const legacy_real_type&, real& x) {
    x = sample();
  }

  auto operator()(const legacy_time_type&, time& x) {
    x += std::chrono::duration_cast<duration>(double_seconds(sample()));
  }

  auto operator()(const legacy_duration_type&, duration& x) {
    x += std::chrono::duration_cast<duration>(double_seconds(sample()));
  }

  void operator()(const legacy_bool_type&, bool& b) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<count> unif{0, 1};
    b = unif(gen);
  }

  void operator()(const legacy_string_type&, std::string& str) {
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<size_t> unif_size{0, 256};
    std::uniform_int_distribution<char> unif_char{32, 126}; // Printable ASCII
    str.resize(unif_size(gen));
    for (auto& c : str)
      c = unif_char(gen);
  }

  void operator()(const legacy_address_type&, address& addr) {
    // We hash the generated sample into a 128-bit digest to spread out the
    // bits over the entire domain of an IPv6 address.
    lcg gen{static_cast<lcg::result_type>(sample())};
    std::uniform_int_distribution<uint32_t> unif0;
    uint32_t bytes[4];
    for (auto& byte : bytes)
      byte = unif0(gen);
    auto ptr = reinterpret_cast<const uint8_t*>(bytes);
    auto span = std::span<const uint8_t, 16>{ptr, 16};
    addr = address{span};
  }

  void operator()(const legacy_subnet_type&, subnet& sn) {
    static legacy_type addr_type = legacy_address_type{};
    address addr;
    (*this)(addr_type, addr);
    std::uniform_int_distribution<uint8_t> unif{0, 128};
    sn = {std::move(addr), unif(gen_)};
  }

  // Can only be a record, because we don't support randomizing containers.
  void operator()(const legacy_record_type& r, list& xs) {
    for (auto i = 0u; i < xs.size(); ++i)
      visit(*this, r.fields[i].type, xs[i]);
  }

  auto sample() {
    return visit(sampler<Generator>{gen_}, dists_[i++]);
  }

  std::vector<distribution>& dists_;
  size_t i = 0;
  Generator& gen_;
};

std::string_view builtin_schema = R"__(
  type test.full = record{
    n: list<int>,
    b: bool #default="uniform(0,1)",
    i: int #default="uniform(-42000,1337)",
    c: count #default="pareto(0,1)",
    r: real #default="normal(0,1)",
    s: string #default="uniform(0,100)",
    t: time #default="uniform(0,10)",
    d: duration #default="uniform(100,200)",
    a: addr #default="uniform(0,2000000)",
    s: subnet #default="uniform(1000,2000)",
  }
)__";

auto default_schema() {
  schema result;
  auto success = parsers::schema(builtin_schema, result);
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

caf::error reader::schema(vast::schema sch) {
  if (sch.empty())
    return caf::make_error(ec::format_error, "empty schema");
  std::unordered_map<legacy_type, blueprint> blueprints;
  auto subset = vast::schema{};
  for (auto& t : sch) {
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
                             "failed to create table slize builder", t);
  }
  if (subset.empty())
    return caf::make_error(ec::format_error, "no test type in schema");
  schema_ = std::move(subset);
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

caf::error
reader::read_impl(size_t max_events, size_t max_slice_size, consumer& f) {
  VAST_TRACE_SCOPE("{} {} {}", VAST_ARG(max_events), VAST_ARG(max_slice_size),
                   VAST_ARG(num_events_));
  // Sanity checks.
  if (schema_.empty())
    if (auto err = schema(default_schema()))
      return err;
  VAST_ASSERT(next_ != schema_.end());
  if (num_events_ == 0)
    return caf::make_error(ec::end_of_input, "completed generation of events");
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
        VAST_ERROR("{} failed to add blueprint data to slice builder",
                   detail::pretty_type_name(this));
        return caf::make_error(ec::format_error, "failed to add blueprint data "
                                                 "to slice builder");
      }
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
    if (schema_.size() > 1) {
      if (++next_ == schema_.end())
        next_ = schema_.begin();
    }
  }
  finish(f);
  return caf::none;
}

} // namespace vast::format::test
