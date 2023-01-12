//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE pivoter

#include "vast/system/pivoter.hpp"

#include "vast/command.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/format/zeek.hpp"
#include "vast/table_slice.hpp"
#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/test.hpp"

#include <caf/actor_system.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

using namespace vast;

namespace {

const auto zeek_conn_m57_head = R"__(#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	conn
#open	2019-06-07-14-30-44
#fields	ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	local_resp	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes	tunnel_parents	community_id
#types	time	string	addr	port	addr	port	enum	string	interval	count	count	string	bool	bool	count	string	count	count	count	count	set[string]	string
1258531221.486539	Cz8F3O3rmUNrd0OxS5	192.168.1.102	68	192.168.1.1	67	udp	dhcp	0.163820	301	300	SF	-	-	0	Dd	1	329	1	328	-	1:aWZfLIquYlCxKGuJ62fQGlgFzAI=
1258531680.237254	CeJFOE1CNssyQjfJo1	192.168.1.103	137	192.168.1.255	137	udp	dns	3.780125	350	0	S0	-	-	0	D	7	546	0	0	-	1:fLbpXGtS1VgDhqUW+WYaP0v+NuA=
1258531693.816224	CJ5Eva2VOSC05Q4yx7	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748647	350	0	S0	-	-	0	D	7	546	0	0	-	1:BY/pbReW8Oa+xSY2fNZPZUB1Nnk=
1258531635.800933	Cj9SnC3M3m1jTn34S5	192.168.1.103	138	192.168.1.255	138	udp	-	46.725380	560	0	S0	-	-	0	D	3	644	0	0	-	1:tShwwbRwEMd3S8SvqZxGyvKm+1c=
1258531693.825212	C1BPJn1ngD4I5yhIL8	192.168.1.102	138	192.168.1.255	138	udp	-	2.248589	348	0	S0	-	-	0	D	2	404	0	0	-	1:4iHhzk49NeoFdK6VHSCw4ruRbsw=
1258531803.872834	CWi3Bb4OlpMeChLx6l	192.168.1.104	137	192.168.1.255	137	udp	dns	3.748893	350	0	S0	-	-	0	D	7	546	0	0	-	1:+igyiyVnNTFDre/V6pYx89+Lgr8=
1258531747.077012	Ccl0yW2y0XqwDCh0Oj	192.168.1.104	138	192.168.1.255	138	udp	-	59.052898	549	0	S0	-	-	0	D	3	633	0	0	-	1:5NWtNjiw4JPUO4fMM0WobJPFeU8=
1258531924.321413	CojK5e1MpFgJnwlp6a	192.168.1.103	68	192.168.1.1	67	udp	dhcp	0.044779	303	300	SF	-	-	0	Dd	1	331	1	328	-	1:oG55uQUH+XuHYHOFV0c+yOutW8E=
1258531939.613071	Cgq2X52qyXxAAi9avc	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	-	0	D	1	229	0	0	-	1:4iHhzk49NeoFdK6VHSCw4ruRbsw=
1258532046.693816	CwF9px2owZEPahqWsg	192.168.1.104	68	192.168.1.1	67	udp	dhcp	0.002103	311	300	SF	-	-	0	Dd	1	339	1	328	-	-
1258532143.457078	Cu0QHL1w6Dp3Z5y0Pg	192.168.1.102	1170	192.168.1.1	53	udp	dns	0.068511	36	215	SF	-	-	0	Dd	1	64	1	243	-	1:FVMx3YawO69eZmiaMJJbrs6447E=
1258532203.657268	C37rla3neljCJgPsE2	192.168.1.104	1174	192.168.1.1	53	udp	dns	0.170962	36	215	SF	-	-	0	Dd	1	64	1	243	-	1:79fDvfGNCWV1JBYjXCE5Ov1FuMM=
1258532331.365294	C4ik1w2JPTX1zO2Ubi	192.168.1.1	5353	224.0.0.251	5353	udp	dns	0.100381	273	0	S0	-	-	0	D	2	329	0	0	-	1:aGi0Bt5ApW6HEEO7wfz+PwvniIU=
1258532331.365330	CCGBMdPGplqLzQCjg	fe80::219:e3ff:fee7:5d23	5353	ff02::fb	5353	udp	dns	0.100371	273	0	S0	-	-	0	D	2	369	0	0	-	1:JoBDvaK4Tt6BfWSKWPKaJTELr2M=)__";

template <class Reader>
std::vector<table_slice> inhale(const char* data) {
  auto input = std::make_unique<std::istringstream>(data);
  Reader reader{caf::settings{}, std::move(input)};
  std::vector<table_slice> slices;
  auto add_slice = [&](table_slice slice) {
    slices.emplace_back(std::move(slice));
  };
  auto [err, produced]
    = reader.read(std::numeric_limits<size_t>::max(),
                  defaults::import::table_slice_size, add_slice);
  if (err != caf::none && err != ec::end_of_input) {
    auto str = to_string(err);
    FAIL("reader returned an error: " << str);
  }
  return slices;
}

struct mock_node_state {
  std::vector<invocation> invocs;
  static inline constexpr const char* name = "mock-node";
};

caf::behavior mock_node(caf::stateful_actor<mock_node_state>* self) {
  return {
    [=](atom::spawn, invocation invocation) {
      self->state.invocs.push_back(std::move(invocation));
    },
  };
}

struct fixture : fixtures::deterministic_actor_system {
  fixture() : fixtures::deterministic_actor_system(VAST_PP_STRINGIFY(SUITE)) {
    MESSAGE("spawn mock node");
    node = sys.spawn(mock_node);
    run();
  }

  ~fixture() {
    self->send_exit(aut, caf::exit_reason::user_shutdown);
  }

  void spawn_aut(expression expr, std::string target_type) {
    aut = sys.spawn(system::pivoter, caf::actor_cast<system::node_actor>(node),
                    std::move(target_type), std::move(expr));
    run();
  }

  const std::vector<table_slice> slices
    = inhale<format::zeek::reader>(zeek_conn_m57_head);

  caf::actor node;
  caf::actor aut;
};

} // namespace

FIXTURE_SCOPE(pivoter_tests, fixture)

TEST(count IP point query without candidate check) {
  MESSAGE("build expression");
  auto expr = unbox(to<expression>("proto == \"udp\" && orig_bytes < 600"));
  MESSAGE("spawn the pivoter with the target type pcap");
  spawn_aut(expr, "pcap.packet");
  MESSAGE("send a table slice");
  self->send(aut, slices[0]);
  // The pivoter maps the slice to an expression and passes it on.
  run();
  auto& node_state = deref<caf::stateful_actor<mock_node_state>>(node).state;
  REQUIRE_EQUAL(node_state.invocs.size(), 1u);
  CHECK_EQUAL(
    node_state.invocs[0].arguments[0],
    "(#type == \"pcap.packet\" && community_id in "
    "[\"1:aWZfLIquYlCxKGuJ62fQGlgFzAI=\", "
    "\"1:fLbpXGtS1VgDhqUW+WYaP0v+NuA=\", \"1:BY/pbReW8Oa+xSY2fNZPZUB1Nnk=\", "
    "\"1:tShwwbRwEMd3S8SvqZxGyvKm+1c=\", \"1:4iHhzk49NeoFdK6VHSCw4ruRbsw=\", "
    "\"1:+igyiyVnNTFDre/V6pYx89+Lgr8=\", \"1:5NWtNjiw4JPUO4fMM0WobJPFeU8=\", "
    "\"1:oG55uQUH+XuHYHOFV0c+yOutW8E=\", \"1:FVMx3YawO69eZmiaMJJbrs6447E=\", "
    "\"1:79fDvfGNCWV1JBYjXCE5Ov1FuMM=\", \"1:aGi0Bt5ApW6HEEO7wfz+PwvniIU=\", "
    "\"1:JoBDvaK4Tt6BfWSKWPKaJTELr2M=\"])");
}

FIXTURE_SCOPE_END()
