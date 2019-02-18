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

#include "vast/format/zeek.hpp"

#define SUITE format

#include "vast/test/test.hpp"

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/event.hpp"

using namespace vast;
using namespace std::string_literals;

namespace {

template <class Attribute>
bool zeek_parse(const type& t, const std::string& s, Attribute& attr) {
  return format::zeek::make_zeek_parser<std::string::const_iterator>(t)(s,
                                                                        attr);
}

std::string_view conn_log_100_events = R"__(#separator \x09
#set_separator	,
#empty_field	(empty)
#unset_field	-
#path	conn
#open	2014-05-23-18-02-04
#fields	ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes	tunnel_parents
#types	time	string	addr	port	addr	port	enum	string	interval	count	count	string	bool	count	string	count	count	count	count	table[string]
1258531221.486539	Pii6cUUq1v4	192.168.1.102	68	192.168.1.1	67	udp	-	0.163820	301	300	SF	-	0	Dd	1	329	1	328	(empty)
1258531680.237254	nkCxlvNN8pi	192.168.1.103	137	192.168.1.255	137	udp	dns	3.780125	350	0	S0	-	0	D	7	546	0	0	(empty)
1258531693.816224	9VdICMMnxQ7	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748647	350	0	S0	-	0	D	7	546	0	0	(empty)
1258531635.800933	bEgBnkI31Vf	192.168.1.103	138	192.168.1.255	138	udp	-	46.725380	560	0	S0	-	0	D	3	644	0	0	(empty)
1258531693.825212	Ol4qkvXOksc	192.168.1.102	138	192.168.1.255	138	udp	-	2.248589	348	0	S0	-	0	D	2	404	0	0	(empty)
1258531803.872834	kmnBNBtl96d	192.168.1.104	137	192.168.1.255	137	udp	dns	3.748893	350	0	S0	-	0	D	7	546	0	0	(empty)
1258531747.077012	CFIX6YVTFp2	192.168.1.104	138	192.168.1.255	138	udp	-	59.052898	549	0	S0	-	0	D	3	633	0	0	(empty)
1258531924.321413	KlF6tbPUSQ1	192.168.1.103	68	192.168.1.1	67	udp	-	0.044779	303	300	SF	-	0	Dd	1	331	1	328	(empty)
1258531939.613071	tP3DM6npTdj	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258532046.693816	Jb4jIDToo77	192.168.1.104	68	192.168.1.1	67	udp	-	0.002103	311	300	SF	-	0	Dd	1	339	1	328	(empty)
1258532143.457078	xvWLhxgUmj5	192.168.1.102	1170	192.168.1.1	53	udp	dns	0.068511	36	215	SF	-	0	Dd	1	64	1	243	(empty)
1258532203.657268	feNcvrZfDbf	192.168.1.104	1174	192.168.1.1	53	udp	dns	0.170962	36	215	SF	-	0	Dd	1	64	1	243	(empty)
1258532331.365294	aLsTcZJHAwa	192.168.1.1	5353	224.0.0.251	5353	udp	dns	0.100381	273	0	S0	-	0	D	2	329	0	0	(empty)
1258532331.365330	EK79I6iD5gl	fe80::219:e3ff:fee7:5d23	5353	ff02::fb	5353	udp	dns	0.100371	273	0	S0	-	0	D	2	369	0	0	(empty)
1258532404.734264	vLsf6ZHtak9	192.168.1.103	137	192.168.1.255	137	udp	dns	3.873818	350	0	S0	-	0	D	7	546	0	0	(empty)
1258532418.272517	Su3RwTCaHL3	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748891	350	0	S0	-	0	D	7	546	0	0	(empty)
1258532404.859431	rPM1dfJKPmj	192.168.1.103	138	192.168.1.255	138	udp	-	2.257840	348	0	S0	-	0	D	2	404	0	0	(empty)
1258532456.089023	4x5ezf34Rkh	192.168.1.102	1173	192.168.1.1	53	udp	dns	0.000267	33	497	SF	-	0	Dd	1	61	1	525	(empty)
1258532418.281002	mymcd8Veike	192.168.1.102	138	192.168.1.255	138	udp	-	2.248843	348	0	S0	-	0	D	2	404	0	0	(empty)
1258532525.592455	07mJRfg5RU5	192.168.1.1	5353	224.0.0.251	5353	udp	dns	0.099824	273	0	S0	-	0	D	2	329	0	0	(empty)
1258532525.592493	V6FODcWHWec	fe80::219:e3ff:fee7:5d23	5353	ff02::fb	5353	udp	dns	0.099813	273	0	S0	-	0	D	2	369	0	0	(empty)
1258532528.348891	H3qLO3SV0j	192.168.1.104	137	192.168.1.255	137	udp	dns	3.748895	350	0	S0	-	0	D	7	546	0	0	(empty)
1258532528.357385	rPqxmvEhfBb	192.168.1.104	138	192.168.1.255	138	udp	-	2.248339	348	0	S0	-	0	D	2	404	0	0	(empty)
1258532644.128655	VkSPS0xGKR	192.168.1.1	5353	224.0.0.251	5353	udp	-	-	-	-	S0	-	0	D	1	154	0	0	(empty)
1258532644.128680	qYIadwKn8wg	fe80::219:e3ff:fee7:5d23	5353	ff02::fb	5353	udp	-	-	-	-	S0	-	0	D	1	174	0	0	(empty)
1258532657.288677	AbCe0UeHRD6	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258532683.876479	4xkhfR2BeX2	192.168.1.103	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	240	0	0	(empty)
1258532824.338291	03rnFQ5hJ3f	192.168.1.104	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258533003.551468	3VNZpT9V3G8	192.168.1.102	68	192.168.1.1	67	udp	-	0.011807	301	300	SF	-	0	Dd	1	329	1	328	(empty)
1258533129.324984	JGyFmSAGkVj	192.168.1.103	137	192.168.1.255	137	udp	dns	3.748641	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533142.729062	jH5gXia1V2b	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748893	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533129.333980	rnymGcMKJa1	192.168.1.103	138	192.168.1.255	138	udp	-	2.248336	348	0	S0	-	0	D	2	404	0	0	(empty)
1258533142.737803	KEbhCATVhq6	192.168.1.102	138	192.168.1.255	138	udp	-	2.248086	348	0	S0	-	0	D	2	404	0	0	(empty)
1258533252.824915	43kp69mNH9h	192.168.1.104	137	192.168.1.255	137	udp	dns	3.764644	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533252.848161	6IrqIPLkMue	192.168.1.104	138	192.168.1.255	138	udp	-	2.249087	348	0	S0	-	0	D	2	404	0	0	(empty)
1258533406.310783	E3V7insZAf3	192.168.1.103	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	240	0	0	(empty)
1258533546.501981	1o9fdj2Mwzk	192.168.1.104	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258533745.340248	BwDhfT4ibLj	192.168.1.1	5353	224.0.0.251	5353	udp	-	-	-	-	S0	-	0	D	1	105	0	0	(empty)
1258533745.340270	xQ3F7WYDuc9	fe80::219:e3ff:fee7:5d23	5353	ff02::fb	5353	udp	-	-	-	-	S0	-	0	D	1	125	0	0	(empty)
1258533706.284625	xC73ngEP6t8	192.168.1.103	68	192.168.1.1	67	udp	-	0.011605	303	300	SF	-	0	Dd	1	331	1	328	(empty)
1258533766.050097	IxBAxd8IHQd	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258533853.790491	QHWe1hZptM5	192.168.1.103	137	192.168.1.255	137	udp	dns	3.748893	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533867.185568	HzzKOZy8Zl	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748900	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533827.650648	O6RgfULxXN3	192.168.1.104	68	192.168.1.1	67	udp	-	0.002141	311	300	SF	-	0	Dd	1	339	1	328	(empty)
1258533853.799477	U17UR8RLuIh	192.168.1.103	138	192.168.1.255	138	udp	-	2.248587	348	0	S0	-	0	D	2	404	0	0	(empty)
1258533867.194313	Z0o7i3H04Mb	192.168.1.102	138	192.168.1.255	138	udp	-	2.248337	348	0	S0	-	0	D	2	404	0	0	(empty)
1258533977.316663	mxs3TNKBBy1	192.168.1.104	137	192.168.1.255	137	udp	dns	3.748892	350	0	S0	-	0	D	7	546	0	0	(empty)
1258533977.325393	yLnPhusc1Fd	192.168.1.104	138	192.168.1.255	138	udp	-	2.248342	348	0	S0	-	0	D	2	404	0	0	(empty)
1258534152.488884	91kNv7QfCzi	192.168.1.102	1180	68.216.79.113	37	tcp	-	2.850214	0	0	S0	-	0	S	2	96	0	0	(empty)
1258534152.297748	LOurbPuyqk7	192.168.1.102	59040	192.168.1.1	53	udp	dns	0.189140	44	178	SF	-	0	Dd	1	72	1	206	(empty)
1258534161.354320	xDClpF8rSJf	192.168.1.102	1180	68.216.79.113	37	tcp	-	-	-	-	S0	-	0	S	1	48	0	0	(empty)
1258534429.059180	lpnjZjmVs05	192.168.1.103	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	240	0	0	(empty)
1258534488.491105	EEdJBMA9rCk	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258534578.255976	omP3BzwITql	192.168.1.103	137	192.168.1.255	137	udp	dns	3.764629	350	0	S0	-	0	D	7	546	0	0	(empty)
1258534582.490064	NnB6PYh0Zng	192.168.1.103	1190	192.168.1.1	53	udp	dns	0.068749	36	215	SF	-	0	Dd	1	64	1	243	(empty)
1258534591.642070	FVtn6tTYXr4	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748895	350	0	S0	-	0	D	7	546	0	0	(empty)
1258534545.219226	S5B6OZaxfKa	192.168.1.104	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258534578.280455	Gz2fEwEvO0a	192.168.1.103	138	192.168.1.255	138	udp	-	2.248587	348	0	S0	-	0	D	2	404	0	0	(empty)
1258534591.650809	hTD6LLqZ7Y7	192.168.1.102	138	192.168.1.255	138	udp	-	2.248337	348	0	S0	-	0	D	2	404	0	0	(empty)
1258534701.792887	zm9y9VwuS0i	192.168.1.104	137	192.168.1.255	137	udp	dns	3.748895	350	0	S0	-	0	D	7	546	0	0	(empty)
1258534701.800881	aJiKvjshkn2	192.168.1.104	138	192.168.1.255	138	udp	-	2.249081	348	0	S0	-	0	D	2	404	0	0	(empty)
1258534785.460075	sU2LS35B0Wc	192.168.1.102	68	192.168.1.1	67	udp	-	0.012542	301	300	SF	-	0	Dd	1	329	1	328	(empty)
1258534856.808007	gd5PK3GL6Q4	192.168.1.103	56940	192.168.1.1	53	udp	dns	0.000218	44	178	SF	-	0	Dd	1	72	1	206	(empty)
1258534856.809509	mkqyZaVMBzf	192.168.1.103	1191	68.216.79.113	37	tcp	-	8.963129	0	0	S0	-	0	S	3	144	0	0	(empty)
1258534970.336456	jlEzGSUZMMk	192.168.1.104	1186	68.216.79.113	37	tcp	-	3.024594	0	0	S0	-	0	S	2	96	0	0	(empty)
1258534970.334447	LD7p2nKzwUa	192.168.1.104	56041	192.168.1.1	53	udp	dns	0.000221	44	178	SF	-	0	Dd	1	72	1	206	(empty)
1258534979.376520	FikbEcyi5ud	192.168.1.104	1186	68.216.79.113	37	tcp	-	-	-	-	S0	-	0	S	1	48	0	0	(empty)
1258535150.337635	r1IqqKncAn1	192.168.1.103	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	240	0	0	(empty)
1258535262.273837	OCdMO0RlDKi	192.168.1.104	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258535302.768650	w7TADZKQmv7	192.168.1.103	137	192.168.1.255	137	udp	dns	3.748655	350	0	S0	-	0	D	7	546	0	0	(empty)
1258535316.098533	EhLt4Xfo998	192.168.1.102	137	192.168.1.255	137	udp	dns	3.748897	350	0	S0	-	0	D	7	546	0	0	(empty)
1258535302.777651	IZ5pW4ZoObi	192.168.1.103	138	192.168.1.255	138	udp	-	2.248077	348	0	S0	-	0	D	2	404	0	0	(empty)
1258535316.107272	DjK0mCmuZKc	192.168.1.102	138	192.168.1.255	138	udp	-	2.248339	348	0	S0	-	0	D	2	404	0	0	(empty)
1258535426.269094	j1nshHTZnc2	192.168.1.104	137	192.168.1.255	137	udp	dns	3.780124	350	0	S0	-	0	D	7	546	0	0	(empty)
1258535426.309819	HT5yJUMEaba	192.168.1.104	138	192.168.1.255	138	udp	-	2.247581	348	0	S0	-	0	D	2	404	0	0	(empty)
1258535488.214929	j5j8aWhnaBl	192.168.1.103	68	192.168.1.1	67	udp	-	0.019841	303	300	SF	-	0	Dd	1	331	1	328	(empty)
1258535580.253637	8F0S5E1XGh4	192.168.1.102	138	192.168.1.255	138	udp	-	-	-	-	S0	-	0	D	1	229	0	0	(empty)
1258535653.062408	YW7idMRahdb	192.168.1.104	1191	65.54.95.64	80	tcp	http	0.050465	173	297	RSTO	-	0	ShADdfR	5	381	3	425	(empty)
1258535650.506019	5txwc6aKNFe	192.168.1.104	56749	192.168.1.1	53	udp	dns	0.044610	30	94	SF	-	0	Dd	1	58	1	122	(empty)
1258535656.471265	HcFUvhy5Wf6	192.168.1.104	1193	65.54.95.64	80	tcp	http	0.050215	195	296	RSTO	-	0	ShADdfR	5	403	3	424	(empty)
1258535656.524478	TsaAKxHC8yh	192.168.1.104	1194	65.54.95.64	80	tcp	http	0.109682	194	21053	RSTO	-	0	ShADdfR	8	522	17	21741	(empty)
1258535652.794076	M6vDMlNtAok	192.168.1.104	52125	192.168.1.1	53	udp	dns	0.266791	44	200	SF	-	0	Dd	1	72	1	228	(empty)
1258535658.712360	Hiphu7fLcC5	192.168.1.104	1195	65.54.95.64	80	tcp	http	0.079452	173	297	RSTO	-	0	ShADdfR	5	381	3	425	(empty)
1258535655.387448	GH3I4uYo0l1	192.168.1.104	64790	192.168.1.1	53	udp	dns	0.042968	42	179	SF	-	0	Dd	1	70	1	207	(empty)
1258535650.551483	04xC2aCJ5i8	192.168.1.104	137	192.168.1.255	137	udp	dns	4.084184	300	0	S0	-	0	D	6	468	0	0	(empty)
1258535666.147439	g6mt9RBZkw	192.168.1.104	1197	65.54.95.64	80	tcp	http	0.049966	173	297	RSTO	-	0	ShADdfR	5	381	3	425	(empty)
1258535697.963212	I8ePTueT9Aj	192.168.1.102	1188	212.227.97.133	80	tcp	http	0.898191	1121	342	SF	-	0	ShADadfF	5	1329	5	550	(empty)
1258535698.862885	lZ58OyvEYY3	192.168.1.102	1189	87.106.1.47	80	tcp	http	0.880456	1118	342	SF	-	0	ShADadfF	5	1326	5	546	(empty)
1258535699.744831	D2ERJCFZD1e	192.168.1.102	1190	87.106.1.89	80	tcp	http	0.914934	1118	342	SF	-	0	ShADadfF	5	1326	5	550	(empty)
1258535696.159584	wIEQmZxJy19	192.168.1.102	1187	192.168.1.1	53	udp	dns	0.068537	36	215	SF	-	0	Dd	1	64	1	243	(empty)
1258535700.662505	HqW58gj5856	192.168.1.102	1191	87.106.12.47	80	tcp	http	0.955409	1160	1264	SF	-	0	ShADadfF	5	1368	5	1472	(empty)
1258535701.622151	zCr8XZTRcvh	192.168.1.102	1192	87.106.12.77	80	tcp	http	0.514927	1222	367	SF	-	0	ShADadfF	6	1470	6	615	(empty)
1258535650.499268	dNSfUrlTwq3	192.168.1.104	68	255.255.255.255	67	udp	-	-	-	-	S0	-	0	D	1	328	0	0	(empty)
1258535609.607942	qomqwkg9Ddg	192.168.1.104	68	192.168.1.1	67	udp	-	40.891774	311	600	SF	-	0	Dd	1	339	2	656	(empty)
1258535707.137448	YUUhPmf1G4c	192.168.1.102	1194	87.106.66.233	80	tcp	http	0.877448	1128	301	SF	-	0	ShADadfF	5	1336	5	505	(empty)
1258535702.138078	yH3dkqFJE8	192.168.1.102	1193	87.106.13.61	80	tcp	-	3.061084	0	0	S0	-	0	S	2	96	0	0	(empty)
1258535708.016137	I60NOMgOQxj	192.168.1.102	1195	87.106.9.29	80	tcp	http	0.876205	1126	342	SF	-	0	ShADadfF	5	1334	5	550	(empty)
1258535655.431418	jM8ATYNKqZg	192.168.1.104	1192	65.55.184.16	80	tcp	http	59.712557	172	262	RSTR	-	0	ShADdr	4	340	3	390	(empty)
1258535710.855364	YmvKAMrJ6v9	192.168.1.102	1196	192.168.1.1	53	udp	dns	0.013042	36	215	SF	-	0	Dd	1	64	1	243	(empty)
1258535660.158200	WfzxgFx2lWb	192.168.1.104	1196	65.55.184.16	443	tcp	ssl	67.887666	57041	8510	RSTR	-	0	ShADdar	54	59209	26	9558	(empty)
#close	2014-05-23-18-02-35)__";

} // namspace <anonymous>

FIXTURE_SCOPE(zeek_reader_tests, fixtures::deterministic_actor_system)

TEST(zeek data parsing) {
  using namespace std::chrono;
  data d;
  CHECK(zeek_parse(boolean_type{}, "T", d));
  CHECK(d == true);
  CHECK(zeek_parse(integer_type{}, "-49329", d));
  CHECK(d == integer{-49329});
  CHECK(zeek_parse(count_type{}, "49329"s, d));
  CHECK(d == count{49329});
  CHECK(zeek_parse(timestamp_type{}, "1258594163.566694", d));
  auto ts = duration_cast<timespan>(double_seconds{1258594163.566694});
  CHECK(d == timestamp{ts});
  CHECK(zeek_parse(timespan_type{}, "1258594163.566694", d));
  CHECK(d == ts);
  CHECK(zeek_parse(string_type{}, "\\x2afoo*"s, d));
  CHECK(d == "*foo*");
  CHECK(zeek_parse(address_type{}, "192.168.1.103", d));
  CHECK(d == *to<address>("192.168.1.103"));
  CHECK(zeek_parse(subnet_type{}, "10.0.0.0/24", d));
  CHECK(d == *to<subnet>("10.0.0.0/24"));
  CHECK(zeek_parse(port_type{}, "49329", d));
  CHECK(d == port{49329, port::unknown});
  CHECK(zeek_parse(vector_type{integer_type{}}, "49329", d));
  CHECK(d == vector{49329});
  CHECK(zeek_parse(set_type{string_type{}}, "49329,42", d));
  CHECK(d == set{"49329", "42"});
}

TEST(zeek reader) {
  using reader_type = format::zeek::reader;
  reader_type reader{defaults::system::table_slice_type,
                     std::make_unique<std::istringstream>(
                       std::string{conn_log_100_events})};
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  auto [err, num] = reader.read(100, 20, add_slice);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(num, 100);
}

FIXTURE_SCOPE_END()

FIXTURE_SCOPE(zeek_writer_tests, fixtures::events)

TEST(zeek writer) {
  // Sanity check some Zeek events.
  CHECK_EQUAL(zeek_conn_log.size(), 20u);
  CHECK_EQUAL(zeek_conn_log.front().type().name(), "zeek.conn");
  auto record = caf::get_if<vector>(&zeek_conn_log.front().data());
  REQUIRE(record);
  REQUIRE_EQUAL(record->size(), 20u);
  CHECK_EQUAL(record->at(6), data{"udp"}); // one after the conn record
  CHECK_EQUAL(record->back(), data{set{}}); // table[T] is actually a set
  // Perform the writing.
  auto dir = path{"vast-unit-test-zeek"};
  auto guard = caf::detail::make_scope_guard([&] { rm(dir); });
  format::zeek::writer writer{dir};
  for (auto& e : zeek_conn_log)
    if (!writer.write(e))
      FAIL("failed to write event");
  for (auto& e : zeek_http_log)
    if (!writer.write(e))
      FAIL("failed to write event");
  CHECK(exists(dir / zeek_conn_log[0].type().name() + ".log"));
  CHECK(exists(dir / zeek_http_log[0].type().name() + ".log"));
}

FIXTURE_SCOPE_END()
