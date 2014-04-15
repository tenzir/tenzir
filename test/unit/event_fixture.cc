#include "event_fixture.h"
#include "vast/regex.h"

using namespace vast;

event_fixture::event_fixture()
{
  events.emplace_back();

  events.emplace_back(
      event{
        invalid,
        true,
        -1,
        9u,
        123.456789,
        "bar",
        "12345678901234567890",
        table{{22, "ssh"}, {25, "smtp"}, {80, "http"}},
        regex{"[0-9][a-z]?\\w+$"},
        record{invalid, true, -42, 4711u},
        address{"192.168.0.1"},
        address{"2001:db8:0000:0000:0202:b3ff:fe1e:8329"},
        prefix{address{"10.1.33.22"}, 8},
        port{139, port::tcp}
      });

  events.emplace_back(
      event{
        false,
        1000000,
        123456789u,
        -123.456789,
        "baz\"qux",
        {"baz\0", 4},
        "Das ist also des Pudels Kern.",
        invalid,
        987.654321,
        -12081983,
        regex{"[0-9][a-z]?\\w+$"},
        time_point{now()},
        time_range{now().since_epoch()},
        address{"ff01::1"},
        address{"2001:db8:0000:0000:0202:b3ff:fe1e:8329"},
        prefix{address{"ff00::"}, 16},
        port{53, port::udp}
      });
}
