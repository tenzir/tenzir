event bro::conn
(
  ts: time,
  duration: interval,
  orig_h: addr,
  resp_h: addr,
  service: string,
  orig_p: port,
  resp_p: port,
  proto: string,
  orig_bytes: count,
  resp_bytes: count,
  state: string,
  direction: string,
  addl: string &optional
)
