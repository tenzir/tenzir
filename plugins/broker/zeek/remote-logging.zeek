# This script activates Broker communication such that it becomes possible to
# the stream of events that Zeek publishes.
#
# In particular, since Log::enable_remote_logging is T by default, this means
# that Zeek publishes logs to the topic `zeek/logs/`.

@load base/frameworks/broker

# Disable logging to local files.
redef Log::enable_local_logging = F;

event zeek_init() 
  {
  Broker::listen(Broker::default_listen_address, Broker::default_port,
                 Broker::default_listen_retry);
  print(fmt("Broker listening on %s:%s", Broker::default_listen_address,
                                         Broker::default_port));
  }
