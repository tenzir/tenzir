@load base/frameworks/communication

Communication::nodes["vast"] = [$host = 192.150.187.38,
                                $p = 42000/tcp,
                                $events = /^.*$/,
                                #$retry=10secs,
                                $connect=T];
