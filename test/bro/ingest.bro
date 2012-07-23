@load base/frameworks/communication

Communication::nodes["vast"] = [$host = 127.0.0.1,
                                $p = 42000/tcp,
                                $events = /^.*$/,
                                #$retry=10secs,
                                $connect=T];
