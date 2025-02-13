We fixed a bug in `load_amqp` and `save_amqp` that prevented the node from
starting if they were used in a pipeline configured as code and failed to connect.
