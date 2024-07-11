We fixed a rare crash when one of multiple `subscribe` operators for the same
topic disconnected while at least one of the other subscribers was overwhelmed
and asked for corresponding publishers to throttle.
