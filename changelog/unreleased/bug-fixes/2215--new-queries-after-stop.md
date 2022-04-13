VAST servers no longer accept queries after initiating shutdown. This fixes a
potential infinite hang if new queries were coming in faster than we were able
to process them.
