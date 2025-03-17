We fixed a regression introduced in v4.29.2 that caused strings passed as
options to the `from_fluent_bit` and `to_fluent_bit` operators to incorrectly be
surrounded by double quotes.
