We fixed various edge cases in parsers where values would not be properly parsed
as typed data and were stored as plain text instead. No input data was lost, but
no valuable type information was gained either.