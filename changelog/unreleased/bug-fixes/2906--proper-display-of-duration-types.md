We fixed incorrect printing of human-readable durations in some edge cases.
E.g., the value 1.999s was rendered as 1.1s instead of the expected 2.0s. This
bug affected the JSON and CSV export formats, and all durations printed in log
messages or the status command.