Operator invocations that directly use parenthesis but continue after the
closing parenthesis are no longer rejected. For example, `where (x or y) and z`
is now being parsed correctly.
