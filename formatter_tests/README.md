# TQL Pretty Printer Formatter Test Suite

This directory contains comprehensive test cases for the TQL Pretty Printer Formatter, demonstrating various language features and formatting capabilities.

## Test Cases

| Test File | Description | Features Tested |
|-----------|-------------|-----------------|
| `01_simple_pipeline.tql` | Basic pipeline operations | Pipe conversion, operator spacing, simple queries |
| `02_nested_structures.tql` | Complex data structures | Record indentation, list formatting, control flow |
| `03_comments_and_lets.tql` | Comments and variable declarations | Comment preservation, let statement formatting |
| `04_comprehensive.tql` | Multi-operator pipeline | Complex expressions, aggregations, sorting |
| `05_control_flow.tql` | Conditional logic | Nested if/else statements, complex conditions |
| `06_functions_and_methods.tql` | Function calls and methods | Lambda expressions, method chaining |
| `07_operators_and_expressions.tql` | Arithmetic and logic | Operator precedence, conditional expressions |
| `08_string_formatting.tql` | String handling | Format strings, raw strings, string functions |
| `09_complex_pipeline.tql` | Advanced pipeline | Comments, aggregations, sorting, filtering |
| `10_nested_records.tql` | Deep nesting | Multi-level records, arrays within records |

## Usage

To test the formatter on any file:
```bash
./build/clang/debug/bin/tenzir --format -f formatter_tests/[test_file].tql
```

To compare with expected output:
```bash
./build/clang/debug/bin/tenzir --format -f formatter_tests/[test_file].tql | diff - formatter_tests/[test_file].txt
```

## Format Features Demonstrated

### ✅ Core Formatting
- **Pipe to newline conversion**: `|` operators become newlines
- **Operator spacing**: Proper spacing around `==`, `>`, `=`, etc.
- **Keyword spacing**: Space after `if`, `else`, `let`, etc.

### ✅ Structure Formatting  
- **Record indentation**: 2-space indentation for nested records
- **List formatting**: Proper comma spacing and line breaks
- **Brace alignment**: Opening/closing braces properly positioned

### ✅ Advanced Features
- **Comment preservation**: Block and line comments maintained
- **Control flow**: `if`/`else` statements with proper nesting
- **String handling**: Format strings, raw strings, escape sequences
- **Function calls**: Method chaining and lambda expressions

### ✅ Pipeline Operations
- **Operator separation**: Each pipeline operator on its own line
- **Complex expressions**: Multi-part calculations properly spaced
- **Variable references**: `$variable` formatting preserved

## Example

**Input (messy):**
```tql
from "data.json"|where name=="John"|select name,age,email|head 10
```

**Output (formatted):**
```tql
from "data.json"
where name == "John"
select name age email
head 10
```

The formatter successfully transforms unreadable, compressed TQL code into clean, professional, and maintainable query language that follows consistent style guidelines.