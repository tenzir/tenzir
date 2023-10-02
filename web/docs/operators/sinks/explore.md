# explore

Renders pipeline data in a terminal user interface.

## Synopsis

```
explore [-f|--fullscreen <bool>] [-h|--height <int>] [-w|--width <int>]
        [-n|--navigator-position <string>] [-N|--navigator-auto-hide]
        [-T|--hide-types]
```

## Description

The `explore` operator renders the data in the pipeline in a terminal user
interface (TUI).

Press `?` to get an help screen that shows available key bindings.

Use the arrow keys or `j`/`k`/`h`/`l` to navigate through focusable components.

You can also use the mouse to scroll vertically and horizontally by moving the
pointer relative to the center of the TUI in a specific direction. For example,
moving the point to the right from the center point scrolls to the right.

The implementation uses the [FTXUI](https://arthursonzogni.github.io/FTXUI/)
library for functional terminal interfaces in C++.

### `-f|--fullscreen`

Use the full terminal screen, as opposed to fitting the output into the
terminal.

### `-h|--height <int>`

The height of the TUI. Only actionable when `-w|--width` is also present.

### `-w|--width <int>`

The width of the TUI. Only actionable when `-h|--height` is also present.

### `-n|--navigator-position <string>`

The position of the navigator pane. Must be one of `left`, `right`, `top`,
`bottom`.

Defaults to `top`.

### `-N|--navigator-auto-hide`

Disables the navigator pane when there is exactly *one* schema.

Press `n` to manually toggle between showing/hiding the navigator.

### `-T|--hide-types`

Do not show the type names in the header column.

Press `t` to toggle between showing/hiding type annotations.

## Example

Show the pipeline results in a TUI:

```bash
explore
```

Show the pipeline results in fullscreen mode:

```bash
explore -f
```

Show the pipeline results a fixed window frame:

```bash
explore -w 80 -h 20
```

Put the navigator at the bottom and hide the column types:

```bash
explore --navigator-position bottom --hide-types
```
