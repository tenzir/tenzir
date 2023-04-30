# tui

Renders pipeline data in a terminal user interface (TUI).

## Synopsis

```
tui [-f|--fullscreen=<bool>] [-h|--height=<int>] [-w|--width=<int>]
```

## Description

The `tui` operator renders the data in the pipeline in a terminal user interface
(TUI).

Use the arrow keys or `j`/`k`/`h`/`l` to navigate through the cells. You can
also use the mouse to scroll vertically and horizontally, but moving the pointer
relative off the center of the TUI in a specific direction. For example, moving
the point to the right from the center point scrolls to the right.

Press `?` to get an interactive help.

The implementation uses the [FTXUI](https://arthursonzogni.github.io/FTXUI/)
library for functional terminal interfaces in C++.

### `-f|--fullscreen`

Renders the TUI in fullscreen mode, as opposed to fitting it into the terminal.

### `-h|--height=<int>`

The height of the TUI. Only actionable when `-w|--width` is also present.

Defaults to 0, which means automatic detection.

### `-w|--width=<int>`

The width of the TUI. Only actionable when `-h|--height` is also present.

Defaults to 0, which means automatic detection.

## Example

Show the pipeline results in a TUI:

```bash
tui
```

Show the pipeline results in a TUI, in fullscreen mode:

```bash
tui -f true
```

Show the pipeline results in a TUI, in a fixed window frame:

```bash
tui -w 80 -h 20
```
