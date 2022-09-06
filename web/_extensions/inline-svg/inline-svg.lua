n = 0

function Image(el)
    if el.src == "" then
      return el
    end
    local ext = ".svg"
    if el.src:lower():sub(-#ext) == ext then
        n = n + 1
        local id = "Svg" .. n
        local import = "import " .. id .. " from './" .. el.src .. "';"
        local svg = "<" .. id .. " />"
        import_inline = pandoc.RawInline("html", import .. "\n")
        svg_inline = pandoc.RawInline("html", svg)
        return {import_inline, "\n", svg_inline}
    end
    return el
end
