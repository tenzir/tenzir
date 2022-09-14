function CodeBlock(code_block)
    if code_block.attributes["code-fold"] == "true" then
        local open = "<details><summary>Code</summary>\n"
        local close = "\n</details>"
        local open_block = pandoc.RawBlock("html", open)
        local close_block = pandoc.RawBlock("html", close)
        return {open_block, code_block, close_block}
    end
    return code_block
end
