# This Makefile runs notebooks and converts them into a different output format.
# The notebook execution is a two-step process:
#
# 1. Run then notebook and capture outputs in an *.ipynb file.
# 2. Translate the *.ipynb file to Markdown
#
# Ideally, we would go directly to the Markdown output, but jupytext only
# captures outputs when converting to *.ipynb. So we use nbconvert afterwards to
# translate the *.ipynb files to Markdown.

POETRY := $(shell command -v poetry 2> /dev/null)
BUILD := output
NOTEBOOKS = $(wildcard *.md)
MD = $(addprefix $(BUILD)/md/, $(NOTEBOOKS))
IPYNB = $(addprefix $(BUILD)/ipynb/, $(NOTEBOOKS:.md=.ipynb))

all: md

md: $(MD)

ipynb: $(IPYNB)

$(IPYNB) $(MD): poetry

poetry:
ifndef POETRY
	$(error "poetry not in $$PATH; please install system-wide")
endif
	@poetry install
	@poetry	run python -m bash_kernel.install

$(BUILD)/ipynb/%.ipynb: %.md
	@echo "==> $@"
	@mkdir -p $(BUILD)/ipynb
	@poetry run jupytext --to notebook --execute -o $@ $<

$(BUILD)/md/%.md: $(BUILD)/ipynb/%.ipynb
	@echo "==> $@"
	@mkdir -p $(BUILD)/md
	@poetry run jupyter nbconvert --to markdown --output-dir $(BUILD)/md $<

clean:
	rm -rf $(BUILD)
