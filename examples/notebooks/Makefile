# This Makefile runs notebooks and converts them into a different output format.
# The notebook execution is a two-step process:
#
# 1. Run then notebook and capture outputs in an *.ipynb file.
# 2. Translate the *.ipynb file to Markdown
#
# Ideally, we would go directly to the Markdown output, but jupytext only
# captures outputs when converting to *.ipynb. So we use nbconvert afterwards to
# translate the *.ipynb files to Markdown.

VENV := venv
BUILD := output
NOTEBOOKS = $(wildcard *.md)
MD = $(addprefix $(BUILD)/md/, $(NOTEBOOKS))
IPYNB = $(addprefix $(BUILD)/ipynb/, $(NOTEBOOKS:.md=.ipynb))

all: md

md: $(MD)

ipynb: $(IPYNB)

$(IPYNB) $(MD): $(VENV)

$(VENV):
	@python3 -m venv $(VENV)
	@[ -n "$$FISH_VERSION" ] && \
    . $(VENV)/bin/activate.fish || \
    . $(VENV)/bin/activate && \
		cd $(VENV) && \
		pip install nbconvert jupytext ipykernel bash_kernel && \
		python -m bash_kernel.install

$(BUILD)/ipynb/%.ipynb: %.md
	@echo "==> $@"
	@mkdir -p $(BUILD)/ipynb
	@[ -n "$$FISH_VERSION" ] && \
    . $(VENV)/bin/activate.fish || \
    . $(VENV)/bin/activate && \
    jupytext --to notebook --execute -o $@ $<

$(BUILD)/md/%.md: $(BUILD)/ipynb/%.ipynb
	@echo "==> $@"
	@mkdir -p $(BUILD)/md
	@[ -n "$$FISH_VERSION" ] && \
    . $(VENV)/bin/activate.fish || \
    . $(VENV)/bin/activate && \
	  jupyter nbconvert --to markdown --output-dir $(BUILD)/md $<

clean:
	rm -rf $(VENV)
	rm -rf $(BUILD)
