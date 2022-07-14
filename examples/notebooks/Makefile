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

all: markdown ipynb

md: $(MD)

ipynb: $(IPYNB)

$(IPYNB) $(MD): $(VENV)

$(VENV):
	@python3 -m venv $(VENV)
	@[ -n "$$FISH_VERSION" ] && \
    source $(VENV)/bin/activate.fish || \
    source $(VENV)/bin/activate && \
		cd $(VENV) && \
		pip install jupytext ipykernel bash_kernel && \
		python -m bash_kernel.install && \
		git clone https://github.com/jupyter/nbconvert.git && \
		cd nbconvert && \
		pip install -e .

$(BUILD)/ipynb/%.ipynb: %.md
	@echo "==> $@"
	@mkdir -p $(BUILD)/ipynb
	@[ -n "$$FISH_VERSION" ] && \
    source $(VENV)/bin/activate.fish || \
    source $(VENV)/bin/activate && \
    jupytext --to notebook --execute -o $@ $<

$(BUILD)/md/%.md: $(BUILD)/ipynb/%.ipynb
	@echo "==> $@"
	@mkdir -p $(BUILD)/md
	@[ -n "$$FISH_VERSION" ] && \
    source $(VENV)/bin/activate.fish || \
    source $(VENV)/bin/activate && \
	  jupyter nbconvert --to markdown --output-dir $(BUILD)/md $<

clean:
	rm -rf $(VENV)
	rm -rf $(BUILD)
