POETRY := $(shell command -v poetry 2> /dev/null)
QUARTO := $(shell command -v quarto 2> /dev/null)

QUARTO_INPUTS := $(shell find . -name '*.qmd')
QUARTO_MD_OUTPUTS := $(patsubst %.qmd, %.md, $(QUARTO_INPUTS))
QUARTO_FILES_OUTPUTS := $(patsubst %.qmd, %_files, $(QUARTO_INPUTS))

all: poetry quarto

poetry:
ifndef POETRY
	$(error "poetry not in $$PATH; please install system-wide")
else
	@poetry install --no-root
	#@poetry	run python -m bash_kernel.install
endif

%.md: %.qmd
ifndef QUARTO
	$(error "quarto not in $$PATH; please install system-wide")
else
	poetry run quarto render $<
endif

quarto: $(QUARTO_MD_OUTPUTS)

clean:
	rm -rf $(QUARTO_MD_OUTPUTS) $(QUARTO_FILES_OUTPUTS)
