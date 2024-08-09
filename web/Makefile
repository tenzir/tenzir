# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

POETRY := $(shell command -v poetry 2> /dev/null)
RSCRIPT := $(shell command -v Rscript 2> /dev/null)
QUARTO := $(shell command -v quarto 2> /dev/null)

CONTRIBUTING_DIR := docs/contribution

QUARTO_INPUTS := $(shell find docs blog -name '*.qmd')
QUARTO_MD_OUTPUTS := $(patsubst %.qmd, %.md, $(QUARTO_INPUTS))
QUARTO_FILES_OUTPUTS := $(patsubst %.qmd, %_files, $(QUARTO_INPUTS))

RFC_INPUT := ../rfc/README.md
RFC_OUTPUT := $(CONTRIBUTING_DIR)/rfc.md

all: dot-github rfc

check-submodule:
	@if ! [ -f .github/.git ]; then \
	  printf "initializing .github submodule\n" >&2; \
	  git submodule update --init .github; \
	fi

dot-github: \
  check-submodule \
  $(CONTRIBUTING_DIR)/code-of-conduct.md \
  $(CONTRIBUTING_DIR)/coding-style.md \
  $(CONTRIBUTING_DIR)/security.md \
  $(CONTRIBUTING_DIR)/workflow.md

$(CONTRIBUTING_DIR)/code-of-conduct.md:
	@printf -- "---\nsidebar_position: 0\n---\n\n" > $@
	@cat .github/CODE-OF-CONDUCT.md >> $@

$(CONTRIBUTING_DIR)/coding-style.md:
	@printf -- "---\nsidebar_position: 2\n---\n\n" > $@
	@cat .github/$(@F) >> $@

$(CONTRIBUTING_DIR)/security.md:
	@printf -- "---\nsidebar_position: 10\n---\n\n" > $@
	@cat .github/SECURITY.md >> $@

$(CONTRIBUTING_DIR)/workflow.md:
	@printf -- "---\nsidebar_position: 1\n---\n\n" > $@
	@cat .github/$(@F) >> $@

rfc: $(RFC_OUTPUT)

$(RFC_OUTPUT): $(RFC_INPUT)
	@printf -- "---\nsidebar_position: 9\n---\n" > $@
	@cat $< >> $@

define INSTALL_R_PACKAGES
if ("pak" %in% rownames(installed.packages()) == FALSE)
  install.packages("pak", repos = "http://cran.us.r-project.org")
require(pak)
pak::local_install_dev_deps(upgrade = FALSE)
endef

# Export the Make variable as shell variable so that Make doesn't try to
# interpret it as a command sequence.
export INSTALL_R_PACKAGES

pak:
ifndef RSCRIPT
	$(error "Rscript not in $$PATH; please install system-wide")
else
	@Rscript -e "$$INSTALL_R_PACKAGES"
endif

poetry:
ifndef POETRY
	$(error "poetry not in $$PATH; please install system-wide")
else
	@poetry install --no-root
	@poetry	run python -m bash_kernel.install
endif

%.md: %.qmd
ifndef QUARTO
	$(error "quarto not in $$PATH; please install system-wide")
else
	poetry run quarto render $<
endif

quarto: poetry pak $(QUARTO_MD_OUTPUTS)

clean:
	rm -f $(CHANGELOG_OUTPUT)
	rm -f $(RFC_OUTPUT)
	rm -f $(CONTRIBUTING_DIR)/code-of-conduct.md
	rm -f $(CONTRIBUTING_DIR)/coding-style.md
	rm -f $(CONTRIBUTING_DIR)/security.md
	rm -f $(CONTRIBUTING_DIR)/workflow.md
	rm -rf $(QUARTO_MD_OUTPUTS) $(QUARTO_FILES_OUTPUTS)
