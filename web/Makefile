# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CHANGELOG_INPUT := ../CHANGELOG.md
CHANGELOG_OUTPUT := src/pages/changelog.md

CONTRIBUTING_DIR := docs/develop-vast/contributing

NOTEBOOK_INPUT_DIR := ../examples/notebooks
NOTEBOOK_OUTPUT_DIR := docs/try-vast
NOTEBOOK_INPUTS := $(wildcard $(NOTEBOOK_INPUT_DIR)/*.md)
NOTEBOOK_OUTPUTS := $(addprefix $(NOTEBOOK_OUTPUT_DIR)/, $(notdir $(NOTEBOOK_INPUTS)))
NOTEBOOK_DOCS_INPUT := $(NOTEBOOK_INPUT_DIR)/README.md
NOTEBOOK_DOCS_OUTPUT := $(CONTRIBUTING_DIR)/notebooks.md

QUARTO_INPUTS := $(shell find . -type f -name "*.qmd" -exec dirname '{}' \; | xargs -I{} find "{}" -type f -not -name "*.md")

RFC_INPUT := ../rfc/README.md
RFC_OUTPUT := $(CONTRIBUTING_DIR)/rfc.md

all: changelog notebooks dot-github rfc quarto

changelog: $(CHANGELOG_OUTPUT)

$(CHANGELOG_OUTPUT): $(CHANGELOG_INPUT)
	cp -f $< $@

.github:
	git clone https://github.com/tenzir/.github.git

dot-github: \
  $(CONTRIBUTING_DIR)/code-of-conduct.md \
  $(CONTRIBUTING_DIR)/coding-style.md \
  $(CONTRIBUTING_DIR)/security.md \
  $(CONTRIBUTING_DIR)/workflow.md

$(CONTRIBUTING_DIR)/code-of-conduct.md: .github
	@printf -- "---\nsidebar_position: 0\n---\n" > $@
	@cat .github/CODE-OF-CONDUCT.md >> $@

$(CONTRIBUTING_DIR)/coding-style.md: .github
	@printf -- "---\nsidebar_position: 2\n---\n" > $@
	@cat .github/$(@F) >> $@

$(CONTRIBUTING_DIR)/security.md: .github
	@printf -- "---\nsidebar_position: 10\n---\n" > $@
	@cat .github/SECURITY.md >> $@

$(CONTRIBUTING_DIR)/workflow.md: .github
	@printf -- "---\nsidebar_position: 1\n---\n" > $@
	@cat .github/$(@F) >> $@

rfc: $(RFC_OUTPUT)

$(RFC_OUTPUT): $(RFC_INPUT)
	@printf -- "---\nsidebar_position: 9\n---\n" > $@
	@cat $< >> $@

notebooks: $(NOTEBOOK_DOCS_OUTPUT) $(NOTEBOOK_OUTPUTS)

$(NOTEBOOK_DOCS_OUTPUT): $(NOTEBOOK_DOCS_INPUT)
	@printf -- "---\nsidebar_position: 6\n---\n" > $@
	@cat $< >> $@

$(NOTEBOOK_OUTPUTS): $(NOTEBOOK_INPUTS)
	@$(MAKE) -C $(NOTEBOOK_INPUT_DIR)
	cp -r $(NOTEBOOK_INPUT_DIR)/output/md/* $(NOTEBOOK_OUTPUT_DIR)

quarto-r-deps:
	@Rscript -e 'if("pak" %in% rownames(installed.packages()) == FALSE) { install.packages("pak", repos = "http://cran.us.r-project.org") }'
	@Rscript -e 'require(pak); pak::local_install_dev_deps()'

quarto: quarto-r-deps $(QUARTO_INPUTS)
	@quarto render

clean:
	rm -f $(CHANGELOG_OUTPUT)
	$(MAKE) -C $(NOTEBOOK_INPUT_DIR) clean
	git clean -f -d -x $(NOTEBOOK_OUTPUT_DIR)
	rm -rf .github
	rm $(RFC_OUTPUT)
	rm $(CONTRIBUTING_DIR)/code-of-conduct.md
	rm $(CONTRIBUTING_DIR)/coding-style.md
	rm $(CONTRIBUTING_DIR)/security.md
	rm $(CONTRIBUTING_DIR)/workflow.md
