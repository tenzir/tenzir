# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CHANGELOG_INPUT := ../CHANGELOG.md
CHANGELOG_OUTPUT := src/pages/changelog.md

DOT_GITHUB_OUTPUT_DIR := docs/develop-vast/contributing

NOTEBOOK_INPUT_DIR := ../examples/notebooks
NOTEBOOK_OUTPUT_DIR := docs/try-vast
NOTEBOOK_INPUTS := $(wildcard $(NOTEBOOK_INPUT_DIR)/*.md)
NOTEBOOK_OUTPUTS := $(addprefix $(NOTEBOOK_OUTPUT_DIR)/, $(notdir $(NOTEBOOK_INPUTS)))
NOTEBOOK_DOCS_INPUT := $(NOTEBOOK_INPUT_DIR)/README.md
NOTEBOOK_DOCS_OUTPUT := $(DOT_GITHUB_OUTPUT_DIR)/notebooks.md

all: changelog notebooks dot-github

changelog: $(CHANGELOG_OUTPUT)

$(CHANGELOG_OUTPUT): $(CHANGELOG_INPUT)
	cp -f $< $@

.github:
	git clone https://github.com/tenzir/.github.git

dot-github: \
  $(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md \
  $(DOT_GITHUB_OUTPUT_DIR)/coding-style.md \
  $(DOT_GITHUB_OUTPUT_DIR)/security.md \
  $(DOT_GITHUB_OUTPUT_DIR)/workflow.md

$(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md: .github
	@echo -e "---\nsidebar_position: 0\n---\n" > $@
	@cat .github/CODE-OF-CONDUCT.md >> $@

$(DOT_GITHUB_OUTPUT_DIR)/coding-style.md: .github
	@echo -e "---\nsidebar_position: 2\n---\n" > $@
	@cat .github/$(@F) >> $@

$(DOT_GITHUB_OUTPUT_DIR)/security.md: .github
	@echo -e "---\nsidebar_position: 10\n---\n" > $@
	@cat .github/SECURITY.md >> $@

$(DOT_GITHUB_OUTPUT_DIR)/workflow.md: .github
	@echo -e "---\nsidebar_position: 1\n---\n" > $@
	@cat .github/$(@F) >> $@

notebooks: $(NOTEBOOK_DOCS_OUTPUT) $(NOTEBOOK_OUTPUTS)

$(NOTEBOOK_DOCS_OUTPUT): $(NOTEBOOK_DOCS_INPUT)
	@echo -e "---\nsidebar_position: 6\n---\n" > $@
	@cat $< >> $@

$(NOTEBOOK_OUTPUTS): $(NOTEBOOK_INPUTS)
	@$(MAKE) -C $(NOTEBOOK_INPUT_DIR)
	cp -r $(NOTEBOOK_INPUT_DIR)/output/md/* $(NOTEBOOK_OUTPUT_DIR)

clean:
	rm -f $(CHANGELOG_OUTPUT)
	$(MAKE) -C $(NOTEBOOK_INPUT_DIR) clean
	git clean -f -d -x $(NOTEBOOK_OUTPUT_DIR)
	rm -rf .github
	rm $(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/coding-style.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/security.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/workflow.md
