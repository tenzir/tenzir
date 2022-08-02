# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CHANGELOG_INPUT := ../CHANGELOG.md
CHANGELOG_OUTPUT := src/pages/changelog.md

NOTEBOOK_INPUT_DIR := ../examples/notebooks
NOTEBOOK_OUTPUT_DIR := docs/try-vast
NOTEBOOK_INPUTS := $(wildcard $(NOTEBOOK_INPUT_DIR)/*.md)
NOTEBOOK_OUTPUTS := $(addprefix $(NOTEBOOK_OUTPUT_DIR)/, $(notdir $(NOTEBOOK_INPUTS)))

DOT_GITHUB_OUTPUT_DIR := docs/develop-vast/contributing

all: changelog notebooks dot-github

changelog: $(CHANGELOG_OUTPUT)

$(CHANGELOG_OUTPUT): $(CHANGELOG_INPUT)
	cp -f $< $@

notebooks: $(NOTEBOOK_OUTPUTS)

$(NOTEBOOK_OUTPUTS): $(NOTEBOOK_INPUTS)
	@$(MAKE) -C $(NOTEBOOK_INPUT_DIR) md
	cp -r $(NOTEBOOK_INPUT_DIR)/output/md/* $(NOTEBOOK_OUTPUT_DIR)

.github:
	git clone https://github.com/tenzir/.github.git

dot-github: \
  $(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md \
  $(DOT_GITHUB_OUTPUT_DIR)/coding-style.md \
  $(DOT_GITHUB_OUTPUT_DIR)/security.md \
  $(DOT_GITHUB_OUTPUT_DIR)/workflow.md

$(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md: .github
	@echo "---\nsidebar_position: 0\n---\n" > $@
	@cat .github/$(@F) >> $@

$(DOT_GITHUB_OUTPUT_DIR)/coding-style.md: .github
	@echo "---\nsidebar_position: 2\n---\n" > $@
	@cat .github/$(@F) >> $@

$(DOT_GITHUB_OUTPUT_DIR)/security.md: .github
	@echo "---\nsidebar_position: 10\n---\n" > $@
	@cat .github/$(@F) >> $@

$(DOT_GITHUB_OUTPUT_DIR)/workflow.md: .github
	@echo "---\nsidebar_position: 1\n---\n" > $@
	@cat .github/$(@F) >> $@

clean:
	rm -f $(CHANGELOG_OUTPUT)
	$(MAKE) -C $(NOTEBOOK_INPUT_DIR) clean
	git clean -f -d -x $(NOTEBOOK_OUTPUT_DIR)
	rm $(DOT_GITHUB_OUTPUT_DIR)/code-of-conduct.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/coding-style.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/security.md
	rm $(DOT_GITHUB_OUTPUT_DIR)/workflow.md
