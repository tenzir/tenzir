# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CHANGELOG_INPUT := ../CHANGELOG.md
CHANGELOG_OUTPUT := src/pages/changelog.md

NOTEBOOK_INPUT_DIR := ../examples/notebooks
NOTEBOOK_OUTPUT_DIR := docs/try-vast
NOTEBOOK_INPUTS := $(wildcard $(NOTEBOOK_INPUT_DIR)/*.md)
NOTEBOOK_OUTPUTS := $(addprefix $(NOTEBOOK_OUTPUT_DIR)/, $(notdir $(NOTEBOOK_INPUTS)))

CONTRIBUTING_DIR := docs/develop-vast/contributing

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
  $(CONTRIBUTING_DIR)/code-of-conduct.md \
  $(CONTRIBUTING_DIR)/coding-style.md \
  $(CONTRIBUTING_DIR)/security.md \
  $(CONTRIBUTING_DIR)/workflow.md

$(CONTRIBUTING_DIR)/code-of-conduct.md: .github
	@echo "---\nsidebar_position: 0\n---\n" > $@
	@cat .github/CODE-OF-CONDUCT.md >> $@

$(CONTRIBUTING_DIR)/coding-style.md: .github
	@echo "---\nsidebar_position: 2\n---\n" > $@
	@cat .github/$(@F) >> $@

$(CONTRIBUTING_DIR)/security.md: .github
	@echo "---\nsidebar_position: 10\n---\n" > $@
	@cat .github/SECURITY.md >> $@

$(CONTRIBUTING_DIR)/workflow.md: .github
	@echo "---\nsidebar_position: 1\n---\n" > $@
	@cat .github/$(@F) >> $@

clean:
	rm -f $(CHANGELOG_OUTPUT)
	$(MAKE) -C $(NOTEBOOK_INPUT_DIR) clean
	git clean -f -d -x $(NOTEBOOK_OUTPUT_DIR)
	rm -rf .github
	rm $(CONTRIBUTING_DIR)/code-of-conduct.md
	rm $(CONTRIBUTING_DIR)/coding-style.md
	rm $(CONTRIBUTING_DIR)/security.md
	rm $(CONTRIBUTING_DIR)/workflow.md
