# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CHANGELOG_INPUT := ../CHANGELOG.md
CHANGELOG_OUTPUT := src/pages/changelog.md

NOTEBOOK_INPUT_DIR := ../examples/notebooks
NOTEBOOK_OUTPUT_DIR := docs/try-vast
NOTEBOOK_INPUTS := $(wildcard $(NOTEBOOK_INPUT_DIR)/*.md)
NOTEBOOK_OUTPUTS := $(addprefix $(NOTEBOOK_OUTPUT_DIR)/, $(notdir $(NOTEBOOK_INPUTS)))

all: changelog notebooks

changelog: $(CHANGELOG_OUTPUT)

notebooks: $(NOTEBOOK_OUTPUTS)

clean:
	rm -f $(CHANGELOG_OUTPUT)
	$(MAKE) -C $(NOTEBOOK_INPUT_DIR) clean
	git clean -f -d -x $(NOTEBOOK_OUTPUT_DIR)

$(CHANGELOG_OUTPUT): $(CHANGELOG_INPUT)
	cp -f $< $@

$(NOTEBOOK_OUTPUTS): $(NOTEBOOK_INPUTS)
	@$(MAKE) -C $(NOTEBOOK_INPUT_DIR) md
	cp -r $(NOTEBOOK_INPUT_DIR)/output/md/* $(NOTEBOOK_OUTPUT_DIR)
