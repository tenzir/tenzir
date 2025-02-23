# This Makefile does the necessary pre-processing and conversion of files such
# that they can be added to the website.

CONTRIBUTING_DIR := docs/contribution

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

clean:
	rm -f $(CHANGELOG_OUTPUT)
	rm -f $(RFC_OUTPUT)
	rm -f $(CONTRIBUTING_DIR)/code-of-conduct.md
	rm -f $(CONTRIBUTING_DIR)/coding-style.md
	rm -f $(CONTRIBUTING_DIR)/security.md
	rm -f $(CONTRIBUTING_DIR)/workflow.md
