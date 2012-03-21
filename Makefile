# A simple static wrapper for a number of standard Makefile targets,
# mostly just forwarding to build/Makefile. This is provided only for
# convenience and supports only a subset of what CMake's Makefile
# offers.

BUILD=build

all: configured
	@$(MAKE) -C $(BUILD) $@

configured:
	@test -d $(BUILD) || ( echo "Error: No build/ directory found. Did you run configure?" && exit 1 )
	@test -e $(BUILD)/Makefile || ( echo "Error: No build/Makefile found. Did you run configure?" && exit 1 )

test: configured
	@$(MAKE) -C $(BUILD) $@

install:
	@$(MAKE) -C $(BUILD) $@

uninstall: configured
	@$(MAKE) -C $(BUILD) $@

clean: configured
	@$(MAKE) -C $(BUILD) $@

distclean:
	rm -rf $(BUILD)

doc:
	@$(MAKE) -C $(BUILD) $@

.PHONY : all configured test install uninstall clean distclean doc
