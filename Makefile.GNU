# The top-level Makefile includes this Makefile and must define the variable
# DIRS to point to at least one directory.

INTERNAL := distclean
EXTERNAL := $(filter-out $(INTERNAL), $(MAKECMDGOALS))

$(or $(lastword $(EXTERNAL)), all):
	@for dir in $(DIRS); do $(MAKE) -C $$dir $(EXTERNAL); done

$(filter-out $(lastword $(EXTERNAL)), $(EXTERNAL)):
	@cd .

distclean:
	rm -rf $(DIRS) Makefile

.PHONY: distclean $(EXTERNAL)
