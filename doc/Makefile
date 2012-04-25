BASE :		.
DOXYFILE := $(BASE)/Doxyfile
DOC := 		$(BASE)/html/
WEB := 		www.icir.org:vast

doc: FORCE
	doxygen $(DOXYFILE)

www: doc
	rsync -az -e ssh --delete $(DOC) $(WEB)

update:
	doxygen -u $(DOXYFILE)

FORCE:
