POETRY := $(shell command -v poetry 2> /dev/null)
QUARTO := $(shell command -v quarto 2> /dev/null)

QUARTO_INPUTS := $(shell find . -name '*.qmd')
QUARTO_MD_OUTPUTS := $(patsubst %.qmd, %.md, $(QUARTO_INPUTS))
QUARTO_FILES_OUTPUTS := $(patsubst %.qmd, %_files, $(QUARTO_INPUTS))

all: poetry quarto

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

quarto: $(QUARTO_MD_OUTPUTS)

clean:
	rm -rf $(QUARTO_MD_OUTPUTS) $(QUARTO_FILES_OUTPUTS)

# A straightforward target to enable `make docker TARGET=bash`
bash:
	@bash

define VAST_COMPOSE_OVERLAYS
-f ../../docker/compose/quarto.yaml
-f ../../docker/compose/quarto.bind.yaml
-f ../../docker/compose/quarto.vast.yaml
-f ../../docker/compose/vast.yaml
endef

export VAST_COMPOSE_OVERLAYS
# Run this makefile from within the Quarto Docker image, with a VAST server
# running in another service.
# You can specify another target using the TARGET variable:
# make docker TARGET=clean
docker: 
	docker compose \
		$$VAST_COMPOSE_OVERLAYS \
		up -d vast && \
	HOST_UID=$$(id -u) HOST_GID=$$(id -g) docker compose \
		$$VAST_COMPOSE_OVERLAYS \
		run quarto \
		make -C examples/notebooks $(TARGET) ; \
	docker compose \
		$$VAST_COMPOSE_OVERLAYS \
		down
