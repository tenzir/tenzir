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

bash: # enable `make docker TARGET=bash`
	@bash

define VAST_COMPOSE_OVERLAYS
-f ../../docker/quarto/docker-compose.build.yaml
-f ../../docker/quarto/docker-compose.bind.yaml
-f ../../docker/quarto/docker-compose.vast.yaml
-f ../../docker/vast/docker-compose.yaml
endef

export VAST_COMPOSE_OVERLAYS
docker: # run Quarto with a VAST server running in another service
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
