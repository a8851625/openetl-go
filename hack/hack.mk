.DEFAULT_GOAL := build

# Update GoFrame and its CLI to latest stable version.
.PHONY: up
up: cli.install
	@gf up -a

# Build binary using configuration from hack/config.yaml.
.PHONY: build
build: cli.install
	@gf build -ew

# Parse api and generate controller/sdk.
.PHONY: ctrl
ctrl: cli.install
	@gf gen ctrl

# Generate Go files for DAO/DO/Entity.
.PHONY: dao
dao: cli.install
	@gf gen dao

# Parse current project go files and generate enums go file.
.PHONY: enums
enums: cli.install
	@gf gen enums

# Generate Go files for Service.
.PHONY: service
service: cli.install
	@gf gen service


# Package release archives for GitHub Releases.
.PHONY: release
release:
	@chmod +x hack/release.sh && ./hack/release.sh


# Build container image.
.PHONY: image
image:
	$(eval _TAG  = $(shell git rev-parse --short HEAD))
ifneq (, $(shell git status --porcelain 2>/dev/null))
	$(eval _TAG  = $(_TAG).dirty)
endif
	$(eval _TAG  = $(if ${TAG},  ${TAG}, $(_TAG)))
	$(eval _PUSH = $(if ${PUSH}, ${PUSH}, ))
	@. ./hack/container-cli.sh; \
	detect_container_cli; \
	"$$CONTAINER_CLI" build -t $(DOCKER_NAME):${_TAG} -f Dockerfile .; \
	if [ -n "${_PUSH}" ]; then "$$CONTAINER_CLI" push $(DOCKER_NAME):${_TAG}; fi


# Build container image and automatically push to image repository.
.PHONY: image.push
image.push:
	@make image PUSH=-p;


# Parsing protobuf files and generating go files.
.PHONY: pb
pb: cli.install
	@gf gen pb

# Generate protobuf files for database tables.
.PHONY: pbentity
pbentity: cli.install
	@gf gen pbentity
