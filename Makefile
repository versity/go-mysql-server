# Package configuration
PROJECT = go-mysql-server
COMMANDS =
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
BREW := $(shell command -v brew 2>/dev/null)
ICU_CELLAR := $(shell if [ -n "$(BREW)" ]; then $(BREW) --cellar icu4c@78 2>/dev/null || $(BREW) --cellar icu4c 2>/dev/null; fi)
ICU_PREFIX := $(shell if [ -n "$(ICU_CELLAR)" ] && [ -d "$(ICU_CELLAR)" ]; then latest="$$(ls -1 "$(ICU_CELLAR)" | tail -n 1)"; if [ -n "$$latest" ]; then echo "$(ICU_CELLAR)/$$latest"; fi; fi)
ifneq ($(ICU_PREFIX),)
export CGO_CPPFLAGS := -I$(ICU_PREFIX)/include $(CGO_CPPFLAGS)
export CGO_LDFLAGS := -L$(ICU_PREFIX)/lib $(CGO_LDFLAGS)
endif
endif

# Including ci Makefile
CI_REPOSITORY ?= https://github.com/src-d/ci.git
CI_BRANCH ?= v1
CI_PATH ?= .ci
MAKEFILE := $(CI_PATH)/Makefile.main
$(MAKEFILE):
	git clone --quiet --depth 1 -b $(CI_BRANCH) $(CI_REPOSITORY) $(CI_PATH);
-include $(MAKEFILE)

integration:
	./_integration/run ${TEST}

oniguruma:
ifeq ($(UNAME_S),Linux)
	$(shell apt-get install libonig-dev)
endif

ifeq ($(UNAME_S),Darwin)
	$(shell brew install oniguruma)
endif

.PHONY: integration