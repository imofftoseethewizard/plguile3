# Variables
EXTENSION = scruple

MODULE_big = $(EXTENSION)
DATA = $(EXTENSION)--1.0.sql
PG_CONFIG = pg_config
SHLIB_LINK = -lguile-3.0
CONTAINER_NAME = pg_scruple_test
EXTENSION_CONTROL = scruple.control
EXTENSION_SO = scruple.so
DOCKER_EXTENSION_DIR = /usr/share/postgresql/14/extension/

# The directory where the build artifacts will be placed
BUILD_DIR = build

# A wildcard expression to select all C source files in the src/ directory
SRC_FILES = $(wildcard src/*.c)

# Generate the list of object files by replacing the .c extension with .o and prefixing with the build directory
OBJS = $(SRC_FILES:src/%.c=$(BUILD_DIR)/%.o)

# Include PGXS settings
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
override CFLAGS := -Wall -Werror -fPIC $(CFLAGS)

# Flags for compiling and linking against Guile
GUILE_CFLAGS = $(shell pkg-config --cflags guile-3.0)
GUILE_LIBS = $(shell pkg-config --libs guile-3.0)

# Update the compile and link flags to include Guile dependencies
override CPPFLAGS += $(GUILE_CFLAGS)
override SHLIB_LINK += $(GUILE_LIBS)

# Update to include build dir, so that scruple.scm.h is reachable
override CPPFLAGS += -I$(BUILD_DIR)

# Compile llvm bitcode files for Postgres' JIT
COMPILE.c.bc = $(CLANG) -Wno-ignored-attributes $(BITCODE_CFLAGS) $(CCFLAGS) $(CPPFLAGS) -emit-llvm -c

# The object file depends on the build directory
$(BUILD_DIR)/scruple.o: $(BUILD_DIR)

# Compile rules
all: $(BUILD_DIR) $(OBJS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/scruple.scm.h: src/scruple.scm
	xxd -i $< > $@

$(BUILD_DIR)/%.o: src/%.c $(BUILD_DIR)/scruple.scm.h
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(BUILD_DIR)/%.o: src/%.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(BUILD_DIR)/%.bc : src/%.c
	$(COMPILE.c.bc) $(CCFLAGS) $(CPPFLAGS) -fPIC -c -o $@ $<

docker-install: all

	# Copy necessary files to the Docker container
	docker cp $(DATA) $(CONTAINER_NAME):$(DOCKER_EXTENSION_DIR)
	docker cp $(EXTENSION_CONTROL) $(CONTAINER_NAME):$(DOCKER_EXTENSION_DIR)
	docker cp $(EXTENSION_SO) $(CONTAINER_NAME):/usr/lib/postgresql/14/lib/

	# Install the extension within the container's PostgreSQL instance
	docker exec -it $(CONTAINER_NAME) psql -U postgres -c "CREATE EXTENSION scruple;"

.PHONY: deploy-to-docker
