# Variables
EXTENSION = plguile3

MODULE_big = $(EXTENSION)
DATA = $(EXTENSION)--1.0.sql
PG_CONFIG = pg_config
SHLIB_LINK = -lguile-3.0
EXTENSION_CONTROL = plguile3.control
EXTENSION_SO = plguile3.so

# The directory where the build artifacts will be placed
BUILD_DIR = build

# A wildcard expression to select all C source files in the src/
# directory
SRC_FILES = $(wildcard src/*.c)
MODULE_FILES = $(wildcard src/modules/*.scm)

# Generate the list of object files by replacing the .c extension with
# .o and prefixing with the build directory
OBJS = $(SRC_FILES:src/%.c=$(BUILD_DIR)/%.o)
MODULE_HEADERS = $(MODULE_FILES:src/modules/%.scm=$(BUILD_DIR)/%.scm.h)

# Include PGXS settings
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
override CFLAGS := -Wall -Werror -fPIC $(CFLAGS)

# Flags for compiling and linking against Guile
GUILE_CFLAGS = $(shell pkg-config --cflags guile-3.0)
GUILE_LIBS = $(shell pkg-config --libs guile-3.0)
GUILE_SITEDIR = $(shell pkg-config --variable=sitedir guile-3.0)

# Update the compile and link flags to include Guile dependencies
override CPPFLAGS += $(GUILE_CFLAGS)
override SHLIB_LINK += $(GUILE_LIBS)

# Update to include build dir, so that plguile3.scm.h is reachable
override CPPFLAGS += -I$(BUILD_DIR)

# Compile llvm bitcode files for Postgres' JIT
COMPILE.c.bc = $(CLANG) -Wno-ignored-attributes $(BITCODE_CFLAGS) -emit-llvm

# The object file depends on the build directory
$(BUILD_DIR)/plguile3.o: $(BUILD_DIR)

# Compile rules
all: $(BUILD_DIR) $(OBJS)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%.scm.h: src/modules/%.scm
	xxd -i $< $@

$(BUILD_DIR)/%.o: src/%.c $(MODULE_HEADERS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

$(BUILD_DIR)/%.bc : src/%.c
	$(COMPILE.c.bc) $(CCFLAGS) $(CPPFLAGS) -c $< -o $@
