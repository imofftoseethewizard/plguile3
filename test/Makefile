BUILD_DIR = build

# A wildcard expression to select all C source files in the src/
# directory
CONCURRENT_TESTS = $(wildcard concurrent/*.sql)

# Generate the list of object files by replacing the .c extension with
# .o and prefixing with the build directory
GENERATED_TESTS  = $(CONCURRENT_TESTS:concurrent/%.sql=$(BUILD_DIR)/%-a.sql)
GENERATED_TESTS += $(CONCURRENT_TESTS:concurrent/%.sql=$(BUILD_DIR)/%-b.sql)
GENERATED_TESTS += $(CONCURRENT_TESTS:concurrent/%.sql=$(BUILD_DIR)/%-c.sql)

all: $(BUILD_DIR) $(GENERATED_TESTS)

clean:
	rm $(BUILD_DIR)/*

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/%-a.sql: concurrent/%.sql
	sed '/^\t/s/^/-- /' $< > $@

$(BUILD_DIR)/%-b.sql: concurrent/%.sql
	sed -E '/^([^\t]|\t\t)/s/^/-- /' $< > $@

$(BUILD_DIR)/%-c.sql: concurrent/%.sql
	sed -E '/^([^\t]|\t[^\t]|\t\t\t)/s/^/-- /' $< > $@
