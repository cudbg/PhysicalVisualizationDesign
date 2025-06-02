
# Makefile for PhysicalVisualizationDesign setup (Optimizer + Execution + Demo)

# Configurable paths
ARROW_DIR=execution/third_party/arrow
ARROW_CMAKE_FILE=$(ARROW_DIR)/cpp/cmake_modules/SetupCxxFlags.cmake
ARROW_REPO=https://github.com/apache/arrow.git

PYTHON=python3
VENV_DIR=optimizer/jade
REPO_ROOT:=$(shell pwd)
VCPKG_DIR=$(REPO_ROOT)/vcpkg
EMSDK_DIR=$(REPO_ROOT)/emsdk
export PVD_BASE := $(REPO_ROOT)

# Export environment variables
export VCPKG_ROOT=$(VCPKG_DIR)
export PATH:=$(VCPKG_DIR):$(PATH)

.PHONY: all optimizer execution run_server run_http check_duckdb clean_optimizer clean_execution clean_all ensure_arrow

# Default target: set up both parts
all: optimizer execution

###########################################################
# PART 1: Optimizer Setup (Python virtual environment)
###########################################################

optimizer:
	@echo "Setting up optimizer virtual environment..."
	cd optimizer && $(PYTHON) -m venv jade
	. $(VENV_DIR)/bin/activate && pip install -r optimizer/requirements.txt
	@echo "Optimizer environment created and dependencies installed."

###########################################################
# PART 2: Execution Setup (C++ server and WASM client)
###########################################################

# Build dependencies and both targets
execution: vcpkg emsdk build_server build_client

vcpkg:
	@if [ ! -d "$(VCPKG_DIR)" ]; then \
		echo "Cloning vcpkg..."; \
		git clone https://github.com/Microsoft/vcpkg.git $(VCPKG_DIR); \
		cd $(VCPKG_DIR) && ./bootstrap-vcpkg.sh; \
	else \
		echo "vcpkg already cloned."; \
	fi

emsdk:
	@if [ ! -d "$(EMSDK_DIR)" ]; then \
		echo "Cloning emsdk..."; \
		git clone https://github.com/emscripten-core/emsdk.git $(EMSDK_DIR); \
		cd $(EMSDK_DIR) && ./emsdk install 3.1.29 && ./emsdk activate 3.1.29; \
	else \
		echo "emsdk already cloned."; \
	fi
	@echo "Activating emsdk..."
	. $(EMSDK_DIR)/emsdk_env.sh

# Check for libduckdb library before building server
check_duckdb:
	@echo "Checking for DuckDB linking library in execution/server/lib..."
	@if [ ! -f execution/server/lib/libduckdb.so ] && [ ! -f execution/server/lib/libduckdb.dylib ]; then \
		echo "ERROR: libduckdb not found in execution/server/lib/"; \
		echo "Please download the DuckDB linking library:"; \
		echo "  - For Linux: libduckdb.so"; \
		echo "  - For macOS: libduckdb.dylib"; \
		echo "Then place the file into execution/server/lib/"; \
		exit 1; \
	else \
		echo "Found DuckDB linking library."; \
	fi

ensure_arrow:
	@if [ ! -f $(ARROW_CMAKE_FILE) ]; then \
		echo "Arrow not found. Cloning..."; \
		git clone $(ARROW_REPO) $(ARROW_DIR); \
		cd $(ARROW_DIR) && git submodule update --init --recursive; \
	else \
		echo "Arrow already present."; \
	fi

build_server: check_duckdb ensure_arrow
	@echo "Building server binary..."
	mkdir -p execution/build
	cd execution/build && cmake -S .. -B . && make -j6
	@echo "Server built at execution/build/pvd_server"

build_client:
	@echo "Building client WASM..."
	mkdir -p execution/build_wasm
	cd execution/build_wasm && emcmake cmake -S .. -B . && emmake make -j6
	@echo "WASM build complete at execution/build_wasm"

###########################################################
# Demo Targets: Run the server and HTTP demo
###########################################################

# Run the compiled server binary.
run_server: build_server
	@echo "Starting demo server..."
	@cd execution/build && ./pvd_server

# Run the HTTP server for the demo.
run_http:
	@echo "Starting HTTP server..."
	@$(PYTHON) http_server.py

###########################################################
# Clean Targets
###########################################################

clean_optimizer:
	rm -rf $(VENV_DIR)
	@echo "Removed optimizer virtual environment."

clean_execution:
	rm -rf execution/build execution/build_wasm
	@echo "Cleaned execution build directories."

clean_all: clean_optimizer clean_execution
	rm -rf $(VCPKG_DIR) $(EMSDK_DIR)
	@echo "Cleaned all automatically installed dependencies (vcpkg and emsdk)."