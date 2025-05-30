# Overview

This repo contains the source code for SIGMOD 2025 submission "Physical Visualization Design: Decoupling Interface and System Design". 

All code are tested in Ubuntu 24.04 and MacOS x86-64. MacOS aarch64 may not be supported.

## Code Organization

    . 
    ├── execution    ----------------  The source code of Jade execution engine (in C++)
    │   ├── client    ---------------  The client-side code that compiles to WASM library
    │   ├── server    ---------------  The server-side cdoe that compiles to an executable websocket server
    │   ├── share    ----------------  The common code (plan definitions, etc) shared with server and client
    │   └── third_party    ----------  Including Apache arrow library
    │   
    ├── optimizer    ----------------  The source code of Jade plan optimizer (in Python)
    │   ├── plan    -----------------  Plan definition
    │   ├── rule    -----------------  Optimization rules
    │   ├── optimize.py    ----------  The entry point of the optimization algorithm, including the
    │                                  integer programming model solved by z3.
    │   ├── search.py    ------------  The multi-stage searching algorithm for candidate plans
    │   └── ...
    │
    └── data
        └── pvd.db    ---------------  Dataset for demo

## Part 1: Optimizer

The optimizer is implemented in Python. To run the code, first create a `venv` and install all dependencies.

    cd optimizer/
    python3 -m venv jade
    source jade/bin/activate
    pip install -r requirements.txt

Set environment variable `PVD_BASE`

    export PVD_BASE='/root/of/this/repo'

The `examples.py` pre-defines the examples used in the experiments of the paper: `weather`, `sdss`, `covid`, `brightkite`, `flights`, `liquor`. 

The repo has attached a sample database `data/pvd.db` which contains one table for each example to run the demo (except for `weather` due to size limit). 

`main.py` has the minimal example to run the optimizer. It constructs an input from `examples.py` and call `optimize()` to generate the optimal execution plan.

    task = covid(client_mem=10000000, server_mem=1000000000),
    res = optimize(task)
    print(res)

See outputs by running
 
    python main.py

## Part 2: Execution

Jade execution engine is implemented in C++. The same codebase will be compiled to two targets: a binary executable server and a WebAssembly library. To build the target, install the following dependencies:

### Dependencies

1. Install `vcpkg` and initialize. https://vcpkg.io/en/getting-started.html

        git clone https://github.com/Microsoft/vcpkg.git
        cd vcpkg
        ./bootstrap-vcpkg.sh
        
        export VCPKG_ROOT=/path/to/vcpkg
        export PATH=$VCPKG_ROOT:$PATH

2. Install `emsdk` and initialize. 

        git clone https://github.com/emscripten-core/emsdk.git
        cd emsdk
        ./emsdk install 3.1.29
        ./emsdk activate 3.1.29
        source "./emsdk/emsdk_env.sh"

3. Install system dependency `ccache`

        brew install ccache
        or
        sudo apt install ccache 

4. Download [duckdb linking library](https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=macos&download_method=direct) into `execution/server/lib`

        cd execution/server/
        mkdir lib
        mv libduckdb.dylib lib/ .  (or libduckdb.so)

### Build server side

    cd execution/
    mkdir build
    cd build
    cmake -S .. -B .
    make -j6
    cd ..

### Build client WASM

    cd execution/
    mkdir build_wasm
    cd build_wasm
    emcmake cmake -S .. -B .
    emmake make -j6
    cd ..

## Start Server

    cd build/
    ./pvd_server

The server will prompt a port number, like

    Server started at port 13154

Remember this port number which will be used in the demo.

## Start Http Server

    python3 http_server.py

* Then access `localhost:8000`. 
* Wait for the status change from `Unconnected` to `Loaded`, which means the WebAssembly library is loaded. 
* Then, fill in `Port` the port number of the server, click `Connect`. Make sure the status change to `websocket connected`.
* The demo has built-in some plans to run. Select one from the dropdown, e.g. `covid&--server`. Click `Run`.
* The webpage will show the plan being executed, and it will run the plan with random binding and show the execution time below.
* Result tables of the executions can be found in console log.
