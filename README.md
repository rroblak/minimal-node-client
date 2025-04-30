# Minimal Node Client

A minimal Java Eth Node Client that uses Hyperledger Besu's P2P discovery (Discv5) libraries to connect to Mainnet using a temporary node identity, performs basic discovery operations (PING/PONG, FINDNODE/NEIGHBORS), and logs peer interactions.

## Features

* Binds to a configurable UDP port for discovery communication.
* Generates a temporary node key and EnodeURL upon startup.
* Connects to specified bootstrap Enode URLs.
* Initiates the bonding process by sending PING requests and processing PONG responses.
* Responds to incoming PING requests with PONG replies.
* Sends FINDNEIGHBORS requests to bonded peers to discover more nodes.
* Processes incoming NEIGHBORS packets and adds discovered peers to its known list.
* Uses Besu's `p2p`, `plugin-api`, `services`, and crypto libraries.
* Configurable via command-line arguments for bootstrap nodes, advertised IP, and listen port.
* Uses SLF4J with Log4j2 for logging network activity and peer states.
* Built with Gradle.

## Prerequisites

* **JDK 21:** Make sure you have Java Development Kit version 21 or later installed.
* **Git:** To clone the repository.

## Building

The project uses the Gradle wrapper. To build the project and create a runnable distribution:

**Build using Gradle:**
* On Linux/macOS:
    ```bash
    ./gradlew build
    ```
* On Windows:
    ```cmd
    .\gradlew.bat build
    ```
This will compile the code, run tests (if any), and assemble the application, potentially creating a distribution zip/tar in `build/distributions`.

## Running

You can run the application directly using the Gradle `run` task. Command-line arguments required by the application must be passed using the `--args` flag.

**Command-line Arguments:**

* `--bootstrap-nodes <enode1[,enode2,...]>`: **Required** (for useful operation). A comma-separated list of Enode URLs to use for bootstrapping into the network.
    * Example: `--bootstrap-nodes enode://abc...@10.1.2.3:30303,enode://def...@10.4.5.6:30303`
* `--advertised-ip <ip_address>`: *Optional*. The IP address this client should advertise in its EnodeURL. Defaults to `127.0.0.1`.
* `--listen-port <port_number>`: *Optional*. The UDP port the client should listen on. Defaults to `30303`.

**Examples:**

1.  **Run with a single bootstrap node (replace with a real mainnet/testnet bootnode):**
    * Linux/macOS:
        ```bash
        ./gradlew run --args="--bootstrap-nodes enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e8bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303"
        ```
    * Windows:
        ```cmd
        .\gradlew.bat run --args="--bootstrap-nodes enode://a979fb575495b8d6db44f750317d0f4622bf4c2aa3365d6af7c284339968eef29b69ad0dce72a4d8db5ebb4968de0e8bec910127f134779fbcb0cb6d3331163c@52.16.188.185:30303"
        ```

2.  **Run with multiple bootstrap nodes and custom settings:**
    * Linux/macOS:
        ```bash
        ./gradlew run --args="--bootstrap-nodes enode://node1@ip1:30303,enode://node2@ip2:30303 --advertised-ip 192.168.1.100 --listen-port 30304"
        ```
    * Windows:
        ```cmd
        .\gradlew.bat run --args="--bootstrap-nodes enode://node1@ip1:30303,enode://node2@ip2:30303 --advertised-ip 192.168.1.100 --listen-port 30304"
        ```

The client will start, bind to the specified port, attempt to connect to the bootstrap nodes, and log its activities to the console. It runs for a short duration (defined by `MAIN_SLEEP_DURATION_MS` in the code) and then prints the final states of known peers before shutting down.

## Configuration

* **Network:** Configured via the command-line arguments described in the [Running](#running) section.
* **Logging:** Logging behavior is controlled by `src/main/resources/log4j2.xml`. By default, it logs `INFO` level messages to the console. You can adjust the log levels here for more detailed (`DEBUG`, `TRACE`) or less detailed output.

