# Syncron

> A bidirectional synchronization tool built with state tracking heuristics.

## Overview
Syncron is a utility designed to keep two directories in sync. It uses a persistent state database to track file history, which allows it to identify and propagate deletions across both locations. The tool includes logic to handle large files where the size is identical but the timestamps show minor discrepancies.

## Features
- **State Tracking:** Uses a local database to remember file presence and manage deletions between folders.
- **Jitter Heuristics:** Skips updates for files when time differences are small and the file size has not changed.
- **Concurrent Scanning:** Processes directory metadata across multiple threads simultaneously.
- **Dry Run:** Provides a count of operations that would occur without modifying the filesystem or database.

## Build
To build this tool from source, clone the repo:

```
git clone https://github.com/durakitus/syncron.git
cd syncron
cargo build --release
```

## Usage
Some useful commands are:

`syncron <path_to_first_dir> <path_to_second_dir>` — standard synchronization command.
`syncron --dry-run <path_to_first_dir> <path_to_second_dir>` — a simulation of the result of a sync operation.
`syncron --config` — show the paths of the state database **and** the binary log file containing the last directory paths that were synced, which allows running `syncron` without arguments.

Run `syncron -h` — or `cargo run -- -h` inside the project folder if it's still not in your `PATH` — for more usage details if you decide to build it.
