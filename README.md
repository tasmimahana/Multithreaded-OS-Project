# CSE 321 Lab Project — Multithreaded Process Manager Simulator

This project implements the PDF spec: a simulated process table (max 64 PCBs) updated concurrently by worker threads executing script files, plus a monitor thread that writes `22101596.txt` whenever the table changes.

## Files

- `pm_sim.c`: full implementation (single-file submission style)
- `thread1.txt`, `thread2.txt`: sample scripts (you can add more)
- Output: `22101596.txt`

## Build (Windows)

You need a compiler with pthreads support.

### Option A) MSYS2 MinGW-w64

From an MSYS2 MinGW64 terminal in this folder:

```bash
gcc -O2 -Wall -Wextra -pthread pm_sim.c -o pm_sim
./pm_sim thread1.txt thread2.txt
```

### Option B) WSL (Ubuntu)

```bash
gcc -O2 -Wall -Wextra -pthread pm_sim.c -o pm_sim
./pm_sim thread1.txt thread2.txt
```

## Script command format (from PDF)

- `fork <parent_pid>`
- `exit <pid> <status>`
- `wait <parent_pid> <child_pid>` where `<child_pid> = -1` means “wait for first child that exits”
- `kill <pid>`
- `sleep <milliseconds>`

## Notes

- PIDs are monotonically increasing and are **not reused**.
- Process states used in snapshots: `RUNNING`, `WAITING`, `ZOMBIE` (terminated processes are removed and not shown).

