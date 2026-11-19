import subprocess
import random
import time
import argparse
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Semaphore

print_lock = Lock()

def log(msg):
    with print_lock:
        print(msg)

def list_chats():
    script = """
    tell application "Messages"
        set chatNames to {}
        repeat with c in every chat
            set end of chatNames to (name of c) & " [id:" & (id of c) & "]"
        end repeat
        return chatNames
    end tell
    """
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error listing chats: {result.stderr.strip()}")
        return
    for line in result.stdout.strip().split(", "):
        print(line.strip())

def build_as_batch(target: str, target_type: str, messages: list[str], is_file: bool) -> str:
    if target_type == "number":
        setup = (f'set targetService to 1st service whose service type = iMessage\n'
                 f'        set recipient to buddy "{target}" of targetService')
    elif target_type == "group_name":
        escaped = target.replace('"', '\\"')
        setup = f'set recipient to (first chat whose name = "{escaped}")'
    else:
        escaped = target.replace('"', '\\"')
        setup = f'set recipient to (first chat whose id = "{escaped}")'

    if is_file:
        path = messages[0].replace('\\', '\\\\').replace('"', '\\"')
        body = f'repeat {len(messages)} times\n            send POSIX file "{path}" to recipient\n        end repeat'
    elif len(set(messages)) == 1:
        msg = messages[0].replace('\\', '\\\\').replace('"', '\\"')
        body = f'repeat {len(messages)} times\n            send "{msg}" to recipient\n        end repeat'
    else:
        escaped_msgs = [m.replace('\\', '\\\\').replace('"', '\\"') for m in messages]
        items = ', '.join(f'"{m}"' for m in escaped_msgs)
        body = f'set msgs to {{{items}}}\n        repeat with msg in msgs\n            send msg to recipient\n        end repeat'

    return f"""
    tell application "Messages"
        {setup}
        {body}
    end tell
    """

def send_batch(target: str, target_type: str, messages: list[str], is_file: bool) -> bool:
    script = build_as_batch(target, target_type, messages, is_file)
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if result.returncode != 0:
        log(f"  Error: {result.stderr.strip()}")
    return result.returncode == 0

def random_number_string(digits: int) -> str:
    return str(random.randint(10 ** (digits - 1), 10 ** digits - 1))

def main():
    parser = argparse.ArgumentParser(description="Send messages/files via Messages app on a timer.")
    parser.add_argument("number", nargs="?", help="Target phone number (e.g. +13035551234)")
    parser.add_argument("--group", help="Target a group chat by name")
    parser.add_argument("--group-id", help="Target a group chat by its ID (from --list)")
    parser.add_argument("--list", action="store_true", help="List all chats and their IDs, then exit")
    parser.add_argument("--message", help="Send this text instead of random numbers")
    parser.add_argument("--file", help="Send a file (image, video, etc.) by path")
    parser.add_argument("--interval", type=float, default=60, help="Seconds between sends (default: 60)")
    parser.add_argument("--digits", type=int, default=6, help="Digits per random number (default: 6)")
    parser.add_argument("--count", type=int, default=0, help="How many to send (0 = infinite)")
    parser.add_argument("--workers", type=int, default=1, help="Parallel osascript processes (default: 1 — more causes drops)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Sends per osascript call (default: 1000)")
    args = parser.parse_args()

    if args.list:
        list_chats()
        return

    if not args.number and not args.group and not getattr(args, "group_id", None):
        parser.error("Provide a phone number, --group 'Name', or --group-id 'id' (use --list to see both)")

    if args.file:
        file_path = os.path.abspath(args.file)
        if not os.path.exists(file_path):
            parser.error(f"File not found: {file_path}")

    # Resolve target
    if getattr(args, "group_id", None):
        target, target_type, label = args.group_id, "group_id", f'group id "{args.group_id}"'
    elif args.group:
        target, target_type, label = args.group, "group_name", f'group "{args.group}"'
    else:
        target, target_type, label = args.number, "number", args.number

    # Describe what we're sending
    if args.file:
        mode = f"file {os.path.basename(args.file)}"
    elif args.message:
        mode = f'"{args.message}"'
    else:
        mode = f"{args.digits}-digit random numbers"

    workers = args.workers
    batch_size = getattr(args, "batch_size", 1)
    interval_desc = f"every {args.interval}s" if args.interval > 0 else "as fast as possible"
    print(f"Sending {mode} to {label} {interval_desc} — {workers} workers × {batch_size} per batch.")
    print("Press Ctrl+C to stop.\n")

    sent = 0
    counter_lock = Lock()
    sem = Semaphore(workers)
    stop = [False]
    batch_size = getattr(args, "batch_size", 1)
    is_file = bool(args.file)

    def send_batch_job(messages):
        nonlocal sent
        ok = send_batch(target, target_type, messages, is_file)
        with counter_lock:
            sent += len(messages)
            n = sent
        status = "ok" if ok else "FAILED"
        log(f"[{n}] batch of {len(messages)} — {status}")
        sem.release()

    def make_batch(size):
        if args.file:
            return [os.path.abspath(args.file)] * size
        elif args.message:
            return [args.message] * size
        else:
            return [random_number_string(args.digits) for _ in range(size)]

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            i = 0
            while not stop[0] and (args.count == 0 or i < args.count):
                remaining = (args.count - i) if args.count > 0 else batch_size
                this_batch = min(batch_size, remaining)
                sem.acquire()
                if stop[0]:
                    sem.release()
                    break
                pool.submit(send_batch_job, make_batch(this_batch))
                i += this_batch
                if args.interval > 0:
                    time.sleep(args.interval)
    except KeyboardInterrupt:
        stop[0] = True
        print(f"\nStopped after {sent} message(s).")

if __name__ == "__main__":
    main()
