from asyncio import sleep, CancelledError
from sys import stdout

# Console api stats display
async def status_loop(stats, interval: float = 1.0):
    is_interactive = stdout.isatty()
    log_interval = interval if is_interactive else 30.0  # Log every 30s in Docker

    try:
        while True:
            line = (
                f"API Requests: {stats['responses']} | "
                f"Cached: {stats['cache_hits']} | "
                f"Network: {stats['network_requests']} | "
                f"Errors: {stats['errors']} | "
                f"Retries: {stats['retries']} "
            )
            
            if is_interactive:
                stdout.write("\r" + line)
                stdout.flush()
            else:
                print(line)

            await sleep(log_interval)
    except CancelledError:
        return
