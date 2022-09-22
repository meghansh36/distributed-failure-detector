import asyncio
from contextlib import AsyncExitStack, suppress
import signal
from members import Member
from transport import UdpTransport

async def run():

    loop = asyncio.get_running_loop()

    async with AsyncExitStack() as stack:

        members = [Member('127.0.0.1', 8000)]
        stack.enter_context(suppress(asyncio.CancelledError))
        tranport = UdpTransport('127.0.0.1', 8001, members)
        worker = await stack.enter_async_context(tranport.enter())

        task = asyncio.create_task(worker.run())
        loop.add_signal_handler(signal.SIGINT, task.cancel)
        loop.add_signal_handler(signal.SIGTERM, task.cancel)
        await task
    
    return 0


asyncio.run(run())