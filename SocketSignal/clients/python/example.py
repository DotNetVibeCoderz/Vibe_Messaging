"""SocketSignal - Python example.

Start a server first:  dotnet run --project ../../src/SocketSignal.Demo
"""

import asyncio

from socketsignal import SignalInvocationError, SocketSignalClient


async def main() -> None:
    client = SocketSignalClient(keep_alive=10.0)

    # A method the server can call on us.
    @client.on("serverHello")
    async def server_hello(text: str) -> str:
        print(f"  <- serverHello: {text}")
        return "python heard you"

    client_id = await client.connect("ws://localhost:8080/ws/")
    print(f"connected as {client_id}")

    print("sum(5, 7)      =", await client.call("sum", 5, 7))
    print("echo('hello')  =", await client.call("echo", "hello"))

    await client.send("echo", "no reply wanted")

    try:
        await client.call("explode", "now")
    except SignalInvocationError as error:
        print("caught         =", error.remote_message)

    await asyncio.sleep(0.5)
    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
