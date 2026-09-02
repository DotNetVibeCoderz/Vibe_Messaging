// SocketSignal - Node.js example.
// Start a server first: dotnet run --project ../../src/SocketSignal.Demo
import { SocketSignalClient } from "./src/index.mjs";

const client = new SocketSignalClient({ keepAliveMs: 10_000 });

// A method the server can call on us.
client.on("serverHello", (text) => {
  console.log(`  <- serverHello: ${text}`);
  return "node heard you";
});

client.addEventListener("disconnected", (e) => console.log(`  -- disconnected: ${e.detail}`));

const id = await client.connect("ws://localhost:8080/ws/");
console.log(`connected as ${id}`);

console.log("sum(5, 7)      =", await client.call("sum", 5, 7));
console.log("echo('hello')  =", await client.call("echo", "hello"));

client.send("echo", "no reply wanted");

try {
  await client.call("explode", "now");
} catch (error) {
  console.log("caught         =", error.message);
}

setTimeout(() => client.close(), 500);
