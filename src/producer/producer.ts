import net from "node:net";

const words = ["flink", "typescript", "stream", "docker"];

net.createServer((socket) => {
  console.log("Flink connected");
  const timer = setInterval(() => {
    const line = Array.from({ length: 5 }, () =>
      words[Math.floor(Math.random() * words.length)]
    ).join(" ");
    socket.write(line + "\n");
    console.log("Sent:", line);
  }, 1000);
  socket.on("close", () => clearInterval(timer));
  socket.on("error", () => clearInterval(timer));
}).listen(9999, () => console.log("Listening on 9999"));