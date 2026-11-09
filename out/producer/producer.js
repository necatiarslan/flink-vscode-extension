"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const node_net_1 = __importDefault(require("node:net"));
const words = ["flink", "typescript", "stream", "docker"];
node_net_1.default.createServer((socket) => {
    console.log("Flink connected");
    const timer = setInterval(() => {
        const line = Array.from({ length: 5 }, () => words[Math.floor(Math.random() * words.length)]).join(" ");
        socket.write(line + "\n");
        console.log("Sent:", line);
    }, 1000);
    socket.on("close", () => clearInterval(timer));
    socket.on("error", () => clearInterval(timer));
}).listen(9999, () => console.log("Listening on 9999"));
//# sourceMappingURL=producer.js.map