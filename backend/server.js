const rooms = new Map();

import express from "express";
import http from "http";
import { Server } from "socket.io";
import mediasoup from "mediasoup";
import os from "os";

import { mediaCodecs } from "./src/config/mediasoupConfig.js";

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// Is array mein hum apne saare active Workers (C++ processes) store karenge
let workers = [];
let nextWorkerIndex = 0; // Load balancing ke liye pointer

// Function to create worker processes
async function createWorkers() {
  const numCores = os.cpus().length;
  console.log(
    `System has ${numCores} CPU cores. Starting Mediasoup workers...`,
  );

  for (let i = 0; i < numCores; i++) {
    // C++ process create ho raha hai
    const worker = await mediasoup.createWorker({
      logLevel: "warn", // Sirf errors/warnings console me daalega
      rtcMinPort: 10000, // WebRTC media transfer inhi ports ke beech hoga
      rtcMaxPort: 10100,
    });

    // Agar kisi error ki wajah se C++ process background me crash ho gaya
    worker.on("died", () => {
      console.error(`Worker ${worker.pid} died. Exiting...`);
      process.exit(1); // Server restart karna padega
    });

    workers.push(worker);
    console.log(`Worker ${i + 1} started with PID: ${worker.pid}`);
  }
}

// Function 1: Ek khali/next worker uthana (Load Balancing)
function getNextWorker() {
  const worker = workers[nextWorkerIndex];
  // Next time ke liye index ko aage badhao, aur end pe pahuche toh wapas 0 kardo
  nextWorkerIndex = (nextWorkerIndex + 1) % workers.length;
  return worker;
}

// Function 2: Actual Room (Router) create karna
async function createRoom() {
  const worker = getNextWorker();

  // Worker ke andar ek Router (Room) banao aur usko apne Codecs de do
  const router = await worker.createRouter({ mediaCodecs });

  console.log(
    `New Room created on Worker PID: ${worker.pid} | Router ID: ${router.id}`,
  );

  return router;
}

io.on("connection", (socket) => {
  console.log(`Socket connected to server: ${socket.id}`);

  // Event: Jab user room join karne ki request karega
  socket.on("joinRoom", async ({ roomId }, callback) => {
    try {
      // 1. Check karo ki kya is ID ka room pehle se hamare Map me maujood hai?
      let room = rooms.get(roomId);

      // 2. Agar room pehle se nahi bana hai, toh naya room khada karo
      if (!room) {
        // Humne Step 3 me createRoom() banaya tha jo Router return karta hai
        const router = await createRoom();

        room = {
          router: router, // Is room ka dedicated Mediasoup Router
          peers: new Map(), // Is room ke andar ke saare users ka sub-map
        };
        // Global rooms map me save kar do
        rooms.set(roomId, room);
      }

      // 3. Naye user (Peer) ka object banakar room ke peers Map me store karo
      room.peers.set(socket.id, {
        id: socket.id,
        sendTransport: null, // Bhejne wali pipe (Producer ke liye)
        recvTransport: null, // Receive karne wali pipe (Consumer ke liye)
        producers: new Map(), // Is peer ke video/audio producers yahan save honge
        consumers: new Map(), // Dusro ke streams ko consume karne wale consumers ka track
      });

      // 4. Socket.io ka standard channel join karwao (Signaling ke liye zaroori hai)
      socket.join(roomId);
      console.log(`Peer ${socket.id} successfully mapped to Room: ${roomId}`);

      // 5. CRITICAL: Frontend ko is room ke Router ki capabilities return karo
      // Yaad hai? Frontend par 'device.load()' chalane ke liye iski strict zaroorat hoti hai.
      callback({ routerRtpCapabilities: room.router.rtpCapabilities });
    } catch (error) {
      console.error("Room join karne me error aaya:", error);
      callback({ error: error.message });
    }
  });

  // Event: Frontend se Transport create karne ki request
  socket.on("createWebRtcTransport", async ({ roomId }, callback) => {
    try {
      const room = rooms.get(roomId);
      if (!room) {
        return callback({ error: "Room not found!" });
      }

      const router = room.router;

      // 1. Router ke andar actual WebRTC Transport create karna (C++ level pe)
      const transport = await router.createWebRtcTransport({
        listenIps: [
          {
            ip: "0.0.0.0", // Server sabhi interfaces pe listen karega
            announcedIp: "127.0.0.1", // LOCALHOST dev ke liye. Prod me yahan public IP aayega.
          },
        ],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true, // Low latency media hamesha UDP prefer karta hai
      });

      // 2. Transport object ko hum user (peer) ke state me store kar lenge
      const peer = room.peers.get(socket.id);
      // peer.transports.set(transport.id, transport);

      peer.sendTransport = transport;

      console.log(
        `Transport created ID: ${transport.id} for Peer: ${socket.id}`,
      );

      // 3. Frontend ko sirf wo parameters return karo jo client side Transport banane ke liye chahiye
      callback({
        params: {
          id: transport.id,
          iceParameters: transport.iceParameters,
          iceCandidates: transport.iceCandidates,
          dtlsParameters: transport.dtlsParameters,
        },
      });

      // (Optional) Memory leak na ho isliye transport close hone pe map se hata do
      transport.on("dtlsstatechange", (dtlsState) => {
        if (dtlsState === "closed") {
          transport.close();
        }
      });

      transport.on("close", () => {
        peer.transports.delete(transport.id);
        console.log(`Transport removed`);
      });
    } catch (error) {
      console.error("Error creating WebRTC Transport:", error);
      callback({ error: error.message });
    }
  });

  // Trap A Backend Handler: Security Handshake (Updated for Both Transports)
  socket.on(
    "transport-connect",
    async ({ roomId, dtlsParameters, isSend }, callback) => {
      console.log(
        `--- Handshake Request (DTLS) from Peer: ${socket.id} | isSend: ${isSend} ---`,
      );
      try {
        const peer = rooms.get(roomId).peers.get(socket.id);

        // CRITICAL: Yahan decide ho raha hai ki kis pipe pe lock lagana hai
        const transport = isSend ? peer.sendTransport : peer.recvTransport;

        if (!transport)
          throw new Error(`${isSend ? "Send" : "Recv"} Transport not found!`);

        // C++ Worker me transport ko securely lock karna
        await transport.connect({ dtlsParameters });

        // Frontend ko bata do ki lock lag gaya
        callback();
      } catch (error) {
        console.error("Transport connect error:", error);
      }
    },
  );

  // Trap B Backend Handler: Actual Producer (Media Endpoint) Banana
  socket.on(
    "transport-produce",
    async ({ roomId, kind, rtpParameters }, callback) => {
      console.log(
        `--- Produce Request for ${kind} from Peer: ${socket.id} ---`,
      );
      try {
        const peer = rooms.get(roomId).peers.get(socket.id);
        // const transport = Array.from(peer.transports.values())[0];
        const transport = peer.sendTransport;

        if (!transport)
          throw new Error("Send Transport not found for producing!");

        // C++ Worker me Server-Side Producer object create karna
        const producer = await transport.produce({
          kind,
          rtpParameters,
        });

        // Is naye producer ko peer ke object me save kar lo
        peer.producers.set(producer.id, producer);

        // Jisne video chalu ki hai usko chhod kar, room ke baaki sabhi logo ko ping karo
        socket.to(roomId).emit("new-producer", {
          producerId: producer.id,
        });

        // Server-Side Producer banne ke baad uski ID frontend ko return karna zaroori hai
        callback({ id: producer.id });
      } catch (error) {
        console.error("Transport produce error:", error);
      }
    },
  );

  // // Handle Disconnect (Memory leaks se bachne ke liye clean-up trap)
  // socket.on("disconnect", () => {
  //   console.log(`Socket disconnected: ${socket.id}`);
  //   // Jab user leave karega, toh yahan se hum usko memory se saaf karne ka logic likhenge
  // });

  // Phase 1: Frontend se Receive Transport banane ki request
  socket.on("createRecvTransport", async ({ roomId }, callback) => {
    try {
      const room = rooms.get(roomId);
      if (!room) return callback({ error: "Room not found!" });

      const router = room.router;

      // C++ Worker me in-bound network socket kholna
      const transport = await router.createWebRtcTransport({
        listenIps: [{ ip: "0.0.0.0", announcedIp: "127.0.0.1" }],
        enableUdp: true,
        enableTcp: true,
        preferUdp: true,
      });

      const peer = room.peers.get(socket.id);
      // Isko recvTransport memory allocation me daal
      peer.recvTransport = transport;

      console.log(
        `Recv Transport created ID: ${transport.id} for Peer: ${socket.id}`,
      );

      // Client ko paramters return kar taaki woh frontend pe pipe bana sake
      callback({
        params: {
          id: transport.id,
          iceParameters: transport.iceParameters,
          iceCandidates: transport.iceCandidates,
          dtlsParameters: transport.dtlsParameters,
        },
      });
    } catch (error) {
      console.error("Error creating Recv Transport:", error);
      callback({ error: error.message });
    }
  });

  // Phase 2: Frontend se Video Consume karne ki request
  socket.on(
    "consume",
    async ({ roomId, producerId, rtpCapabilities }, callback) => {
      console.log(`--- Consume Request for Producer: ${producerId} ---`);
      try {
        const room = rooms.get(roomId);
        if (!room) return callback({ error: "Room not found!" });

        const router = room.router;
        const peer = room.peers.get(socket.id);
        const recvTransport = peer.recvTransport;

        if (!recvTransport) {
          return callback({
            error: "Receive Transport abhi tak nahi bani hai!",
          });
        }

        // 1. TERA WALA LOGIC: Compatibility Check
        if (router.canConsume({ producerId, rtpCapabilities })) {
          // 2. CRITICAL STEP: Paused state me Server-Side Consumer banana
          const consumer = await recvTransport.consume({
            producerId,
            rtpCapabilities,
            paused: true, // Backend tab tak ruka rahega jab tak frontend DTLS handshake na kar le
          });

          // 3. Is consumer ko memory (peer object) me save kar lo
          peer.consumers.set(consumer.id, consumer);

          console.log(`Server-side Consumer Created ID: ${consumer.id}`);

          // Cleanup Traps: Agar video bhejne wale ne tab band kar diya toh ye consumer bhi delete ho jaye
          consumer.on("transportclose", () => {
            peer.consumers.delete(consumer.id);
          });
          
          consumer.on("producerclose", () => {
            peer.consumers.delete(consumer.id);
            console.log(`Video ruki! Frontend ko bol raha hu dabba hatane...`);
            // --- YE NAYI LINE DAAL ---
            // Frontend ko emit karke batao ki ye wali video band ho gayi hai
            socket.emit("producer-closed", { consumerId: consumer.id });
          });

          // 4. Frontend ko parameters wapas bhejo taaki wahan 'clientConsumer' ban sake
          callback({
            params: {
              id: consumer.id,
              producerId: consumer.producerId,
              kind: consumer.kind,
              rtpParameters: consumer.rtpParameters,
            },
          });
        } else {
          callback({
            error:
              "Client ka browser is video format ko support nahi karta (canConsume false)",
          });
        }
      } catch (error) {
        console.error("Consume error:", error);
        callback({ error: error.message });
      }
    },
  );

  // Phase 4 (Server Side): Consumer ko Un-pause karna
  socket.on("consumer-resume", async ({ roomId, consumerId }) => {
    try {
      const peer = rooms.get(roomId).peers.get(socket.id);
      const consumer = peer.consumers.get(consumerId);

      if (!consumer) return;

      // C++ Worker ko green signal do ki video frames bhejna chalu kare
      await consumer.resume();
      console.log(
        `Consumer ID: ${consumerId} UN-PAUSED! Video is flowing to client.`,
      );
    } catch (error) {
      console.error("Error resuming consumer:", error);
    }
  });

  // Backend se pucho ki room me kon kon video bhej raha hai
  socket.on("get-producers", ({ roomId }, callback) => {
    const room = rooms.get(roomId);
    if (!room) return callback([]);

    let producerIds = [];

    // Room ke saare users me check karo
    room.peers.forEach((peer) => {
      // Khud ki video chhod kar baaki sabki ID le lo
      if (peer.id !== socket.id) {
        peer.producers.forEach((producer) => {
          producerIds.push(producer.id);
        });
      }
    });

    callback(producerIds);
  });

  // Handle Disconnect (Clean-up trap)
  socket.on("disconnect", () => {
    console.log(`Socket disconnected: ${socket.id}`);

    // Har room me check karo ki ye user kahan tha
    rooms.forEach((room, roomId) => {
      const peer = room.peers.get(socket.id);

      if (peer) {
        // 1. Saare Producers (Bheji hui videos) ko destroy karo
        peer.producers.forEach((producer) => {
          producer.close();
        });

        // 2. Saare Consumers (Dekhi hui videos) ko destroy karo
        peer.consumers.forEach((consumer) => {
          consumer.close();
        });

        // 3. Transports (Pipes) ko destroy karo
        if (peer.sendTransport) peer.sendTransport.close();
        if (peer.recvTransport) peer.recvTransport.close();

        // 4. User ko room se nikal do
        room.peers.delete(socket.id);
        console.log(
          `Peer ${socket.id} ki memory saaf kar di gayi! Room: ${roomId}`,
        );
      }
    });
  });
});

// Server aur Workers initialize karne ka sahi tarika
async function startServer() {
  try {
    // 1. Darwaza kholne se pehle C++ Workers ko create karke wait karo
    await createWorkers();

    // 2. Jab array me workers aa jayein, tab server ko listen karne bolo
    server.listen(3000, () => {
      console.log("Server is running on port 3000 AND Workers are ready!");
    });
  } catch (error) {
    console.error("Server start karne me dikkat aayi:", error);
  }
}

// Function call kar de
startServer();
