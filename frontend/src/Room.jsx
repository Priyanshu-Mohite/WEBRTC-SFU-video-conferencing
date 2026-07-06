import { useEffect, useRef, useState } from "react";
import { io } from "socket.io-client";
import { Device } from "mediasoup-client";

const Room = () => {
  const [device, setDevice] = useState(null);
  // Transport ko state me save karenge taaki baad me video bhej sakein
  const [sendTransport, setSendTransport] = useState(null);
  const [recvTransport, setRecvTransport] = useState(null);
  const socketRef = useRef(null);
  const localVideoRef = useRef(null);
  const remoteVideoRef = useRef(null);

  useEffect(() => {
    socketRef.current = io("http://localhost:3000");

    socketRef.current.on("connect", () => {
      console.log("Socket connected:", socketRef.current.id);
      joinRoomAndLoadDevice();
    });

    return () => {
      if (socketRef.current) socketRef.current.disconnect();
    };
  }, []);

  // AUTOMATION RADAR: Backend se aane wale naye videos ko sunna
  useEffect(() => {
    // Agar socket ya Recv pipe ready nahi hai, toh kuch mat kar
    if (!socketRef.current || !recvTransport) return;

    const handleNewProducer = ({ producerId }) => {
      console.log("🚨 Room me naya video detect hua! ID:", producerId);
      // Nayi ID milte hi turant consumeMedia function ko background me chala do
      consumeMedia(producerId);
    };

    socketRef.current.on("new-producer", handleNewProducer);

    return () => {
      socketRef.current.off("new-producer", handleNewProducer);
    };
  }, [recvTransport]); // Yeh effect tabhi active hoga jab Recv pipe ban jayegi

  const joinRoomAndLoadDevice = () => {
    const roomId = "test-room-1";

    socketRef.current.emit("joinRoom", { roomId }, async (response) => {
      if (response.error) return console.error(response.error);

      try {
        const newDevice = new Device();
        await newDevice.load({
          routerRtpCapabilities: response.routerRtpCapabilities,
        });
        setDevice(newDevice);
        console.log("Phase 2 Done: Device Loaded!");
      } catch (error) {
        console.error("Error loading device:", error);
      }
    });
  };

  // Phase 3 & 4: Transport (Empty Pipe) Banana aur Traps lagana
  const createWebRtcTransport = () => {
    const roomId = "test-room-1";

    // 1. Backend ko bol pipe (Transport) banane ke liye
    socketRef.current.emit(
      "createWebRtcTransport",
      { roomId },
      async (response) => {
        if (response.error) return console.error(response.error);

        const { params } = response;
        console.log("Backend se 4 parameters aaye:", params);

        // 2. Frontend pe apni taraf ki pipe bana (Phase 3)
        const transport = device.createSendTransport(params);

        // 3. Traps set karna (Phase 4) - Ye abhi fire nahi honge, bas wait karenge

        // Trap A: Security Handshake ke liye
        // transport.on(
        //   "connect",
        //   async ({ dtlsParameters }, callback, errback) => {
        //     console.log("--- Event: @connect fired! ---");
        //     try {
        //       // Yahan hum Socket.io se backend ko dtlsParameters bhejenge lock karne ke liye
        //       // (Iska logic hum next step me backend me likhenge)
        //       // callback() chalane se transport ko pata chalega ki handshake done!
        //       // callback();
        //     } catch (error) {
        //       errback(error);
        //     }
        //   },
        // );

        // Tere existing createWebRtcTransport me ye update kar
        transport.on(
          "connect",
          async ({ dtlsParameters }, callback, errback) => {
            try {
              socketRef.current.emit(
                "transport-connect",
                { roomId, dtlsParameters, isSend: true }, // <-- YE FLAG ADD KAR
                () => {
                  callback();
                },
              );
            } catch (error) {
              errback(error);
            }
          },
        );

        // Trap B: Actual media (video/audio) flow start karne ke liye
        // transport.on("produce", async (parameters, callback, errback) => {
        //   console.log("--- Event: @produce fired! ---");
        //   try {
        //     // Yahan hum rtpParameters backend ko bhejenge ek Producer banane ke liye
        //     // callback({ id: serverProducerId });
        //   } catch (error) {
        //     errback(error);
        //   }
        // });

        transport.on("produce", async (parameters, callback, errback) => {
          console.log("--- @produce fired! Sending RTP Params to Backend ---");
          try {
            // Backend ko bolo Producer banaye
            socketRef.current.emit(
              "transport-produce",
              {
                roomId,
                kind: parameters.kind,
                rtpParameters: parameters.rtpParameters,
              },
              ({ id }) => {
                // Backend se Producer ID mili, ab pipe puri jud gayi!
                callback({ id });
              },
            );
          } catch (error) {
            errback(error);
          }
        });

        setSendTransport(transport);
        console.log(
          "Phase 3 & 4 Done: Frontend Send Transport ready aur Listeners lag gaye!",
        );
      },
    );
  };

  // PHASE 1 (Consumer): Receive Transport (Empty Pipe) Banana aur Trap lagana
  const createRecvTransport = () => {
    const roomId = "test-room-1";

    // 1. Backend ko bol Recv pipe banane ke liye
    socketRef.current.emit(
      "createRecvTransport",
      { roomId },
      async (response) => {
        if (response.error) return console.error(response.error);

        const { params } = response;
        console.log("Backend se Recv parameters aaye:", params);

        // 2. Frontend pe apni taraf ki Recv pipe bana (Phase 1)
        const transport = device.createRecvTransport(params);

        // 3. Security Handshake Trap set karna
        transport.on(
          "connect",
          async ({ dtlsParameters }, callback, errback) => {
            console.log(
              "--- @connect fired on RECV Transport! Sending DTLS ---",
            );
            try {
              socketRef.current.emit(
                "transport-connect",
                { roomId, dtlsParameters, isSend: false }, // <-- isSend: false (Consumer)
                () => {
                  callback(); // Rasta saaf hai!
                },
              );
            } catch (error) {
              errback(error);
            }
          },
        );

        // (Yahan @produce event NAHI hota kyunki client recv transport se kuch bhejta nahi hai)

        setRecvTransport(transport);
        console.log(
          "Phase 1 Done: Frontend Recv Transport ready aur Listener lag gaya!",
        );

        socketRef.current.emit("get-producers", { roomId }, (producerIds) => {
          console.log(
            "Room me pehle se ye videos chal rahi hain:",
            producerIds,
          );
          // Har existing video ke liye automate consume chala do
          producerIds.forEach((id) => {
            // consumeMedia(id);
            consumeMedia(id, transport, device);
          });
        });
      },
    );
  };

  // PHASE 5: THE TRIGGER (Yeh naya function add kar)
  // const startWebcam = async () => {
  //   try {
  //     console.log("Webcam access maang raha hu...");
  //     const stream = await navigator.mediaDevices.getUserMedia({
  //       video: true,
  //       audio: false,
  //     });

  //     // Stream me se raw video track nikal
  //     const videoTrack = stream.getVideoTracks()[0];

  //     // THE TRIGGER: Transport me track daal do
  //     // Jaise hi ye line chalegi, @connect aur @produce lagatar fire honge!
  //     const producer = await sendTransport.produce({ track: videoTrack });
  //     console.log(
  //       "BINGOO! Local Producer Created & Video is flowing! ID:",
  //       producer.id,
  //     );

  //     // --- NAYA DEBUG CODE ---
  //     // Har 2 second mein check karega ki kitna data server pe bheja gaya
  //     setInterval(async () => {
  //       const stats = await producer.getStats();
  //       stats.forEach((stat) => {
  //         if (stat.type === "outbound-rtp" && stat.kind === "video") {
  //           console.log(`Video Bytes Sent to Backend: ${stat.bytesSent}`);
  //         }
  //       });
  //     }, 2000);

  //     // Video ko screen pe dikhane ke liye (optional DOM attach)
  //     document.getElementById("localVideo").srcObject = stream;
  //   } catch (error) {
  //     console.error("Camera access failed or produce failed:", error);
  //   }
  // };

  // PHASE 5: THE TRIGGER
  // const startWebcam = async () => {
  //   try {
  //     console.log("Webcam access maang raha hu...");
  //     const stream = await navigator.mediaDevices.getUserMedia({
  //       video: true,
  //       audio: false,
  //     });

  //     // 1. Stream me se raw video track nikal
  //     const videoTrack = stream.getVideoTracks()[0];

  //     // 2. FIX: Video ko turant screen pe dikha de (backend ka wait mat kar)
  //     document.getElementById("localVideo").srcObject = stream;

  //     // 3. THE TRIGGER: Ab aaram se backend transport me track daal do
  //     const producer = await sendTransport.produce({ track: videoTrack });
  //     console.log(
  //       "BINGOO! Local Producer Created & Video is flowing! ID:",
  //       producer.id,
  //     );

  //     // --- NAYA DEBUG CODE ---
  //     setInterval(async () => {
  //       const stats = await producer.getStats();
  //       stats.forEach((stat) => {
  //         if (stat.type === "outbound-rtp" && stat.kind === "video") {
  //           console.log(`Video Bytes Sent to Backend: ${stat.bytesSent}`);
  //         }
  //       });
  //     }, 2000);

  //   } catch (error) {
  //     console.error("Camera access failed or produce failed:", error);
  //   }
  // };

  // PHASE 5: THE TRIGGER
  const startWebcam = async () => {
    try {
      console.log("Webcam access maang raha hu...");
      const stream = await navigator.mediaDevices.getUserMedia({
        video: true,
        audio: false,
      });

      // 1. Stream me se raw video track nikal
      const videoTrack = stream.getVideoTracks()[0];

      // 2. THE PROPER REACT FIX: useRef ke through stream attach karna
      if (localVideoRef.current) {
        localVideoRef.current.srcObject = stream;
      }

      // 3. Backend me raw track bhejna
      const producer = await sendTransport.produce({ track: videoTrack });
      console.log(
        "BINGOO! Local Producer Created & Video is flowing! ID:",
        producer.id,
      );

      // --- DEBUG CODE ---
      // setInterval(async () => {
      //   const stats = await producer.getStats();
      //   stats.forEach((stat) => {
      //     if (stat.type === "outbound-rtp" && stat.kind === "video") {
      //       console.log(`Video Bytes Sent to Backend: ${stat.bytesSent}`);
      //     }
      //   });
      // }, 2000);
    } catch (error) {
      console.error("Camera access failed or produce failed:", error);
    }
  };

  // PHASE 3 & 4 (Consumer): Media Receive karke Screen pe chalana
  // const consumeMedia = () => {
  //   const roomId = "test-room-1";

  //   // 1. Backend se pucho ki kiski video available hai
  //   socketRef.current.emit("get-producers", { roomId }, (producerIds) => {
  //     if (producerIds.length === 0) {
  //       return console.log("Room me koi active video nahi hai bhai!");
  //     }

  //     // Testing ke liye pehli available video utha rahe hain
  //     const producerId = producerIds[0];
  //     console.log(`Mujhe iski video dekhni hai: ${producerId}`);

  //     // 2. Backend ke canConsume check (Phase 2) ke liye request bhejo
  //     socketRef.current.emit(
  //       "consume",
  //       {
  //         roomId,
  //         producerId: producerId,
  //         rtpCapabilities: device.rtpCapabilities,
  //       },
  //       async (response) => {
  //         if (response.error) return console.error(response.error);

  //         const { params } = response;

  //         // 3. PHASE 3: Frontend Consumer Banana (Ye chalte hi DTLS handshake fire hoga!)
  //         const consumer = await recvTransport.consume({
  //           id: params.id,
  //           producerId: params.producerId,
  //           kind: params.kind,
  //           rtpParameters: params.rtpParameters,
  //         });

  //         // 4. THE TRACK IS HERE: Backend ki ruki hui (paused) video ka track mil gaya
  //         const { track } = consumer;
  //         console.log("Client Consumer ready! Track mil gaya:", track.id);

  //         // 5. DOM Render: useRef ke through naye video tag me track attach karna
  //         const stream = new MediaStream([track]);
  //         if (remoteVideoRef.current) {
  //           remoteVideoRef.current.srcObject = stream;
  //         }

  //         // 6. PHASE 4 (The Resume Signal): Backend ke C++ Worker ko Un-pause karo
  //         socketRef.current.emit("consumer-resume", {
  //           roomId,
  //           consumerId: consumer.id,
  //         });

  //         console.log("Bingo! Play button dab gaya, video flow chalu!");
  //       },
  //     );
  //   });
  // };

  // PHASE 3 & 4: Naya Dynamic Consume Function
  // const consumeMedia = (producerId) => {
  //   const roomId = "test-room-1";
  //   console.log(`Automated Consume Triggered for ID: ${producerId}`);

  //   // Seedha backend ke canConsume check (Phase 2) ke liye request bhejo
  //   socketRef.current.emit(
  //     "consume",
  //     {
  //       roomId,
  //       producerId: producerId,
  //       rtpCapabilities: device.rtpCapabilities,
  //     },
  //     async (response) => {
  //       if (response.error) return console.error(response.error);

  //       const { params } = response;

  //       // Frontend Consumer Banana (DTLS handshake fire hoga)
  //       const consumer = await recvTransport.consume({
  //         id: params.id,
  //         producerId: params.producerId,
  //         kind: params.kind,
  //         rtpParameters: params.rtpParameters,
  //       });

  //       const { track } = consumer;
  //       console.log("Client Consumer ready! Track mil gaya:", track.id);

  //       // DOM Render: Naye video tag me track attach karna
  //       const stream = new MediaStream([track]);
  //       if (remoteVideoRef.current) {
  //         remoteVideoRef.current.srcObject = stream;
  //       }

  //       // Backend ke C++ Worker ko Un-pause karo
  //       socketRef.current.emit("consumer-resume", {
  //         roomId,
  //         consumerId: consumer.id,
  //       });

  //       console.log("Bingo! Play button dab gaya, video flow chalu!");
  //     },
  //   );
  // };

  // Parameter me transport aur currentDevice accept kar
const consumeMedia = (producerId, transport, currentDevice) => {
  const roomId = "test-room-1";
  console.log(`Automated Consume Triggered for ID: ${producerId}`);

  // Ab yahan outer state device ki jagah passed currentDevice use hoga
  socketRef.current.emit(
    "consume",
    {
      roomId,
      producerId: producerId,
      rtpCapabilities: currentDevice.rtpCapabilities, 
    },
    async (response) => {
      if (response.error) return console.error(response.error);

      const { params } = response;

      // Outer state ki jagah passed transport parameter use hoga
      const consumer = await transport.consume({
        id: params.id,
        producerId: params.producerId,
        kind: params.kind,
        rtpParameters: params.rtpParameters,
      });

      const { track } = consumer;
      console.log("Client Consumer ready! Track mil gaya:", track.id);

      const stream = new MediaStream([track]);
      if (remoteVideoRef.current) {
        remoteVideoRef.current.srcObject = stream;
      }

      socketRef.current.emit("consumer-resume", {
        roomId,
        consumerId: consumer.id,
      });

      console.log("Bingo! Play button dab gaya, video flow chalu!");
    },
  );
};

  return (
    <div style={{ padding: "20px", fontFamily: "sans-serif" }}>
      <h2>SFU Video Room</h2>
      <p>Device Status: {device ? " Ready \u2705" : " Loading..."}</p>

      {/* Jab device load ho jaye, tabhi ye button dikhega */}
      {device && !sendTransport && (
        <button
          onClick={createWebRtcTransport}
          style={{ padding: "10px", cursor: "pointer" }}
        >
          Create Send Transport (Make Empty Pipe)
        </button>
      )}

      {sendTransport && (
        <>
          <p>Send Transport Status: Ready ✅</p>
          <button
            onClick={startWebcam}
            style={{
              padding: "10px",
              cursor: "pointer",
              background: "green",
              color: "white",
            }}
          >
            Start Webcam (Fire The Trigger!)
          </button>
          <br />
          <br />
        </>
      )}

      {device && !recvTransport && (
        <button
          onClick={createRecvTransport}
          style={{
            padding: "10px",
            margin: "10px",
            cursor: "pointer",
            background: "blue",
            color: "white",
          }}
        >
          Create Recv Transport (Consumer Pipe)
        </button>
      )}

      <br />
      <br />

      <video
        ref={localVideoRef} // Yeh change kiya
        autoPlay
        muted
        playsInline
        style={{ width: "300px", border: "2px solid black" }}
      ></video>

      <br />
      <br />

      {/* Naya Button aur Video Tag */}
      {/* {recvTransport && (
        <>
          <button
            // onClick={consumeMedia}
            style={{
              padding: "10px",
              margin: "10px",
              cursor: "pointer",
              background: "purple",
              color: "white",
            }}
          >
            Consume Media (Get Remote Video)
          </button>

          <br />
          <h4>Remote User Video:</h4>
          <video
            ref={remoteVideoRef}
            autoPlay
            playsInline
            style={{ width: "300px", border: "2px solid red" }}
          ></video>
        </>
      )} */}

      {/* Naya Video Tag (Bina button ke) */}
      {recvTransport && (
        <>
          <br />
          <h4>Remote User Video:</h4>
          <video
            ref={remoteVideoRef}
            autoPlay
            playsInline
            style={{ width: "300px", border: "2px solid red" }}
          ></video>
        </>
      )}
    </div>
  );
};

export default Room;
