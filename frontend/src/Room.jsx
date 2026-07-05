import { useEffect, useRef, useState } from "react";
import { io } from "socket.io-client";
import { Device } from "mediasoup-client";

const Room = () => {
  const [device, setDevice] = useState(null);
  // Transport ko state me save karenge taaki baad me video bhej sakein
  const [sendTransport, setSendTransport] = useState(null);
  const socketRef = useRef(null);

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

        transport.on(
          "connect",
          async ({ dtlsParameters }, callback, errback) => {
            console.log("--- @connect fired! Sending DTLS to Backend ---");
            try {
              // Backend ka 'transport-connect' fire karo aur wait karo
              socketRef.current.emit(
                "transport-connect",
                { roomId, dtlsParameters },
                () => {
                  // Success aate hi local transport ko bata do ki connection done
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

  // PHASE 5: THE TRIGGER (Yeh naya function add kar)
  const startWebcam = async () => {
    try {
      console.log("Webcam access maang raha hu...");
      const stream = await navigator.mediaDevices.getUserMedia({
        video: true,
        audio: false,
      });

      // Stream me se raw video track nikal
      const videoTrack = stream.getVideoTracks()[0];

      // THE TRIGGER: Transport me track daal do
      // Jaise hi ye line chalegi, @connect aur @produce lagatar fire honge!
      const producer = await sendTransport.produce({ track: videoTrack });
      console.log(
        "BINGOO! Local Producer Created & Video is flowing! ID:",
        producer.id,
      );

      // --- NAYA DEBUG CODE ---
      // Har 2 second mein check karega ki kitna data server pe bheja gaya
      setInterval(async () => {
        const stats = await producer.getStats();
        stats.forEach((stat) => {
          if (stat.type === "outbound-rtp" && stat.kind === "video") {
            console.log(`Video Bytes Sent to Backend: ${stat.bytesSent}`);
          }
        });
      }, 2000);

      // Video ko screen pe dikhane ke liye (optional DOM attach)
      document.getElementById("localVideo").srcObject = stream;
    } catch (error) {
      console.error("Camera access failed or produce failed:", error);
    }
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
          <video
            id="localVideo"
            autoPlay
            muted
            playsInline
            style={{ width: "300px", border: "2px solid black" }}
          ></video>
        </>
      )}
    </div>
  );
};

export default Room;
