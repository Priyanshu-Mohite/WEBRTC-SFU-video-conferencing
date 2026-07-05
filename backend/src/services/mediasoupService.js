import { mediaCodecs } from "../config/mediasoupConfig.js";

const router = await worker.createRouter({
    mediaCodecs,
});