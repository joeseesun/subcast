// Turns speech into text for a note. The system dictation API is instant when the webview has it;
// otherwise a short recording goes through the same recogniser the subtitles use.
import { api, type CloudAsr } from "./api";
import { t } from "./i18n";
import { useStore } from "../store";

export interface Dictation {
  stop: () => Promise<string>;
  cancel: () => void;
}

interface SpeechResult {
  isFinal: boolean;
  0: { transcript: string };
}

interface SpeechEvent {
  results: ArrayLike<SpeechResult>;
}

interface SpeechRec {
  lang: string;
  continuous: boolean;
  interimResults: boolean;
  onstart: (() => void) | null;
  onresult: ((ev: SpeechEvent) => void) | null;
  onerror: ((ev: { error: string }) => void) | null;
  onend: (() => void) | null;
  start: () => void;
  stop: () => void;
  abort: () => void;
}

function speechCtor(): (new () => SpeechRec) | null {
  const w = window as unknown as { SpeechRecognition?: new () => SpeechRec; webkitSpeechRecognition?: new () => SpeechRec };
  return w.SpeechRecognition ?? w.webkitSpeechRecognition ?? null;
}

function beginWeb(): Promise<Dictation> {
  const Ctor = speechCtor();
  if (!Ctor) return Promise.reject(new Error("web-unavailable"));
  return new Promise((resolve, reject) => {
    const rec = new Ctor();
    rec.lang = navigator.language || "zh-CN";
    rec.continuous = true;
    rec.interimResults = true;
    let finalText = "";
    let interim = "";
    let settled = false;
    let ended: (() => void) | null = null;
    rec.onresult = (ev) => {
      interim = "";
      for (let i = 0; i < ev.results.length; i++) {
        const piece = ev.results[i][0]?.transcript ?? "";
        if (ev.results[i].isFinal) finalText = `${finalText} ${piece}`.trim();
        else interim = piece;
      }
    };
    rec.onend = () => { ended?.(); };
    const handle: Dictation = {
      stop: () => new Promise((done) => {
        ended = () => done(`${finalText} ${interim}`.trim());
        try { rec.stop(); } catch { done(`${finalText} ${interim}`.trim()); }
      }),
      cancel: () => { ended = null; try { rec.abort(); } catch { /* already stopped */ } },
    };
    rec.onerror = (ev) => {
      if (settled) return;
      settled = true;
      reject(new Error(ev.error === "not-allowed" || ev.error === "audio-capture" ? t("麦克风没有打开") : "web-unavailable"));
    };
    rec.onstart = () => { if (!settled) { settled = true; resolve(handle); } };
    try { rec.start(); } catch (e) { reject(e instanceof Error ? e : new Error("web-unavailable")); return; }
    window.setTimeout(() => { if (!settled) { settled = true; resolve(handle); } }, 700);
  });
}

function wavBase64(samples: Float32Array): string {
  const bytes = new ArrayBuffer(44 + samples.length * 2);
  const v = new DataView(bytes);
  const write = (o: number, s: string) => { for (let i = 0; i < s.length; i++) v.setUint8(o + i, s.charCodeAt(i)); };
  write(0, "RIFF");
  v.setUint32(4, 36 + samples.length * 2, true);
  write(8, "WAVEfmt ");
  v.setUint32(16, 16, true);
  v.setUint16(20, 1, true);
  v.setUint16(22, 1, true);
  v.setUint32(24, 16000, true);
  v.setUint32(28, 32000, true);
  v.setUint16(32, 2, true);
  v.setUint16(34, 16, true);
  write(36, "data");
  v.setUint32(40, samples.length * 2, true);
  for (let i = 0; i < samples.length; i++) {
    const s = Math.max(-1, Math.min(1, samples[i]));
    v.setInt16(44 + i * 2, s < 0 ? s * 0x8000 : s * 0x7fff, true);
  }
  const bin = new Uint8Array(bytes);
  let raw = "";
  for (let i = 0; i < bin.length; i += 0x8000) raw += String.fromCharCode(...bin.subarray(i, i + 0x8000));
  return btoa(raw);
}

function to16k(chunks: Float32Array[], from: number): Float32Array {
  const len = chunks.reduce((n, c) => n + c.length, 0);
  const joined = new Float32Array(len);
  let o = 0;
  for (const c of chunks) { joined.set(c, o); o += c.length; }
  if (from === 16000) return joined;
  const outLen = Math.max(1, Math.round(joined.length * 16000 / from));
  const out = new Float32Array(outLen);
  for (let i = 0; i < outLen; i++) {
    const x = i * from / 16000;
    const j = Math.floor(x);
    const f = x - j;
    const a = joined[j] ?? 0;
    const b = joined[Math.min(j + 1, joined.length - 1)] ?? a;
    out[i] = a + (b - a) * f;
  }
  return out;
}

async function beginWav(): Promise<Dictation> {
  let stream: MediaStream;
  try { stream = await navigator.mediaDevices.getUserMedia({ audio: true }); }
  catch { throw new Error(t("麦克风没有打开")); }
  const ctx = new AudioContext();
  const source = ctx.createMediaStreamSource(stream);
  const proc = ctx.createScriptProcessor(4096, 1, 1);
  const gain = ctx.createGain();
  gain.gain.value = 0;
  const chunks: Float32Array[] = [];
  let samples = 0;
  const cap = 16000 * 20;
  proc.onaudioprocess = (ev) => {
    if (samples >= cap) return;
    const data = new Float32Array(ev.inputBuffer.getChannelData(0));
    chunks.push(data);
    samples += Math.round(data.length * 16000 / (ctx.sampleRate || 16000));
  };
  source.connect(proc);
  proc.connect(gain);
  gain.connect(ctx.destination);
  let stopped = false;
  const shutdown = () => {
    if (stopped) return;
    stopped = true;
    proc.disconnect();
    source.disconnect();
    stream.getTracks().forEach((t) => t.stop());
    void ctx.close();
  };
  return {
    cancel: shutdown,
    stop: async () => {
      const rate = ctx.sampleRate || 16000;
      shutdown();
      const pcm = to16k(chunks, rate);
      if (pcm.length < 1600) throw new Error(t("没有听到声音"));
      const { settings } = useStore.getState();
      const cloud: CloudAsr | undefined = settings.asrEngine !== "local" ? settings.cloudAsr[settings.asrEngine] : undefined;
      return api.dictate(wavBase64(pcm), settings.asrModel, settings.asrLanguage || "auto", cloud);
    },
  };
}

/** Starts listening. `stop` resolves to the words; `cancel` throws them away. */
export async function beginDictation(): Promise<Dictation> {
  if (speechCtor()) {
    try { return await beginWeb(); }
    catch (e) { if (e instanceof Error && e.message === t("麦克风没有打开")) throw e; }
  }
  return beginWav();
}
