// Notes a person keeps on one episode: a line copied from the subtitles, guide or Q&A, or something they typed or said.
import { create } from "zustand";
import { api } from "./api";
import { t } from "./i18n";

export type NoteSource = "transcript" | "summary" | "chat" | "deep" | "typed" | "voice";

export interface EpisodeNote {
  id: string;
  text: string;
  source: NoteSource;
  ts?: string;
  seconds?: number;
  createdAt: number;
}

const KIND = "user-notes";
const EMPTY: EpisodeNote[] = [];

interface NotesState {
  byId: Record<string, EpisodeNote[]>;
  loaded: Record<string, boolean>;
}

export const useNotes = create<NotesState>(() => ({ byId: {}, loaded: {} }));

export function useEpisodeNotes(id: string): EpisodeNote[] {
  return useNotes((s) => s.byId[id] ?? EMPTY);
}

export function sourceLabel(s: NoteSource): string {
  if (s === "transcript") return t("字幕");
  if (s === "summary") return t("导读");
  if (s === "chat") return t("问答");
  if (s === "deep") return t("精读");
  if (s === "voice") return t("语音");
  return t("自己写的");
}

function parse(raw: string | null): EpisodeNote[] {
  if (!raw) return [];
  try {
    const v: unknown = JSON.parse(raw);
    if (!Array.isArray(v)) return [];
    return v.filter((n): n is EpisodeNote => !!n && typeof n.id === "string" && typeof n.text === "string" && typeof n.source === "string");
  } catch {
    return [];
  }
}

let chain = Promise.resolve();
function enqueue(job: () => Promise<void>): Promise<void> {
  const run = chain.then(job, job);
  chain = run.then(() => undefined, () => undefined);
  return run;
}

async function current(id: string): Promise<EpisodeNote[]> {
  const s = useNotes.getState();
  if (s.loaded[id]) return s.byId[id] ?? [];
  return parse(await api.getBlob(id, KIND));
}

function put(id: string, notes: EpisodeNote[]) {
  useNotes.setState((s) => ({ byId: { ...s.byId, [id]: notes }, loaded: { ...s.loaded, [id]: true } }));
}

export function loadNotes(id: string): Promise<void> {
  return enqueue(async () => {
    if (useNotes.getState().loaded[id]) return;
    put(id, parse(await api.getBlob(id, KIND)));
  });
}

export function addNote(id: string, draft: { text: string; source: NoteSource; ts?: string; seconds?: number }): Promise<void> {
  const text = draft.text.trim().slice(0, 4000);
  if (!text) return Promise.resolve();
  return enqueue(async () => {
    const note: EpisodeNote = {
      id: `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`,
      createdAt: Date.now(),
      text,
      source: draft.source,
      ts: draft.ts,
      seconds: draft.seconds,
    };
    const notes = [note, ...(await current(id))];
    await api.setBlob(id, KIND, JSON.stringify(notes));
    put(id, notes);
  });
}

export function removeNote(id: string, noteId: string): Promise<void> {
  return enqueue(async () => {
    const notes = (await current(id)).filter((n) => n.id !== noteId);
    await api.setBlob(id, KIND, JSON.stringify(notes));
    put(id, notes);
  });
}
