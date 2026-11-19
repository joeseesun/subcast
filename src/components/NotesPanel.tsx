import { useEffect, useRef, useState } from "react";
import { Mic, Square, Trash2 } from "lucide-react";
import { errorText, type Episode } from "../lib/api";
import { beginDictation, type Dictation } from "../lib/dictate";
import { t } from "../lib/i18n";
import { addNote, loadNotes, removeNote, sourceLabel, useEpisodeNotes, type NoteSource } from "../lib/notes";
import { seekSoon, useStore } from "../store";

/** The episode's own notes: lines taken from the other tabs, plus anything typed or spoken here. */
export function NotesPanel({ ep }: { ep: Episode }) {
  const notes = useEpisodeNotes(ep.id);
  const [input, setInput] = useState("");
  const [fromVoice, setFromVoice] = useState(false);
  const [hearing, setHearing] = useState(false);
  const dict = useRef<Dictation | null>(null);
  const box = useRef<HTMLTextAreaElement>(null);
  useEffect(() => {
    void loadNotes(ep.id);
    return () => { dict.current?.cancel(); dict.current = null; };
  }, [ep.id]);

  const seek = (seconds: number) => {
    const s = useStore.getState();
    if (s.current?.id === ep.id) s.player?.seek(seconds);
    else { s.load({ ...ep, progress: seconds }); seekSoon(seconds); }
  };
  const save = async (source: NoteSource) => {
    const text = input.trim();
    if (!text || hearing) return;
    setInput("");
    setFromVoice(false);
    try { await addNote(ep.id, { text, source }); }
    catch (e) { useStore.getState().notify(errorText(e)); }
  };
  const listen = async () => {
    if (dict.current) {
      const handle = dict.current;
      dict.current = null;
      setHearing(false);
      try {
        const text = (await handle.stop()).trim();
        if (!text) { useStore.getState().notify(t("没有听到声音")); return; }
        setInput((prev) => (prev.trim() ? `${prev.trim()} ${text}` : text));
        setFromVoice(true);
        box.current?.focus();
      } catch (e) { useStore.getState().notify(errorText(e)); }
      return;
    }
    setHearing(true);
    try { dict.current = await beginDictation(); }
    catch (e) { setHearing(false); useStore.getState().notify(errorText(e)); }
  };

  return (
    <div className="notes-panel">
      <div className="notes-scroll">
        {notes.length === 0 && (
          <div className="panel-empty">
            <p className="lead">{t("还没有笔记")}</p>
            <p className="muted small">{t("在字幕、导读或问答里选中一句，点「记到笔记」。也可以在下面打字或说话。")}</p>
          </div>
        )}
        {notes.map((n) => (
          <article key={n.id} className="note-card">
            <p>{n.text}</p>
            <div className="note-meta">
              <span>{sourceLabel(n.source)}</span>
              {n.seconds != null && <button type="button" onClick={() => seek(n.seconds!)}>{n.ts || formatTs(n.seconds)}</button>}
              <span className="spacer" />
              <button type="button" title={t("删除这条笔记")} aria-label={t("删除这条笔记")} onClick={() => void removeNote(ep.id, n.id).catch((e) => useStore.getState().notify(errorText(e)))}><Trash2 size={13} /></button>
            </div>
          </article>
        ))}
      </div>
      <form className="composer" onSubmit={(e) => { e.preventDefault(); void save(fromVoice ? "voice" : "typed"); }}>
        <textarea
          ref={box}
          value={input}
          rows={1}
          placeholder={hearing ? t("正在听…") : t("写一句，或点麦克风说")}
          onChange={(e) => { setInput(e.target.value); setFromVoice(false); }}
          onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) { e.preventDefault(); void save(fromVoice ? "voice" : "typed"); } }}
        />
        <div className="composer-bar">
          <button type="button" className={`icon-btn sm ${hearing ? "on" : ""}`} title={hearing ? t("停止并记下") : t("语音")} aria-pressed={hearing} onClick={() => void listen()}>
            {hearing ? <Square size={13} /> : <Mic size={15} />}
          </button>
          <span className="spacer" />
          <button type="submit" className="btn sm primary" disabled={!input.trim() || hearing}>{t("保存")}</button>
        </div>
      </form>
    </div>
  );
}

function formatTs(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds));
  const m = Math.floor(s / 60);
  return `${m}:${String(s % 60).padStart(2, "0")}`;
}
