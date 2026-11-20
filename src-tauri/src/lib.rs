mod appupdate;
mod asr;
mod anki;
mod asr_context;
mod clip;
mod agents;
mod cloud;
mod db;
mod edge_tts;
mod fastclip;
mod feeds;
mod hlsproxy;
mod import;
mod llm;
mod local_tts;
mod model;
mod net;
mod align;
mod offline;
mod shadow;
#[cfg_attr(debug_assertions, allow(dead_code))] // serving the interface is release-only
mod ota;
mod qiaomu;
mod shared;
mod speech;
mod transcript;
mod translate;
mod videoclip;
mod youtube;
mod ytlogin;

use model::*;
use rusqlite::Connection;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tauri::{AppHandle, Emitter, Manager, State};

/// The port the bundled app serves its pages from. YouTube's embedded player refuses pages served from the
/// `tauri://` scheme (no valid origin / referrer), so release builds use http://localhost like a normal website.
#[cfg_attr(debug_assertions, allow(dead_code))]
const LOCAL_PORT: u16 = 14327;
/// How many ports from LOCAL_PORT the interface may use (each listed in capabilities/default.json).
#[cfg_attr(debug_assertions, allow(dead_code))]
const UI_PORTS: u16 = 20;

pub struct AppState {
    db: Mutex<Connection>,
    data: PathBuf,
    jobs: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl AppState {
    fn job(&self, id: &str) -> Arc<AtomicBool> {
        let flag = Arc::new(AtomicBool::new(false));
        self.jobs.lock().unwrap().insert(id.to_string(), flag.clone());
        flag
    }
    /// Ends a job: only its own entry, so a job that was cancelled and then started again isn't forgotten
    /// (and can still be cancelled) when the old run finishes.
    fn done_with(&self, id: &str, flag: &Arc<AtomicBool>) {
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.get(id).is_some_and(|f| Arc::ptr_eq(f, flag)) { jobs.remove(id); }
    }
    /// Whether a job with this id is running and not cancelled.
    fn running(&self, id: &str) -> bool { self.jobs.lock().unwrap().get(id).is_some_and(|f| !f.load(Ordering::Relaxed)) }
}

type Res<T> = Result<T, String>;
fn e<E: std::fmt::Display>(err: E) -> String { err.to_string() }

// ---- subscriptions ---------------------------------------------------------------------------------

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Added { show: Option<Show>, episode: Option<Episode> }

fn store(state: &AppState, show: &Show, eps: &[Episode]) -> Res<()> {
    let mut conn = state.db.lock().unwrap();
    db::upsert_show(&conn, show).map_err(e)?;
    db::upsert_episodes(&mut conn, eps).map_err(e)?;
    Ok(())
}

/// Fetches any subscription's latest state. 乔木精读 channels also bring curated Chinese titles, which go straight
/// into the translation cache so the UI shows them instead of machine translations.
async fn fetch_any(state: &AppState, feed_url: &str, kind: &str) -> anyhow::Result<(Show, Vec<Episode>)> {
    if kind == "qiaomu" {
        let ch = if let Some(id) = qiaomu::rss_source_id(feed_url) { qiaomu::rss_channel(&id).await? } else {
            let id = qiaomu::site_id(feed_url).ok_or_else(|| anyhow::anyhow!("不是精读频道"))?;
            qiaomu::channel(&id).await?
        };
        let conn = state.db.lock().unwrap();
        // One transaction for the few hundred titles, not one commit each.
        if let Ok(tx) = conn.unchecked_transaction() {
            for (orig, zh) in &ch.zh_titles { let _ = db::tr_put(&tx, &short_hash(&format!("zh-CN|{orig}"), 24), zh); }
            let _ = tx.commit();
        }
        return Ok((ch.show, ch.episodes));
    }
    feeds::fetch_show(feed_url, kind).await
}

/// The channel feed for a video that was opened on its own (pasted link), so the episode page can subscribe.
#[tauri::command]
async fn youtube_channel_feed(video_id: String) -> Res<String> {
    let info = youtube::video_info(&video_id).await.map_err(e)?;
    let id = info.channel_id.filter(|id| id.starts_with("UC") && id.len() == 24).ok_or("没有在这个页面找到 YouTube 频道")?;
    Ok(feeds::channel_feed(&id))
}

async fn single_youtube(state: &AppState, video_id: &str) -> Res<Episode> {
    let info = youtube::video_info(video_id).await.map_err(e)?;
    let ep = Episode {
        id: short_hash(&format!("single:{video_id}"), 20),
        show_id: "single".into(),
        guid: video_id.into(),
        title: if info.title.is_empty() { video_id.to_string() } else { info.title },
        published: Some(now_ms()),
        duration: info.duration,
        video_id: Some(video_id.into()),
        link: Some(format!("https://www.youtube.com/watch?v={video_id}")),
        cover: Some(format!("https://i.ytimg.com/vi/{video_id}/hqdefault.jpg")),
        notes: Some(format!("{}\n\n{}", info.author, info.description)),
        ..Default::default()
    };
    let mut conn = state.db.lock().unwrap();
    db::upsert_episodes(&mut conn, std::slice::from_ref(&ep)).map_err(e)?;
    Ok(db::get_episode(&conn, &ep.id).map_err(e)?.unwrap_or(ep))
}

enum Fetched { Show(Show, Vec<Episode>), Single(Episode) }

/// Reads whatever the user pasted (or a curated channel) without subscribing.
async fn fetch_input(state: &AppState, input: &str) -> Res<Fetched> {
    let normalized = if input.trim().starts_with("http") { input.trim().to_string() } else { format!("https://{}", input.trim()) };
    if let Some(id) = qiaomu::rss_source_id(&normalized) {
        let (show, eps) = fetch_any(state, &qiaomu::rss_feed_url(&id), "qiaomu").await.map_err(e)?;
        return Ok(Fetched::Show(show, eps));
    }
    if let Some(id) = qiaomu::site_id(&normalized) {
        let (show, eps) = fetch_any(state, &qiaomu::feed_url(&id), "qiaomu").await.map_err(e)?;
        return Ok(Fetched::Show(show, eps));
    }
    let (show, eps) = match feeds::resolve(input).await.map_err(e)? {
        feeds::Source::YouTubeVideo(id) => return Ok(Fetched::Single(single_youtube(state, &id).await?)),
        feeds::Source::XiaoyuzhouEpisode(url) => {
            let mut ep = feeds::xiaoyuzhou_episode(&url).await.map_err(e)?;
            ep.id = short_hash(&format!("single:{}", ep.guid), 20);
            let mut conn = state.db.lock().unwrap();
            db::upsert_episodes(&mut conn, std::slice::from_ref(&ep)).map_err(e)?;
            let ep = db::get_episode(&conn, &ep.id).map_err(e)?.unwrap_or(ep);
            return Ok(Fetched::Single(ep));
        }
        feeds::Source::Xiaoyuzhou(url) => feeds::xiaoyuzhou_show(&url).await.map_err(e)?,
        feeds::Source::Feed(url) => match feeds::fetch_show(&url, "podcast").await {
            Ok(r) if !r.1.is_empty() => r,
            // A channel with no long-form videos: fall back to everything it uploads.
            other if url.contains("playlist_id=UULF") => {
                let all = url.replace("playlist_id=UULF", "channel_id=UC");
                match feeds::fetch_show(&all, "podcast").await { Ok(r) => r, Err(err) => return Err(other.err().map(e).unwrap_or_else(|| e(err))) }
            }
            other => other.map_err(e)?,
        },
    };
    if eps.is_empty() { return Err("这个订阅源里没有可播放的节目".into()); }
    Ok(Fetched::Show(show, eps))
}

/// YouTube's feed only carries a video thumbnail. The sidebar should show the channel picture instead.
/// Once that picture is stored, later refreshes keep it and do not download the channel page again.
async fn with_channel_avatar(mut show: Show, existing: Option<String>) -> Show {
    if show.kind != "youtube" { return show; }
    if existing.as_deref().is_some_and(|c| c.starts_with("data:image/")) {
        show.cover = existing;
        return show;
    }
    if let Ok(Some(avatar)) = tokio::time::timeout(std::time::Duration::from_secs(8), feeds::youtube_channel_avatar(&show.feed_url)).await {
        show.cover = Some(avatar);
    }
    show
}

#[tauri::command]
async fn add_subscription(state: State<'_, AppState>, input: String) -> Res<Added> {
    match fetch_input(&state, &input).await? {
        Fetched::Single(ep) => Ok(Added { show: None, episode: Some(ep) }),
        Fetched::Show(show, eps) => {
            let show = with_channel_avatar(show, None).await;
            store(&state, &show, &eps)?;
            Ok(Added { show: Some(show), episode: None })
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Preview { show: Show, episodes: Vec<Episode>, fetched_at: i64 }

/// The id a channel input will have, when that can be known without the network (curated 精读 sites, direct feeds).
fn known_show_id(input: &str) -> Option<String> {
    let normalized = if input.trim().starts_with("http") { input.trim().to_string() } else { format!("https://{}", input.trim()) };
    if let Some(id) = qiaomu::rss_source_id(&normalized) { return Some(short_hash(&qiaomu::rss_feed_url(&id), 16)); }
    if let Some(id) = qiaomu::site_id(&normalized) { return Some(short_hash(&qiaomu::feed_url(&id), 16)); }
    Some(short_hash(input.trim(), 16))
}

/// A channel page from what was saved last time, so it opens instantly; the page then refreshes it in the background.
#[tauri::command(async)]
fn cached_show(state: State<'_, AppState>, input: String) -> Res<Option<Preview>> {
    let Some(id) = known_show_id(&input) else { return Ok(None) };
    let conn = state.db.lock().unwrap();
    let Some(json) = db::get_blob(&conn, &id, "show-json").map_err(e)? else { return Ok(None) };
    let Ok(show) = serde_json::from_str::<Show>(&json) else { return Ok(None) };
    let fetched_at = db::get_blob(&conn, &id, "fetched-at").map_err(e)?.and_then(|t| t.parse().ok()).unwrap_or(0);
    let episodes = db::list_episodes(&conn, Some(&id), 300).map_err(e)?;
    if episodes.is_empty() { return Ok(None); }
    Ok(Some(Preview { show, episodes, fetched_at }))
}

/// A channel page before subscribing: its episodes are saved (so they can be opened and played) but the show is
/// not, so it stays out of the sidebar and the home feed until the user subscribes.
#[tauri::command]
async fn preview_show(state: State<'_, AppState>, input: String) -> Res<Preview> {
    match fetch_input(&state, &input).await? {
        Fetched::Single(_) => Err("这是单集链接，不是频道".into()),
        Fetched::Show(show, eps) => {
            let mut conn = state.db.lock().unwrap();
            db::upsert_episodes(&mut conn, &eps).map_err(e)?;
            let now = now_ms();
            if let Ok(json) = serde_json::to_string(&show) { let _ = db::set_blob(&conn, &show.id, "show-json", &json); }
            let _ = db::set_blob(&conn, &show.id, "fetched-at", &now.to_string());
            let episodes = db::list_episodes(&conn, Some(&show.id), 300).map_err(e)?;
            Ok(Preview { show, episodes, fetched_at: now })
        }
    }
}

#[tauri::command]
async fn refresh_show(state: State<'_, AppState>, id: String) -> Res<Show> {
    let show = db::get_show(&state.db.lock().unwrap(), &id).map_err(e)?.ok_or("节目不存在")?;
    let existing = show.cover.clone();
    let (fresh, eps) = fetch_any(&state, &show.feed_url, &show.kind).await.map_err(e)?;
    let fresh = Show { id: show.id.clone(), kind: show.kind.clone(), ..fresh };
    let fresh = with_channel_avatar(fresh, existing).await;
    store(&state, &fresh, &eps)?;
    Ok(fresh)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Refreshed { shows: usize, new_episodes: usize, skipped: bool }

static REFRESHING: AtomicBool = AtomicBool::new(false);

/// Checks every subscription for new episodes. Runs at launch and periodically; overlapping calls are skipped.
#[tauri::command]
async fn refresh_all(state: State<'_, AppState>) -> Res<Refreshed> {
    if REFRESHING.swap(true, Ordering::SeqCst) { return Ok(Refreshed { shows: 0, new_episodes: 0, skipped: true }); }
    let r = async {
        let shows: Vec<Show> = db::list_shows(&state.db.lock().unwrap()).map_err(e)?.into_iter().filter(|s| s.kind != "single").collect();
        let (mut ok, mut fresh_eps) = (0, 0);
        // Six at a time, each starting as soon as a slot frees up (not in batches that wait for their slowest),
        // and at most 30 s per feed.
        let slots = tokio::sync::Semaphore::new(6);
        let results = futures_util::future::join_all(shows.iter().map(|s| {
            let (st, slots) = (&state, &slots);
            async move {
                let _slot = slots.acquire().await;
                (s, tokio::time::timeout(std::time::Duration::from_secs(30), fetch_any(st, &s.feed_url, &s.kind)).await)
            }
        })).await;
        for (s, r) in results {
            {
                let Ok(Ok((fresh, eps))) = r else { continue };
                let known = {
                    let conn = state.db.lock().unwrap();
                    let ids = db::list_show_episode_ids(&conn, &s.id).unwrap_or_default();
                    eps.iter().filter(|x| ids.contains(&x.id)).count()
                };
                let fresh = Show { id: s.id.clone(), kind: s.kind.clone(), ..fresh };
                let fresh = with_channel_avatar(fresh, s.cover.clone()).await;
                if store(&state, &fresh, &eps).is_ok() { ok += 1; fresh_eps += eps.len() - known; }
            }
        }
        Ok(Refreshed { shows: ok, new_episodes: fresh_eps, skipped: false })
    }.await;
    REFRESHING.store(false, Ordering::SeqCst);
    r
}

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn list_shows(state: State<'_, AppState>) -> Res<Vec<Show>> { db::list_shows(&state.db.lock().unwrap()).map_err(e) }

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn list_episodes(state: State<'_, AppState>, show_id: Option<String>, limit: Option<i64>) -> Res<Vec<Episode>> {
    db::list_episodes(&state.db.lock().unwrap(), show_id.as_deref(), limit.unwrap_or(300)).map_err(e)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ShowHit {
    episode: Episode,
    /// title | notes | transcript
    field: &'static str,
    snippet: String,
    /// Where in the episode a subtitle hit is, in seconds.
    time: Option<f64>,
}

/// Searches one show's episodes on this computer: titles (and their Chinese translations), show notes, and the
/// subtitles of episodes that have them, original and Chinese. Every word must appear in the same field.
/// Runs off the main thread on its own read-only connection, so typing in the search box never holds up playback
/// progress, settings or anything else that needs the database.
#[tauri::command]
async fn search_show(state: State<'_, AppState>, show_id: String, query: String) -> Res<Vec<ShowHit>> {
    let path = state.data.join("subcast.db");
    tokio::task::spawn_blocking(move || {
        let conn = Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX)?;
        search_episodes(&conn, &show_id, &query)
    }).await.map_err(e)?.map_err(e)
}

fn search_episodes(conn: &Connection, show_id: &str, query: &str) -> anyhow::Result<Vec<ShowHit>> {
    let words: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
    if words.is_empty() { return Ok(vec![]); }
    let eps = db::list_episodes(conn, Some(show_id), 5000)?;
    // The list leaves notes out; search needs them.
    let all_notes = db::show_notes(conn, show_id)?;
    static TAGS: once_cell::sync::Lazy<regex::Regex> = once_cell::sync::Lazy::new(|| regex::Regex::new(r"<[^>]+>").unwrap());
    let tags = &*TAGS;
    let has = |text: &str| { let l = text.to_lowercase(); words.iter().all(|w| l.contains(w)) };
    let mut hits = Vec::new();
    for ep in eps {
        let zh_title = db::tr_get(conn, &short_hash(&format!("zh-CN|{}", ep.title), 24)).ok().flatten();
        let title = match &zh_title { Some(z) => format!("{} {z}", ep.title), None => ep.title.clone() };
        if has(&title) { hits.push(ShowHit { snippet: String::new(), field: "title", time: None, episode: ep }); continue; }
        let notes = all_notes.get(&ep.id).map(|n| tags.replace_all(n, " ").to_string()).unwrap_or_default();
        if has(&notes) { hits.push(ShowHit { snippet: snippet(&notes, &words[0]), field: "notes", time: None, episode: ep }); continue; }
        if !ep.has_transcript { continue; }
        let Some((raw_segs, raw_zh)) = db::raw_transcript_and_zh(conn, &ep.id).ok().flatten() else { continue };
        // Pre-filter on raw JSON strings before expensive serde deserialization when words contain no JSON-escaped chars.
        if words.iter().all(|w| !w.contains('"') && !w.contains('\\')) {
            let segs_lower = raw_segs.to_lowercase();
            let zh_lower = raw_zh.as_deref().map(str::to_lowercase);
            if !words.iter().all(|w| segs_lower.contains(w) || zh_lower.as_deref().is_some_and(|z| z.contains(w))) {
                continue;
            }
        }
        let Ok(segs) = serde_json::from_str::<Vec<Segment>>(&raw_segs) else { continue };
        let zh: Vec<Option<String>> = raw_zh.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
        // A line matches with its translation; the first one that contains every word is shown.
        let found = segs.iter().enumerate().find_map(|(i, s)| {
            let line = match zh.get(i).cloned().flatten() { Some(z) => format!("{} {z}", s.text), None => s.text.clone() };
            has(&line).then(|| (s.start, line))
        });
        if let Some((start, line)) = found { hits.push(ShowHit { snippet: snippet(&line, &words[0]), field: "transcript", time: Some(start), episode: ep }); }
    }
    // Titles first, then show notes, then subtitles; newest first within each (the list is already newest first).
    hits.sort_by_key(|h| match h.field { "title" => 0, "notes" => 1, _ => 2 });
    Ok(hits)
}

/// About 70 characters around the first match.
fn snippet(text: &str, word: &str) -> String {
    let chars: Vec<char> = text.split_whitespace().collect::<Vec<_>>().join(" ").chars().collect();
    let lower: String = chars.iter().collect::<String>().to_lowercase();
    let at = lower.find(word).map(|b| lower[..b].chars().count()).unwrap_or(0);
    let from = at.saturating_sub(25);
    let to = (at + 45).min(chars.len());
    format!("{}{}{}", if from > 0 { "…" } else { "" }, chars[from..to].iter().collect::<String>(), if to < chars.len() { "…" } else { "" })
}

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn recent_episodes(state: State<'_, AppState>) -> Res<Vec<Episode>> { db::recent_episodes(&state.db.lock().unwrap(), 60).map_err(e) }

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn get_episode(state: State<'_, AppState>, id: String) -> Res<Option<Episode>> { db::get_episode(&state.db.lock().unwrap(), &id).map_err(e) }

#[tauri::command(async)]
fn remove_show(state: State<'_, AppState>, id: String) -> Res<()> { db::delete_show(&state.db.lock().unwrap(), &id).map_err(e) }

#[tauri::command(async)]
fn set_progress(state: State<'_, AppState>, id: String, seconds: f64) -> Res<()> { db::set_progress(&state.db.lock().unwrap(), &id, seconds).map_err(e) }

#[tauri::command]
fn set_watched(state: State<'_, AppState>, id: String, watched: Option<String>) -> Res<()> {
    db::set_watched(&state.db.lock().unwrap(), &id, watched.as_deref()).map_err(e)
}

#[tauri::command]
fn mark_show_watched(state: State<'_, AppState>, show_id: String) -> Res<usize> {
    db::mark_show_watched(&state.db.lock().unwrap(), &show_id).map_err(e)
}

#[tauri::command]
async fn search_podcasts(term: String, country: Option<String>) -> Res<Vec<feeds::SearchResult>> {
    feeds::search_podcasts(&term, country.as_deref().unwrap_or("US")).await.map_err(e)
}

// ---- transcripts -----------------------------------------------------------------------------------

#[tauri::command(async)]
fn load_transcript(state: State<'_, AppState>, episode_id: String) -> Res<Option<Transcript>> {
    db::get_transcript(&state.db.lock().unwrap(), &episode_id).map_err(e)
}

/// One YouTube caption track as a transcript. When the video is not in Chinese and the publisher uploaded
/// Chinese subtitles, those become the translation (blob "zh", engine "youtube") instead of machine translation.
async fn youtube_transcript(state: &AppState, ep: &Episode, info: &youtube::VideoInfo, track: &youtube::Track) -> anyhow::Result<Transcript> {
    let segs = transcript::resegment(transcript::clean(youtube::fetch_track(track).await?));
    let t = Transcript { episode_id: ep.id.clone(), source: if track.auto { "youtube-auto" } else { "youtube" }.into(), language: track.language.clone(), segments: segs };
    if t.segments.is_empty() { return Ok(t); }
    db::save_transcript(&state.db.lock().unwrap(), &t)?;
    if let Some(zh) = youtube::official_chinese(&info.tracks, &track.language) {
        if let Ok(lines) = youtube::fetch_track(zh).await {
            let items = youtube::align(&t.segments, &transcript::clean(lines));
            // Mostly covered, or the tracks are timed too differently to pair up; gaps get machine translation.
            if items.iter().filter(|x| x.is_some()).count() * 10 >= items.len() * 6 {
                let conn = state.db.lock().unwrap();
                db::set_blob(&conn, &ep.id, "zh", &serde_json::to_string(&items)?)?;
                db::set_blob(&conn, &ep.id, "zh-engine", "youtube")?;
            }
        }
    }
    Ok(t)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrackChoice { language: String, auto: bool }

/// The caption tracks a video offers, for switching the subtitle language. Auto tracks only in languages
/// with no uploaded track (YouTube's automatic ones are worse when a human one exists).
#[tauri::command]
async fn youtube_tracks(state: State<'_, AppState>, episode_id: String) -> Res<Vec<TrackChoice>> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let Some(vid) = &ep.video_id else { return Ok(vec![]) };
    let info = youtube::video_info(vid).await.map_err(e)?;
    let mut out: Vec<TrackChoice> = Vec::new();
    for t in info.tracks.iter().filter(|t| !t.auto).chain(info.tracks.iter().filter(|t| t.auto)) {
        if !out.iter().any(|c| c.language == t.language) { out.push(TrackChoice { language: t.language.clone(), auto: t.auto }); }
    }
    Ok(out)
}

/// Replaces the transcript with another YouTube caption track.
#[tauri::command]
async fn use_youtube_track(state: State<'_, AppState>, episode_id: String, language: String, auto: bool) -> Res<Transcript> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let vid = ep.video_id.clone().ok_or("不是视频节目")?;
    let info = youtube::video_info(&vid).await.map_err(e)?;
    let track = info.tracks.iter().find(|t| t.language == language && t.auto == auto).ok_or("没有这个语言的字幕")?.clone();
    let t = youtube_transcript(&state, &ep, &info, &track).await.map_err(e)?;
    if t.segments.is_empty() { return Err("这条字幕是空的".into()); }
    // The listener's own choice: never replaced by the automatic pick when the episode opens again.
    db::set_blob(&state.db.lock().unwrap(), &episode_id, "track-picked", "1").map_err(e)?;
    Ok(t)
}

/// The publisher's own subtitles: YouTube captions, or a feed's <podcast:transcript>. None when there are none.
#[tauri::command]
async fn fetch_official_transcript(state: State<'_, AppState>, episode_id: String) -> Res<Option<Transcript>> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let t = if let Some(vid) = &ep.video_id {
        let info = youtube::video_info(vid).await.map_err(e)?;
        let hint = youtube::script_hint(&info.title).or_else(|| youtube::script_hint(&ep.title));
        match youtube::choose(&info.tracks, info.original_language.as_deref(), hint) {
            // Saved inside, together with the publisher's Chinese when there is one.
            Some(track) => { let t = youtube_transcript(&state, &ep, &info, track).await.map_err(e)?; return Ok((!t.segments.is_empty()).then_some(t)); }
            None => None,
        }
    } else if let Some(url) = ep.transcript_url.as_ref().filter(|_| ep.transcript_type.as_deref() != Some("podscribe")) {
        let body = net::get_text(url).await.map_err(e)?;
        let kind = ep.transcript_type.clone().unwrap_or_default();
        let segs = if kind.contains("json") { transcript::parse_podcast_json(&body) } else { transcript::parse_cues(&body) };
        let segs = transcript::resegment(transcript::clean(segs));
        (!segs.is_empty()).then(|| Transcript { episode_id: ep.id.clone(), source: "rss".into(), language: "auto".into(), segments: segs })
    } else {
        None
    };
    // 乔木精读 episodes carry the original transcript on their page (untimed; spread across the episode).
    let t = match t {
        Some(t) => Some(t),
        None if ep.transcript_type.as_deref() == Some("podscribe") && ep.transcript_url.is_some() => {
            let segs = qiaomu::podscribe_transcript(ep.transcript_url.as_ref().unwrap()).await.map_err(e)?;
            (!segs.is_empty()).then(|| Transcript { episode_id: ep.id.clone(), source: "podscribe".into(), language: "en".into(), segments: transcript::resegment(segs) })
        }
        None if ep.transcript_type.as_deref() == Some("qiaomu") => match &ep.link {
            Some(link) => {
                let d = qiaomu::detail(link).await.map_err(e)?;
                d.transcript.map(|text| Transcript { episode_id: ep.id.clone(), source: "qiaomu".into(), language: "auto".into(), segments: qiaomu::transcript_segments(&text, ep.duration.unwrap_or(3600.0)) })
            }
            None => None,
        },
        None => None,
    };
    if let Some(t) = &t {
        if t.segments.is_empty() { return Ok(None); }
        db::save_transcript(&state.db.lock().unwrap(), t).map_err(e)?;
    }
    Ok(t)
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct AsrEvent { episode_id: String, stage: String, progress: f64, message: Option<String>, segment: Option<Segment> }

fn asr_emit(app: &AppHandle, id: &str, stage: &str, progress: f64, message: Option<String>, segment: Option<Segment>) {
    let _ = app.emit("asr", AsrEvent { episode_id: id.into(), stage: stage.into(), progress, message, segment });
}

/// A 乔木精读 episode's page: fills in its audio address and keeps the Chinese deep-read.
#[tauri::command]
async fn qiaomu_detail(state: State<'_, AppState>, episode_id: String) -> Res<qiaomu::Detail> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let d = if matches!(ep.transcript_type.as_deref(), Some("qiaomu-rss") | Some("podscribe")) {
        qiaomu::Detail { audio: None, deep_read: qiaomu::rss_rewrite(&ep.guid).await.map_err(e)?, transcript: None }
    } else {
        let link = ep.link.clone().ok_or("这集没有网页")?;
        qiaomu::detail(&link).await.map_err(e)?
    };
    let conn = state.db.lock().unwrap();
    if let (Some(a), None) = (&d.audio, &ep.audio_url) { let _ = db::set_audio_url(&conn, &ep.id, a); }
    if let Some(h) = &d.deep_read { let _ = db::set_blob(&conn, &ep.id, "deep-read", h); }
    Ok(qiaomu::Detail { transcript: None, ..d })
}

/// Saves a generated image (a quote poster) to Downloads and returns its path.
#[tauri::command]
fn save_image(name: String, base64: String) -> Res<String> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD.decode(base64.trim_start_matches("data:image/png;base64,")).map_err(e)?;
    let dir = dirs::download_dir().or_else(dirs::desktop_dir).ok_or("找不到下载文件夹")?;
    let safe: String = name.chars().filter(|c| !"/\\:*?\"<>|".contains(*c)).take(60).collect();
    let mut path = dir.join(format!("{safe}.png"));
    let mut i = 2;
    while path.exists() { path = dir.join(format!("{safe} {i}.png")); i += 1; }
    std::fs::write(&path, bytes).map_err(e)?;
    Ok(path.to_string_lossy().into_owned())
}

/// The speaker's own voice for one sentence (clip.rs): a small local file, made once and then reused.
#[tauri::command]
async fn learn_clip(state: State<'_, AppState>, episode_id: String, start: f64, end: f64) -> Res<String> {
    let data = state.data.clone();
    let done = clip::path_for(&data, &episode_id, start, end);
    if done.exists() { return Ok(done.to_string_lossy().into_owned()); }
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    if let Some(saved) = offline::find(&data, &episode_id).or_else(|| offline::cached(&data, &episode_id)) {
        // Subtitle times, moved onto this file when it is a different cut (align_audio).
        let map = db::get_blob(&state.db.lock().unwrap(), &episode_id, "tmap").ok().flatten()
            .and_then(|s| serde_json::from_str::<align::TimeMap>(&s).ok()).filter(|m| m.play_bytes == saved.bytes);
        let (a, b) = map.map(|m| (m.to_play(start), m.to_play(end))).unwrap_or((start, end));
        return clip::make(&data, clip::Source::File(saved.path.into()), &episode_id, a, b).await.map(|p| p.to_string_lossy().into_owned()).map_err(e);
    }
    if let Some(url) = ep.audio_url.filter(|u| !u.is_empty()) {
        return clip::make(&data, clip::Source::Url(url), &episode_id, start, end).await.map(|p| p.to_string_lossy().into_owned()).map_err(e);
    }
    let vid = ep.video_id.ok_or("这一集没有音频")?;
    asr::streams::init(&data);
    // A saved stream address may have gone stale: one more try with a fresh one.
    for fresh in [false, true] {
        let v = vid.clone();
        let url = asr::with_ytdlp(&data, |bin| { let v = v.clone(); async move { asr::youtube_stream_url(&bin, &v, false, fresh).await } }).await.map_err(e)?;
        match clip::make(&data, clip::Source::Url(url), &episode_id, start, end).await {
            Ok(p) => return Ok(p.to_string_lossy().into_owned()),
            Err(err) if !fresh => { let _ = err; }
            Err(err) => return Err(e(err)),
        }
    }
    Err("没能截取原声".into())
}

/// The word book as an Anki package at `path` (chosen in the save dialog); returns how many cards have audio.
#[tauri::command]
async fn export_anki(path: String, deck: String, model: String, notes: Vec<anki::Note>) -> Res<usize> {
    tokio::task::spawn_blocking(move || anki::write(std::path::Path::new(&path), &deck, &model, &notes)).await.map_err(e)?.map_err(e)
}

/// Asks where to save is the interface's job (the save dialog); this writes text there (a CSV of the word book).
#[tauri::command]
fn save_text(path: String, text: String) -> Res<()> { std::fs::write(path, text).map_err(e) }

#[tauri::command(async)]
fn video_clip_env(state: State<'_, AppState>) -> videoclip::FfmpegStatus {
    videoclip::env_status(&state.data)
}

/// Resolves a video's download addresses in the background (and readies ffmpeg), so a clip starts downloading at once.
#[tauri::command]
fn video_clip_warm(state: State<'_, AppState>, video_id: String, ranges: Option<Vec<(f64, f64)>>) {
    let data = state.data.clone();
    tauri::async_runtime::spawn(async move {
        let d2 = data.clone();
        let never = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (warmed, ff) = tokio::join!(fastclip::warm(&data, &video_id), videoclip::ensure_ffmpeg(&d2, &never, |_, _| {}));
        // Then the likeliest clips (the best few), one by one, so opening them shows the real cut at once.
        if warmed.is_ok() && ff.is_ok() {
            for (a, b) in ranges.unwrap_or_default().into_iter().take(3) {
                let _ = videoclip::prepare_segment(&data, &video_id, a, b, never.clone(), |_, _, _| {}).await;
            }
        }
    });
}

#[tauri::command]
async fn video_clip_prepare(
    app: AppHandle,
    state: State<'_, AppState>,
    job_id: String,
    video_id: String,
    start: f64,
    end: f64,
    ranges: Option<Vec<(f64, f64)>>,
) -> Res<videoclip::PreparedClip> {
    let data = state.data.clone();
    let cancel = state.job(&job_id);
    let (app2, jid) = (app.clone(), job_id.clone());
    let report = move |stage: &str, progress: f64, message: &str| {
        let _ = app2.emit("video-clip-progress", json!({
            "jobId": jid, "stage": stage, "progress": progress, "message": message
        }));
    };
    let res = match ranges.filter(|r| r.len() > 1) {
        Some(r) => videoclip::prepare_montage(&data, &video_id, &r, cancel.clone(), report).await,
        None => videoclip::prepare_segment(&data, &video_id, start, end, cancel.clone(), report).await,
    };
    state.done_with(&job_id, &cancel);
    res.map_err(e)
}

#[tauri::command]
async fn video_clip_render(
    app: AppHandle,
    state: State<'_, AppState>,
    job_id: String,
    req: videoclip::RenderRequest,
) -> Res<videoclip::RenderResult> {
    let data = state.data.clone();
    let cancel = state.job(&job_id);
    let (app2, jid) = (app.clone(), job_id.clone());
    let res = videoclip::render_clip(&data, req, cancel.clone(), move |stage, progress, message| {
        let _ = app2.emit("video-clip-progress", json!({
            "jobId": jid, "stage": stage, "progress": progress, "message": message
        }));
    }).await;
    state.done_with(&job_id, &cancel);
    res.map_err(e)
}

#[tauri::command]
fn reveal_file(app: AppHandle, path: String) -> Res<()> {
    use tauri_plugin_opener::OpenerExt;
    if !std::path::Path::new(&path).exists() { return Err("找不到导出的文件".into()); }
    app.opener().reveal_item_in_dir(&path).map_err(e)
}

#[tauri::command]
async fn list_models(provider: llm::Provider) -> Res<Vec<String>> { cloud::list_models(&provider).await.map_err(e) }

/// Legacy Doubao TTS command (kept for compatibility with older UI bundles).
#[tauri::command]
async fn tts_speak(text: String, voice: String, resource: Option<String>) -> Res<String> {
    use base64::Engine;
    let wav = cloud::tts(&voice, resource.as_deref().unwrap_or(""), &text).await.map_err(e)?;
    Ok(base64::engine::general_purpose::STANDARD.encode(wav))
}

/// Multi-engine TTS: synthesises to `<data>/tts-cache/` and returns the local file path + duration.
#[tauri::command]
async fn tts_synthesize(state: State<'_, AppState>, req: speech::TtsRequest) -> Res<speech::TtsResult> {
    speech::synthesize(&state.data, req).await.map_err(e)
}

/// Installed OS voices for the System TTS engine.
#[tauri::command(async)]
fn tts_system_voices() -> Vec<speech::SystemVoice> {
    speech::list_system_voices()
}

/// Offline Melo / Kokoro: whether this system can run them and which are downloaded.
#[tauri::command(async)]
fn local_tts_status(state: State<'_, AppState>) -> local_tts::Status {
    local_tts::status(&state.data)
}

/// Downloads a local TTS model (and the sherpa-onnx runtime when missing), emitting `local-tts-download`.
/// Cancel with `cancel_job("local-tts:<id>")`.
#[tauri::command]
async fn local_tts_install(app: AppHandle, state: State<'_, AppState>, id: String, mirror_first: bool) -> Res<()> {
    let job = format!("local-tts:{id}");
    let cancel = state.job(&job);
    let (app2, id2) = (app.clone(), id.clone());
    let r = local_tts::install(&state.data, &id, cancel.clone(), mirror_first, move |got, total| {
        let _ = app2.emit("local-tts-download", json!({"id": id2, "received": got, "total": total}));
    }).await;
    state.done_with(&job, &cancel);
    r.map_err(e)
}

#[tauri::command(async)]
fn local_tts_delete(state: State<'_, AppState>, id: String) -> Res<()> {
    local_tts::delete(&state.data, &id).map_err(e)
}

/// Clears cached TTS audio files.
#[tauri::command(async)]
fn clear_tts_cache(state: State<'_, AppState>) -> Res<()> {
    speech::clear_cache(&state.data).map_err(e)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct VideoStream { url: String, /** hls | mp4 */ kind: &'static str }

/// A loopback port for the interface that nothing else answers on. The page is opened as `http://localhost:<port>`,
/// which the system may resolve to IPv6 (::1) first, so both 127.0.0.1 and ::1 are taken: another program on
/// [::1]:14327 (the mobile app in the iOS Simulator, say) would otherwise be the one that answers, and the window
/// would show its pages. Tries LOCAL_PORT first (localStorage belongs to the port), then the next few.
fn bind_ui_port(preferred: u16) -> Option<(u16, Vec<std::net::TcpListener>)> {
    use std::net::TcpListener;
    // The same 20 ports are allowed in capabilities/default.json (remote.urls); keep the two in step.
    for port in preferred..preferred.saturating_add(UI_PORTS) {
        let Ok(v4) = TcpListener::bind(("127.0.0.1", port)) else { continue };
        match TcpListener::bind(("::1", port)) {
            Ok(v6) => return Some((port, vec![v4, v6])),
            // Someone else answers "localhost" on this port over IPv6.
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
            // No IPv6 on this computer: IPv4 alone is what "localhost" means here.
            Err(_) => return Some((port, vec![v4])),
        }
    }
    None
}

/// Serves the interface on localhost (what tauri-plugin-localhost did), from the active hot update when there is
/// one. Returns the port, bound before the window opens so it can never load someone else's pages.
#[cfg(not(debug_assertions))]
fn serve_ui(app: &AppHandle, active: ota::Active) -> u16 {
    let Some((port, listeners)) = bind_ui_port(LOCAL_PORT) else {
        eprintln!("SubCast: no free port for the interface near {LOCAL_PORT}");
        return LOCAL_PORT;
    };
    if port != LOCAL_PORT { eprintln!("SubCast: port {LOCAL_PORT} is taken by another program; using {port}"); }
    for listener in listeners {
        let Ok(server) = tiny_http::Server::from_listener(listener, None) else { continue };
        let (resolver, active) = (app.asset_resolver(), active.clone());
        std::thread::spawn(move || {
            for req in server.incoming_requests() {
                let path = req.url().split(['?', '#']).next().unwrap_or("/").to_string();
                let found = match ota::read(&active, &path) {
                    Some(file) => file.map(|(bytes, mime)| (bytes, mime.to_string(), None)),
                    None => resolver.get(path).map(|a| (a.bytes, a.mime_type, a.csp_header)),
                };
                let Some((bytes, mime, csp)) = found else { let _ = req.respond(tiny_http::Response::empty(404)); continue };
                let mut resp = tiny_http::Response::from_data(bytes);
                for (k, v) in [("Content-Type", Some(mime)), ("Cache-Control", Some("no-cache".to_string())), ("Content-Security-Policy", csp)] {
                    if let Some(h) = v.and_then(|v| tiny_http::Header::from_bytes(k.as_bytes(), v.as_bytes()).ok()) { resp.add_header(h); }
                }
                let _ = req.respond(resp);
            }
        });
    }
    port
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct UiInfo { shell: &'static str, build: Option<u64> }

/// The page rendered: a hot update being tried becomes the known-good one.
#[tauri::command]
fn ui_ready(state: State<'_, AppState>) -> UiInfo {
    UiInfo { shell: ota::SHELL, build: if cfg!(debug_assertions) { None } else { ota::ready(&state.data) } }
}

/// Remote config (https://subcast.qiaomu.ai/tools/config.json): values that go stale outside our control, such as
/// the YouTube client versions captions depend on. Signed like updates; the last good copy is kept in settings
/// and applied at startup, so the app works offline and a bad fetch changes nothing.
const CONFIG_URL: &str = "https://subcast.qiaomu.ai/tools/config.json";

fn apply_config(v: &Value) {
    youtube::set_clients(&v["youtubeClients"]);
}

#[tauri::command]
async fn refresh_config(app: AppHandle, state: State<'_, AppState>) -> Res<()> {
    let pubkey = app.config().plugins.0.get("updater").and_then(|u| u["pubkey"].as_str()).ok_or("缺少更新公钥")?.to_string();
    let body = net::client().get(CONFIG_URL).send().await.map_err(e)?.error_for_status().map_err(e)?.bytes().await.map_err(e)?;
    let sig = net::get_text(&format!("{CONFIG_URL}.sig")).await.map_err(e)?;
    ota::verify(&pubkey, &body, &sig).map_err(e)?;
    let v: Value = serde_json::from_slice(&body).map_err(e)?;
    apply_config(&v);
    db::set_setting(&state.db.lock().unwrap(), "remote-config", &v.to_string()).map_err(e)?;
    Ok(())
}

/// Checks for a hot update of the interface; a new one is used from the next launch. none | staged:N | rollback
#[tauri::command]
async fn ui_check(app: AppHandle, state: State<'_, AppState>) -> Res<String> {
    let pubkey = app.config().plugins.0.get("updater").and_then(|u| u["pubkey"].as_str()).ok_or("缺少更新公钥")?.to_string();
    Ok(match ota::check(&state.data, &pubkey).await.map_err(e)? {
        ota::Outcome::None => "none".into(),
        ota::Outcome::Staged(b) => format!("staged:{b}"),
        ota::Outcome::RolledBack => "rollback".into(),
    })
}

/// The direct stream for a video the embedded player can't play. `mode`: "native-hls" (the WebView plays HLS
/// itself), "proxy-hls" (hls.js through the loopback proxy, for HD where HLS isn't native) or "mp4" (360p, plays
/// anywhere). Emits `yt-stream` events ("tool" with the first-time yt-dlp download progress, then "resolve").
#[tauri::command]
async fn youtube_stream(app: AppHandle, state: State<'_, AppState>, video_id: String, mode: Option<String>, fresh: Option<bool>) -> Res<VideoStream> {
    asr::streams::init(&state.data);
    let fresh = fresh.unwrap_or(false);
    let mode = mode.unwrap_or_else(|| if cfg!(target_os = "macos") { "native-hls".into() } else { "proxy-hls".into() });
    let hls = mode != "mp4";
    let emit = |stage: &str, progress: f64| { let _ = app.emit("yt-stream", json!({ "videoId": video_id, "stage": stage, "progress": progress })); };
    // The first-time download of yt-dlp has its own progress and limits; the clock starts after it.
    asr::ytdlp_with(&state.data, |got, total| emit("tool", total.map_or(0.0, |t| got as f64 / t.max(1) as f64))).await.map_err(e)?;
    emit("resolve", 0.0);
    let job = asr::with_ytdlp(&state.data, |bin| { let v = video_id.clone(); async move { asr::youtube_stream_url(&bin, &v, hls, fresh).await } });
    let url = tokio::time::timeout(std::time::Duration::from_secs(120), job).await
        .map_err(|_| "获取视频地址超时，请检查网络或代理".to_string())?
        .map_err(e)?;
    // yt-dlp falls back to an MP4 when a video has no HLS.
    let is_hls = url.contains("m3u8") || url.contains("/manifest/hls");
    if is_hls && mode == "proxy-hls" { return Ok(VideoStream { url: hlsproxy::proxied(&url).await.map_err(e)?, kind: "hls" }); }
    Ok(VideoStream { url, kind: if is_hls { "hls" } else { "mp4" } })
}

/// Opens a SubCast window to sign in to YouTube (ytlogin.rs); the interface hears `youtube-signed-in`.
#[tauri::command]
fn youtube_sign_in(app: AppHandle) -> Res<()> { ytlogin::open(&app).map_err(e) }

/// Whether the listener has signed in to YouTube in SubCast.
#[tauri::command]
async fn youtube_signed_in() -> Res<bool> { Ok(ytlogin::signed_in().await) }

#[tauri::command]
async fn youtube_sign_out() -> Res<()> { ytlogin::sign_out().await.map_err(e) }

/// Gets yt-dlp ready in the background when a video page opens, so a fallback doesn't start with a download.
#[tauri::command]
async fn prepare_youtube(state: State<'_, AppState>) -> Res<()> {
    let bin = asr::ytdlp(&state.data).await.map_err(e)?;
    asr::warm_ytdlp(&bin).await;
    Ok(())
}

// ---- offline -------------------------------------------------------------------------------------------

#[tauri::command]
async fn download_episode(app: AppHandle, state: State<'_, AppState>, episode_id: String) -> Res<offline::Saved> {
    if let Some(s) = offline::find(&state.data, &episode_id) { return Ok(s); }
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let job = format!("dl:{episode_id}");
    let cancel = state.job(&job);
    let id = episode_id.clone();
    let result = offline::download(&state.data, &ep, cancel.clone(), |p| { let _ = app.emit("offline", json!({ "episodeId": id, "progress": p })); }).await.map_err(e);
    state.done_with(&job, &cancel);
    result
}

#[tauri::command(async)]
fn list_downloads(state: State<'_, AppState>) -> Vec<offline::Saved> { offline::list(&state.data) }

#[tauri::command(async)]
fn delete_download(state: State<'_, AppState>, episode_id: String) -> Res<()> { offline::remove(&state.data, &episode_id).map_err(e) }

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn clear_downloads(state: State<'_, AppState>) -> Res<()> { offline::clear(&state.data).map_err(e) }

fn dir_bytes(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p).map(|rd| rd.flatten().map(|f| match f.metadata() {
        Ok(m) if m.is_dir() => dir_bytes(&f.path()),
        Ok(m) => m.len(),
        Err(_) => 0,
    }).sum()).unwrap_or(0)
}

/// The full show notes when the feed only carried a short summary (rss.qiaomu.ai channels cut it at
/// ~300 characters): the YouTube description when the episode has a video (chapters and links), else
/// the 小宇宙 shownotes. Kept in the "notes-full" blob, so each episode is fetched once.
#[tauri::command]
async fn full_notes(state: State<'_, AppState>, episode_id: String) -> Res<Option<String>> {
    if let Some(cached) = db::get_blob(&state.db.lock().unwrap(), &episode_id, "notes-full").map_err(e)? { return Ok(Some(cached).filter(|s| !s.is_empty())); }
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let have = ep.notes.as_deref().map(|n| n.chars().count()).unwrap_or(0);
    let mut best: Option<String> = None;
    let mut reached = false;
    if let Some(v) = &ep.video_id {
        if let Ok(info) = youtube::video_info(v).await { reached = true; if !info.description.trim().is_empty() { best = Some(info.description); } }
    }
    if let Some(link) = ep.link.as_deref().filter(|l| l.contains("xiaoyuzhoufm.com/episode/")) {
        if let Ok(x) = feeds::xiaoyuzhou_episode(link).await {
            reached = true;
            if let Some(n) = x.notes { if best.as_ref().map_or(true, |b| n.chars().count() > b.chars().count()) { best = Some(n); } }
        }
    }
    // Only keep it when it really is longer; "" marks "checked, nothing better" so we don't ask again.
    let out = best.filter(|b| b.chars().count() > have + 40);
    if out.is_none() && !reached { return Ok(None); } // offline or blocked: try again next time
    db::set_blob(&state.db.lock().unwrap(), &episode_id, "notes-full", out.as_deref().unwrap_or("")).map_err(e)?;
    Ok(out)
}

/// What SubCast keeps on disk, for the storage section of Settings.
// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn storage_info(state: State<'_, AppState>) -> serde_json::Value {
    let d = &state.data;
    let library: u64 = ["subcast.db", "subcast.db-wal", "subcast.db-shm"].iter().map(|f| std::fs::metadata(d.join(f)).map(|m| m.len()).unwrap_or(0)).sum();
    json!({
        "downloads": dir_bytes(&offline::dir(d)),
        "models": dir_bytes(&asr::models_dir(d)),
        "library": library,
        "temp": dir_bytes(&d.join("work")),
        "audioCache": dir_bytes(&offline::cache_dir(d)),
        "ttsCache": dir_bytes(&speech::cache_dir(d)),
        "ttsModels": dir_bytes(&local_tts::models_dir(d)),
    })
}

/// Opens one of SubCast's folders in Finder / Explorer ("downloads", "models", or "tts-cache"), creating it first.
#[tauri::command]
fn open_data_folder(app: AppHandle, state: State<'_, AppState>, kind: String) -> Res<()> {
    use tauri_plugin_opener::OpenerExt;
    let dir = match kind.as_str() {
        "downloads" => offline::dir(&state.data),
        "models" => asr::models_dir(&state.data),
        "tts-cache" => speech::cache_dir(&state.data),
        "tts-models" => local_tts::models_dir(&state.data),
        _ => state.data.clone(),
    };
    std::fs::create_dir_all(&dir).map_err(e)?;
    app.opener().open_path(dir.to_string_lossy(), None::<&str>).map_err(e)
}

/// Shows one downloaded episode's file selected in Finder / Explorer.
#[tauri::command]
fn reveal_download(app: AppHandle, state: State<'_, AppState>, episode_id: String) -> Res<()> {
    use tauri_plugin_opener::OpenerExt;
    let saved = offline::find(&state.data, &episode_id).ok_or("没有找到下载的文件")?;
    app.opener().reveal_item_in_dir(&saved.path).map_err(e)
}

/// Removes leftovers of interrupted transcriptions.
// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn clear_temp(state: State<'_, AppState>) -> Res<()> {
    let dir = state.data.join("work");
    if dir.exists() { std::fs::remove_dir_all(&dir).map_err(e)?; }
    Ok(())
}

/// Someone else's subtitles for this episode from the shared library, saved as this episode's transcript (and
/// their Chinese as its translation). None when the library hasn't got it.
async fn take_shared(state: &AppState, ep: &Episode) -> Option<Transcript> {
    let mut hit = shared::lookup(&shared::keys(ep)).await?;
    let translation = hit.translation.take();
    // The file the shared subtitles were timed against, so a listener with another cut can line them up (align_audio).
    let audio_ref = hit.audio.take().map(|a| align::AudioRef { bytes: a.bytes, etag: a.etag, secs: a.secs, fp: None, shared_id: Some(hit.id.clone()), has_fp: a.fp });
    let t = shared::into_transcript(&ep.id, hit);
    db::save_transcript(&state.db.lock().unwrap(), &t).ok()?;
    if let Some(r) = audio_ref { let _ = db::set_blob(&state.db.lock().unwrap(), &ep.id, "audio-ref", &serde_json::to_string(&r).unwrap_or_default()); }
    // The shared Chinese lines become this episode's translation (engine "shared"); the subtitle view only
    // machine-translates the gaps. Saved after the transcript, which clears an episode's old blobs.
    if let Some(tr) = translation.filter(|tr| tr.items.len() == t.segments.len() && tr.lang.starts_with("zh")) {
        let conn = state.db.lock().unwrap();
        let _ = db::set_blob(&conn, &ep.id, "zh", &serde_json::to_string(&tr.items).unwrap_or_default());
        let _ = db::set_blob(&conn, &ep.id, "zh-engine", "shared");
    }
    Some(t)
}

/// The audio file an episode's subtitles were timed against, with its fingerprint, when it is known here.
fn audio_ref(state: &AppState, episode_id: &str) -> Option<align::AudioRef> {
    db::get_blob(&state.db.lock().unwrap(), episode_id, "audio-ref").ok().flatten()
        .and_then(|s| serde_json::from_str::<align::AudioRef>(&s).ok()).filter(|r| r.fp.is_some())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Aligned {
    /// same: subtitles fit the file that plays | aligned: `map` converts | unknown: nothing to compare with |
    /// failed: the files share too little
    status: &'static str,
    map: Option<align::TimeMap>,
    /// The file to play, when the map is for a downloaded copy rather than the stream.
    path: Option<String>,
    /// Size of the local file that plays (0: the stream), which a map adjusted by hand is for.
    local_bytes: u64,
}

/// Lines an episode's subtitles up with the audio that plays. Cheap when nothing changed (the file size is the
/// one the subtitles were made from). Otherwise the episode's current audio is fetched into the cache and the two
/// are compared: by fingerprint when the subtitles' file has one, else (`force`, with a local speech `model`) by
/// transcribing a few stretches of it and finding them in the subtitles. The map comes back with the file it fits.
#[tauri::command]
async fn align_audio(app: AppHandle, state: State<'_, AppState>, episode_id: String, force: bool, model: Option<String>) -> Res<Aligned> {
    let (ep, aref, saved) = {
        let conn = state.db.lock().unwrap();
        let ep = db::get_episode(&conn, &episode_id).map_err(e)?.ok_or("节目不存在")?;
        let aref = db::get_blob(&conn, &episode_id, "audio-ref").ok().flatten().and_then(|s| serde_json::from_str::<align::AudioRef>(&s).ok());
        let saved = db::get_blob(&conn, &episode_id, "tmap").ok().flatten().and_then(|s| serde_json::from_str::<align::TimeMap>(&s).ok());
        (ep, aref, saved)
    };
    let local = offline::find(&state.data, &episode_id).or_else(|| offline::cached(&state.data, &episode_id));
    let local_bytes = local.as_ref().map(|l| l.bytes).unwrap_or(0);
    let none = |status| Ok(Aligned { status, map: None, path: None, local_bytes });
    let (Some(url), None) = (ep.audio_url.clone(), &ep.video_id) else { return none("same") };
    if let Some(m) = saved {
        // A map is for one file: the local one of that size, or the stream (0) for a map set by hand while streaming.
        let fits = m.play_bytes == local_bytes || (m.method == "same" && local.is_none());
        if fits && m.method != "same" && !(force && m.method != "manual") {
            return Ok(Aligned { status: "aligned", map: Some(m), path: local.map(|l| l.path), local_bytes });
        }
        if fits && m.method == "same" && !force { return none("same"); }
    }
    let has_fp = aref.as_ref().is_some_and(|a| a.fp.is_some() || a.has_fp);
    if !has_fp && !force { return none("unknown"); }
    let job = format!("al:{episode_id}");
    if state.running(&job) { return none("unknown"); }
    let cancel = state.job(&job);
    let result: Result<Aligned, String> = async {
        let save = |m: &align::TimeMap| { let _ = db::set_blob(&state.db.lock().unwrap(), &episode_id, "tmap", &serde_json::to_string(m).unwrap_or_default()); };
        if let Some(a) = &aref {
            let play_bytes = match &local { Some(l) => Some(l.bytes), None => align::remote_size(&url).await.map(|x| x.0) };
            if play_bytes == Some(a.bytes) { save(&align::TimeMap::same(a.bytes)); return none("same"); }
        }
        // Another file (or one nobody recorded): fetch it, since it is also what will play (a stream with ads
        // chosen per request could differ again next time).
        let path = match &local {
            Some(l) => PathBuf::from(&l.path),
            None => {
                let dest = offline::cache_path(&state.data, &episode_id, &url);
                let part = dest.with_extension("part");
                let _ = tokio::fs::create_dir_all(offline::cache_dir(&state.data)).await;
                let (a, id) = (app.clone(), episode_id.clone());
                asr::download(&url, &part, &cancel, move |got, total| {
                    let _ = a.emit("align", json!({ "episodeId": id, "progress": total.map(|t| got as f64 / t.max(1) as f64) }));
                }).await.map_err(e)?;
                PathBuf::from(offline::keep_cached(&state.data, &episode_id, &part).map_err(e)?.path)
            }
        };
        let _ = app.emit("align", json!({ "episodeId": episode_id, "progress": null }));
        let bytes = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        let p2 = path.clone();
        let (secs, play_fp) = tokio::task::spawn_blocking(move || align::fingerprint_file(&p2)).await.map_err(e)?.map_err(e)?;
        let (pieces, method) = if let Some(mut a) = aref.clone().filter(|_| has_fp) {
            let ref_fp = match a.fp.as_deref().and_then(align::decode_fp) {
                Some(fp) => fp,
                None => {
                    let fp = shared::fingerprint(a.shared_id.as_deref().unwrap_or("")).await.ok_or("没有拿到字幕对应音频的指纹")?;
                    a.fp = Some(align::encode_fp(&fp));
                    let _ = db::set_blob(&state.db.lock().unwrap(), &episode_id, "audio-ref", &serde_json::to_string(&a).unwrap_or_default());
                    fp
                }
            };
            (align::map_fingerprints(&ref_fp, &play_fp).map_err(e)?, "fingerprint")
        } else {
            let t = db::get_transcript(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("没有字幕")?;
            let model = model.as_deref().ok_or("先在「语音识别」里下载一个本机模型，才能自动比对")?;
            let mut anchors = Vec::new();
            let mut starts = align::probe_starts(secs, 120.0);
            for _round in 0..2 {
                if starts.is_empty() { break; }
                let heard = listen_probes(&state, &path, &starts, model, &t.language, cancel.clone()).await?;
                anchors.extend(align::probe_anchors(&t.segments, &heard, &starts));
                starts = align::refine_starts(&anchors);
            }
            (align::map_anchors(&anchors), "probe")
        };
        // Too little in common: another episode, or something these comparisons can't see through.
        let shared_secs: f64 = pieces.iter().map(|p| p.ref_end - p.ref_start).sum();
        if pieces.is_empty() || (method == "fingerprint" && shared_secs < secs / 3.0) { return Ok(Aligned { status: "failed", map: None, path: Some(path.to_string_lossy().into_owned()), local_bytes: bytes }); }
        let map = align::TimeMap { play_bytes: bytes, method: method.into(), pieces };
        save(&map);
        Ok(Aligned { status: "aligned", map: Some(map), path: Some(path.to_string_lossy().into_owned()), local_bytes: bytes })
    }.await;
    state.done_with(&job, &cancel);
    result
}

/// Transcribes short stretches of an audio file (`align::PROBE_SECS` from each start), all in one run of the local
/// recogniser: each stretch sits in its own 30 s slot of one file.
async fn listen_probes(state: &AppState, path: &std::path::Path, starts: &[f64], model: &str, language: &str, cancel: Arc<AtomicBool>) -> Result<Vec<Segment>, String> {
    let spec = asr::resolve(&state.data, model).ok_or("未知模型")?;
    if !spec.path.is_file() { return Err("先在「语音识别」里下载一个本机模型，才能自动比对".into()); }
    let work = state.data.join("work").join(format!("align-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::create_dir_all(&work).await.map_err(e)?;
    let wav = work.join("probes.wav");
    let (p, st, w) = (path.to_path_buf(), starts.to_vec(), wav.clone());
    let made = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let ext = p.extension().and_then(|x| x.to_str()).map(str::to_string);
        let slot = (align::SLOT_SECS * 16000.0) as usize;
        let mut all: Vec<f32> = Vec::with_capacity(slot * st.len());
        for s in st {
            let mut v = clip::cut(Box::new(std::fs::File::open(&p)?), ext.as_deref(), s, s + align::PROBE_SECS).unwrap_or_default();
            v.resize(slot, 0.0);
            all.extend(v);
        }
        asr::write_wav(&w, &all)
    }).await.map_err(e)?;
    let heard = match made {
        Err(err) => Err(e(err)),
        Ok(()) if spec.engine == "qwen3" => match asr::find_crispasr(&state.data) {
            Some(bin) => asr::run_crispasr(&bin, &spec.path, &wav, language, &asr::AsrContext::default(), cancel, |_| {}).await.map_err(e),
            None => Err("Qwen3-ASR 识别程序还没准备好，先转写一集就会自动安装".into()),
        },
        Ok(()) => asr::run_worker(&wav, &spec.path, language, "", cancel, |_| {}).await.map(|r| r.segments).map_err(e),
    };
    let _ = tokio::fs::remove_dir_all(&work).await;
    heard
}

/// 跟读打分 (src-tauri/src/shadow.rs): the microphone on or off, a new turn, and what was said in it.
#[tauri::command]
fn shadow_start(app: tauri::AppHandle) -> Res<()> { shadow::start(move |peak| { let _ = app.emit("shadow-level", peak); }).map_err(e) }
#[tauri::command]
fn shadow_stop() { shadow::stop() }
#[tauri::command]
fn shadow_clear() { shadow::clear() }
#[tauri::command]
fn shadow_ready(state: State<'_, AppState>, model: String) -> bool { shadow::ready(&state.data, &model) }
#[tauri::command]
async fn shadow_hear(state: State<'_, AppState>, model: String, language: String) -> Res<String> { shadow::hear(&state.data, &model, &language).await.map_err(e) }
#[tauri::command]
fn open_mic_settings() { shadow::open_mic_settings() }

/// Saves a time map adjusted by hand ("对齐到这一句").
#[tauri::command]
fn save_time_map(state: State<'_, AppState>, episode_id: String, map: align::TimeMap) -> Res<()> {
    db::set_blob(&state.db.lock().unwrap(), &episode_id, "tmap", &serde_json::to_string(&map).map_err(e)?).map_err(e)
}

#[tauri::command]
fn list_audio_cache(state: State<'_, AppState>) -> Vec<offline::Saved> { offline::list_cached(&state.data) }

#[tauri::command]
fn clear_audio_cache(state: State<'_, AppState>) -> Res<()> { offline::clear_cache(&state.data).map_err(e) }

/// Checked when an episode opens, before it is called "no subtitles", so no model is downloaded for it.
#[tauri::command]
async fn shared_transcript(state: State<'_, AppState>, episode_id: String) -> Res<Option<Transcript>> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    Ok(take_shared(&state, &ep).await)
}

/// The episodes (of these ids) someone already shared subtitles for, so lists can mark and sort them.
#[tauri::command]
async fn shared_exists(state: State<'_, AppState>, episode_ids: Vec<String>) -> Res<Vec<String>> {
    let eps: Vec<Episode> = {
        let conn = state.db.lock().unwrap();
        episode_ids.iter().take(200).filter_map(|id| db::get_episode(&conn, id).ok().flatten()).collect()
    };
    let lists: Vec<Vec<String>> = eps.iter().map(shared::keys).collect();
    let hits = shared::existing(&lists).await;
    Ok(eps.into_iter().zip(hits).filter(|(_, h)| *h).map(|(ep, _)| ep.id).collect())
}

#[tauri::command]
async fn transcribe(app: AppHandle, state: State<'_, AppState>, episode_id: String, model: String, language: String, cloud: Option<cloud::CloudAsr>) -> Res<Transcript> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    // Someone else may have shared this episode's subtitles; a hit skips the download + ASR pass
    // entirely (and works even without a local model).
    let share_keys = shared::keys(&ep);
    if let Some(t) = take_shared(&state, &ep).await {
        asr_emit(&app, &episode_id, "done", 1.0, None, None);
        return Ok(t);
    }
    let spec = asr::resolve(&state.data, &model).ok_or("未知模型")?;
    let model_path = spec.path.clone();
    if cloud.is_none() && !model_path.is_file() { return Err("请先下载识别模型".into()); }
    // One transcription per episode at a time (a second start while one runs would fight over its files).
    if state.running(&episode_id) { return Err("这一集正在转写".into()); }
    let cancel = state.job(&episode_id);
    // A folder per run: a cancelled run still finishing its cleanup can't delete a new run's audio.
    let work = state.data.join("work").join(format!("{episode_id}-{}", uuid::Uuid::new_v4().simple()));
    let result = async {
        let _ = tokio::fs::create_dir_all(&work).await;
        asr_emit(&app, &episode_id, "download", 0.0, None, None);
        let audio = if let Some(saved) = offline::find(&state.data, &episode_id) {
            PathBuf::from(saved.path)
        } else if let Some(vid) = &ep.video_id {
            if asr::find_ytdlp(&state.data).is_none() { asr_emit(&app, &episode_id, "download", 0.0, Some("正在准备 YouTube 下载工具".into()), None); }
            asr::with_ytdlp(&state.data, |bin| { let (v, w, c) = (vid.clone(), work.clone(), cancel.clone()); async move { asr::youtube_audio(&bin, &v, &w, c).await } }).await.map_err(e)?
        } else {
            let url = ep.audio_url.clone().ok_or("这集没有音频地址")?;
            let dest = work.join(format!("audio.{}", offline::audio_ext(&url)));
            let app2 = app.clone();
            let id2 = episode_id.clone();
            asr::download(&url, &dest, &cancel, move |got, total| {
                let p = total.map(|t| got as f64 / t as f64).unwrap_or(0.0);
                asr_emit(&app2, &id2, "download", p, None, None);
            }).await.map_err(e)?;
            dest
        };
        asr_emit(&app, &episode_id, "transcribe", 0.0, None, None);
        // What the episode says about itself helps spell names and terms (asr_context.rs). The "useShowNotes"
        // setting (default on) can turn it off; it is dropped when its script doesn't match the spoken language.
        let use_notes = {
            let conn = state.db.lock().unwrap();
            db::get_setting(&conn, "app").ok().flatten().and_then(|r| serde_json::from_str::<Value>(&r).ok())
                .and_then(|s| s["useShowNotes"].as_bool()).unwrap_or(true)
        };
        let meta = if use_notes {
            asr_context::Meta { title: ep.title.clone(), show: ep.show_title.clone().unwrap_or_default(), description: ep.notes.clone().unwrap_or_default(), chapters: vec![] }
        } else { asr_context::Meta::default() };
        let hotwords: Vec<String> = if use_notes { db::get_blob(&state.db.lock().unwrap(), &ep.id, "ctx").ok().flatten()
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .map(|v| v["terms"].as_array().into_iter().flatten().filter_map(|t| t["src"].as_str().map(str::to_string)).collect()).unwrap_or_default() } else { vec![] };
        let lang_for = |text: &str| if language == "auto" { asr_context::script_of(&format!("{} {}", ep.title, text)) } else { language.as_str() };
        let fit = |text: String| if asr_context::fits_language(&text, lang_for(&text)) && (language != "auto" || asr_context::script_of(&ep.title) == asr_context::script_of(&text)) { text } else { String::new() };
        let ctx = asr::AsrContext {
            prompt: fit(asr_context::prompt(&meta, asr_context::BUDGET_PROMPT)),
            background: fit(asr_context::background(&meta, asr_context::BUDGET_QWEN)),
            doubao: { let b = fit(asr_context::background(&meta, asr_context::BUDGET_DOUBAO)); if b.is_empty() && hotwords.is_empty() { String::new() } else { asr_context::doubao(&asr_context::Meta { description: b, ..Default::default() }, &hotwords) } },
            hotwords: hotwords.clone(),
        };
        let app2 = app.clone();
        let id2 = episode_id.clone();
        let out = if let Some(cfg) = &cloud {
            asr_emit(&app, &episode_id, "transcribe", 0.01, Some("正在解码音频".into()), None);
            let src = audio.clone();
            let samples = tokio::task::spawn_blocking(move || asr::decode_16k_mono(&src)).await.map_err(e)?.map_err(e)?;
            let segments = cloud::transcribe(cfg, samples, &language, &ctx, cancel.clone(), move |p| asr_emit(&app2, &id2, "transcribe", p, Some("云端识别中".into()), None)).await.map_err(e)?;
            let sample: String = segments.iter().take(5).map(|s| s.text.as_str()).collect();
            let lang = if language != "auto" { language.clone() } else if sample.chars().any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c)) { "zh".into() } else { "auto".into() };
            asr::WorkerResult { language: lang, segments }
        } else if spec.engine == "qwen3" {
            let bin = match asr::find_crispasr(&state.data) {
                Some(p) => p,
                None => {
                    asr_emit(&app, &episode_id, "download", 0.0, Some("正在准备 Qwen3-ASR 识别程序".into()), None);
                    let (a, i) = (app.clone(), episode_id.clone());
                    asr::install_crispasr(&state.data, &cancel, move |got, total| {
                        asr_emit(&a, &i, "download", total.map(|t| got as f64 / t as f64).unwrap_or(0.0), Some("正在准备 Qwen3-ASR 识别程序".into()), None);
                    }).await.map_err(e)?
                }
            };
            asr_emit(&app, &episode_id, "transcribe", 0.01, Some("正在解码音频".into()), None);
            let (src, wav) = (audio.clone(), work.join("audio16k.wav"));
            let wav2 = wav.clone();
            tokio::task::spawn_blocking(move || -> anyhow::Result<()> { asr::write_wav(&wav2, &asr::decode_16k_mono(&src)?) }).await.map_err(e)?.map_err(e)?;
            let segments = asr::run_crispasr(&bin, &model_path, &wav, &language, &ctx, cancel.clone(), move |p| asr_emit(&app2, &id2, "transcribe", p, None, None)).await.map_err(e)?;
            let sample: String = segments.iter().take(5).map(|s| s.text.as_str()).collect();
            let lang = if language != "auto" { language.clone() } else if sample.chars().any(|c| ('\u{4e00}'..='\u{9fff}').contains(&c)) { "zh".into() } else { "auto".into() };
            asr::WorkerResult { language: lang, segments }
        } else {
            asr::run_worker(&audio, &model_path, &language, &ctx.prompt, cancel.clone(), move |ev| match ev {
                asr::WorkerEvent::Progress(p) => asr_emit(&app2, &id2, "transcribe", p, None, None),
                asr::WorkerEvent::Segment(s) => asr_emit(&app2, &id2, "segment", 0.0, None, Some(s)),
            }).await.map_err(e)?
        };
        // A recogniser can echo its background on silence or music: such cues are the prompt, not speech.
        let echo_of = format!("{} {} {}", ctx.background, ctx.prompt, ctx.doubao);
        let heard: Vec<Segment> = out.segments.into_iter().filter(|s| !asr_context::is_echo(&s.text, &echo_of)).collect();
        let segments = transcript::resegment(transcript::clean(heard));
        if segments.is_empty() { return Err("没有识别出文字（可能是纯音乐或静音）".to_string()); }
        let t = Transcript { episode_id: ep.id.clone(), source: if cloud.is_some() { "cloud" } else { "whisper" }.into(), language: out.language, segments };
        db::save_transcript(&state.db.lock().unwrap(), &t).map_err(e)?;
        // What made it, for sharing the translation later (share_translation).
        let made_by = json!({ "source": if cloud.is_some() { "cloud" } else if spec.engine == "qwen3" { "qwen3" } else { "whisper" },
            "model": cloud.as_ref().map(|c| c.model.clone()).unwrap_or_else(|| spec.id.clone()) });
        let _ = db::set_blob(&state.db.lock().unwrap(), &ep.id, "asr-meta", &made_by.to_string());
        // Remember the file these times fit (size, length, fingerprint) and keep it to play from, so a listener whose
        // copy has other ads, or this app streaming a re-cut later, can still line the subtitles up.
        if ep.video_id.is_none() {
            let src = audio.clone();
            if let Ok(Ok((secs, fp))) = tokio::task::spawn_blocking(move || align::fingerprint_file(&src)).await {
                let bytes = std::fs::metadata(&audio).map(|m| m.len()).unwrap_or(0);
                let r = align::AudioRef { bytes, etag: String::new(), secs, fp: Some(align::encode_fp(&fp)), shared_id: None, has_fp: true };
                let _ = db::set_blob(&state.db.lock().unwrap(), &ep.id, "audio-ref", &serde_json::to_string(&r).unwrap_or_default());
                let _ = db::set_blob(&state.db.lock().unwrap(), &ep.id, "tmap", &serde_json::to_string(&align::TimeMap::same(bytes)).unwrap_or_default());
            }
            if audio.starts_with(&work) { let _ = offline::keep_cached(&state.data, &ep.id, &audio); }
        }
        Ok(t)
    }.await;
    let _ = tokio::fs::remove_dir_all(&work).await;
    state.done_with(&episode_id, &cancel);
    // Share what we just made, so the next person listening to this episode skips transcribing.
    if let (Ok(t), false) = (&result, share_keys.is_empty()) {
        let (share_on, install) = {
            let conn = state.db.lock().unwrap();
            let on = db::get_setting(&conn, "app").ok().flatten()
                .and_then(|r| serde_json::from_str::<Value>(&r).ok())
                .and_then(|s| s["shareTranscripts"].as_bool()).unwrap_or(true);
            (on, install_id(&conn))
        };
        if share_on {
            let last_end = t.segments.last().map(|s| s.end).unwrap_or(0.0);
            let translation = {
                let conn = state.db.lock().unwrap();
                let items: Vec<Option<String>> = t.segments.iter().map(|s| db::tr_get(&conn, &short_hash(&format!("zh-CN|{}", s.text), 24)).ok().flatten()).collect();
                let filled = items.iter().filter(|i| i.is_some()).count();
                (filled * 5 >= items.len() * 3).then(|| ("zh-CN".to_string(), items))
            };
            let meta = shared::UploadMeta {
                title: ep.title.clone(),
                show: ep.show_title.clone().unwrap_or_default(),
                duration: ep.duration.unwrap_or(0.0).max(last_end).max(1.0),
                source: if cloud.is_some() { "cloud".into() } else if spec.engine == "qwen3" { "qwen3".into() } else { "whisper".into() },
                model: cloud.as_ref().map(|c| c.model.clone()).unwrap_or_else(|| spec.id.clone()),
                language: t.language.clone(),
                install,
                translation,
                audio: audio_ref(&state, &ep.id),
            };
            let segs = t.segments.clone();
            let app2 = app.clone();
            tokio::spawn(async move {
                if let Ok(action) = shared::upload(&share_keys, meta, &segs).await { let _ = app2.emit("shared-upload", action); }
            });
        }
    }
    match &result {
        Ok(_) => asr_emit(&app, &episode_id, "done", 1.0, None, None),
        Err(m) => asr_emit(&app, &episode_id, "error", 0.0, Some(m.clone()), None),
    }
    result
}

/// A random id for this install; the server stores only a salted hash of it, for vote/report dedupe.
/// Shares an episode's finished Chinese translation with its recognised transcript, so the next listener gets
/// bilingual subtitles at once. Only transcripts the shared library takes (speech recognition, ours or shared)
/// and only with the "shareTranscripts" setting on. `engine`: llm | google. Returns the server's action.
#[tauri::command]
async fn share_translation(state: State<'_, AppState>, episode_id: String, engine: String, model: String) -> Res<String> {
    let (ep, t, items, made_by, share_on, install) = {
        let conn = state.db.lock().unwrap();
        let ep = db::get_episode(&conn, &episode_id).map_err(e)?.ok_or("节目不存在")?;
        let t = db::get_transcript(&conn, &episode_id).map_err(e)?.ok_or("没有字幕")?;
        let items: Vec<Option<String>> = db::get_blob(&conn, &episode_id, "zh").map_err(e)?.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or_default();
        let made_by = db::get_blob(&conn, &episode_id, "asr-meta").ok().flatten().and_then(|s| serde_json::from_str::<Value>(&s).ok());
        let on = db::get_setting(&conn, "app").ok().flatten().and_then(|r| serde_json::from_str::<Value>(&r).ok())
            .and_then(|s| s["shareTranscripts"].as_bool()).unwrap_or(true);
        (ep, t, items, made_by, on, install_id(&conn))
    };
    if !share_on || !matches!(engine.as_str(), "llm" | "google") { return Ok("skipped".into()); }
    // Shared hits carry their origin in the source ("shared:qwen3"); an unknown model scores lowest, so the
    // server keeps the existing segments and only takes the translation.
    let (source, asr_model) = match (t.source.as_str(), &made_by) {
        ("whisper" | "cloud", Some(m)) => (m["source"].as_str().unwrap_or("whisper").to_string(), m["model"].as_str().unwrap_or("").to_string()),
        (s, _) if s.starts_with("shared:") => (s.trim_start_matches("shared:").to_string(), String::new()),
        _ => return Ok("skipped".into()),
    };
    if !matches!(source.as_str(), "whisper" | "qwen3" | "cloud") || items.len() != t.segments.len() { return Ok("skipped".into()); }
    let filled = items.iter().filter(|i| i.as_ref().is_some_and(|s| !s.is_empty())).count();
    if filled * 10 < items.len() * 9 { return Ok("skipped".into()); }
    let keys = shared::keys(&ep);
    if keys.is_empty() { return Ok("skipped".into()); }
    let meta = shared::UploadMeta {
        title: ep.title.clone(), show: ep.show_title.clone().unwrap_or_default(),
        duration: ep.duration.unwrap_or(0.0).max(t.segments.last().map(|s| s.end).unwrap_or(0.0)).max(1.0),
        source, model: asr_model, language: t.language.clone(), install,
        translation: Some(("zh-CN".to_string(), items)),
        audio: audio_ref(&state, &episode_id),
    };
    shared::upload_with(&keys, meta, &t.segments, &engine, &model).await.map_err(e)
}

/// A 导读 someone shared for this episode; kept as this episode's own (blob "summary") when found.
#[tauri::command]
async fn shared_guide(state: State<'_, AppState>, episode_id: String) -> Res<Option<String>> {
    let ep = db::get_episode(&state.db.lock().unwrap(), &episode_id).map_err(e)?.ok_or("节目不存在")?;
    let Some(text) = shared::lookup_guide(&shared::keys(&ep)).await else { return Ok(None) };
    let conn = state.db.lock().unwrap();
    let _ = db::set_blob(&conn, &episode_id, "summary", &text);
    let _ = db::set_blob(&conn, &episode_id, "summary-from", "shared");
    Ok(Some(text))
}

/// Shares a 导读 made here, when 「匿名分享」 is on.
#[tauri::command]
async fn share_guide(state: State<'_, AppState>, episode_id: String, text: String, model: String) -> Res<String> {
    let (ep, on, install) = {
        let conn = state.db.lock().unwrap();
        let ep = db::get_episode(&conn, &episode_id).map_err(e)?.ok_or("节目不存在")?;
        let on = db::get_setting(&conn, "app").ok().flatten().and_then(|r| serde_json::from_str::<Value>(&r).ok())
            .and_then(|s| s["shareTranscripts"].as_bool()).unwrap_or(true);
        let _ = db::set_blob(&conn, &episode_id, "summary-from", "");
        (ep, on, install_id(&conn))
    };
    let keys = shared::keys(&ep);
    if !on || keys.is_empty() { return Ok("skipped".into()); }
    shared::upload_guide(&keys, &ep.title, ep.show_title.as_deref().unwrap_or(""), &model, &text, install).await.map_err(e)
}

fn install_id(conn: &Connection) -> String {
    if let Ok(Some(id)) = db::get_setting(conn, "install") { return id; }
    let id = uuid::Uuid::new_v4().to_string();
    let _ = db::set_setting(conn, "install", &id);
    id
}

#[tauri::command]
fn cancel_job(state: State<'_, AppState>, id: String) {
    if let Some(f) = state.jobs.lock().unwrap().get(&id) { f.store(true, Ordering::Relaxed); }
}

#[tauri::command]
async fn asr_models(state: State<'_, AppState>) -> Res<Vec<asr::ModelInfo>> {
    // Looking through other apps' folders takes a second or two: off the main thread.
    let data = state.data.clone();
    tokio::task::spawn_blocking(move || asr::list_models(&data)).await.map_err(e)
}

#[tauri::command]
async fn download_model(app: AppHandle, state: State<'_, AppState>, id: String, mirror_first: bool) -> Res<()> {
    let job = format!("model:{id}");
    let cancel = state.job(&job);
    let app2 = app.clone();
    let id2 = id.clone();
    let r = asr::download_model(&state.data, &id, &cancel, mirror_first, move |got, total| {
        let _ = app2.emit("model-download", json!({"id": id2, "received": got, "total": total}));
    }).await;
    state.done_with(&job, &cancel);
    r.map_err(e)
}

/// Listening time from the player (only while playing; seeks don't count).
#[tauri::command(async)]
fn log_listen(state: State<'_, AppState>, episode_id: String, show_id: String, seconds: f64, translated: bool) -> Res<()> {
    db::log_listen(&state.db.lock().unwrap(), &episode_id, &show_id, seconds, translated).map_err(e)
}

#[tauri::command(async)]
fn listening_stats(state: State<'_, AppState>, since: String) -> Res<Vec<Value>> {
    db::listens_since(&state.db.lock().unwrap(), &since).map_err(e)
}

#[tauri::command]
fn asr_elsewhere() -> Vec<asr::Elsewhere> { asr::elsewhere() }

#[tauri::command]
fn rescan_models() { asr::rescan(); }

#[tauri::command]
fn delete_model(state: State<'_, AppState>, id: String) -> Res<()> {
    // A link to a file another app keeps is only unlinked; the file itself stays.
    let r = asr::resolve(&state.data, &id).ok_or("未知模型")?;
    let _ = std::fs::remove_file(&r.path);
    Ok(())
}

// ---- stored data and settings ------------------------------------------------------------------------

#[tauri::command(async)]
fn get_blob(state: State<'_, AppState>, episode_id: String, kind: String) -> Res<Option<String>> {
    db::get_blob(&state.db.lock().unwrap(), &episode_id, &kind).map_err(e)
}

#[tauri::command(async)]
fn set_blob(state: State<'_, AppState>, episode_id: String, kind: String, data: String) -> Res<()> {
    db::set_blob(&state.db.lock().unwrap(), &episode_id, &kind, &data).map_err(e)
}

#[tauri::command(async)]
fn get_settings(state: State<'_, AppState>) -> Res<Value> {
    let raw = db::get_setting(&state.db.lock().unwrap(), "app").map_err(e)?;
    Ok(raw.and_then(|r| serde_json::from_str(&r).ok()).unwrap_or(Value::Null))
}

#[tauri::command(async)]
fn set_settings(state: State<'_, AppState>, settings: Value) -> Res<()> {
    net::set_proxy(settings["proxy"].as_str().unwrap_or("")).map_err(e)?;
    *asr::YT_BROWSER.write().unwrap() = settings["ytCookiesBrowser"].as_str().unwrap_or("").to_string();
    db::set_setting(&state.db.lock().unwrap(), "app", &settings.to_string()).map_err(e)
}

#[tauri::command]
fn set_api_key(provider: String, key: String) -> Res<()> { llm::set_key(&provider, key.trim()).map_err(e) }

// Off the main thread: the window keeps drawing while it runs.
#[tauri::command(async)]
fn has_api_key(provider: String) -> bool { llm::get_key(&provider).is_some_and(|k| !k.is_empty()) }

#[tauri::command]
async fn llm_chat(app: AppHandle, state: State<'_, AppState>, request_id: String, provider: llm::Provider, system: String, messages: Vec<llm::Message>, max_tokens: Option<u32>, json_mode: Option<bool>, quick: Option<bool>) -> Res<String> {
    let cancel = state.job(&request_id);
    let rid = request_id.clone();
    let r = if provider.protocol == "cli" {
        // A local agent (agents.rs): the conversation goes as one message; it keeps no history of its own.
        let kind = agents::Kind::parse(provider.id.trim_start_matches("cli-")).ok_or("未知的本机 AI")?;
        let path = agents::path_of(kind).ok_or_else(|| format!("没有找到 {}，请在设置里点「重新检测」", agents::display(kind)))?;
        let user = flatten(&messages);
        let app2 = app.clone();
        agents::complete(kind, &path, agents::Request { system: &system, user: &user, model: Some(provider.model.as_str()).filter(|m| !m.is_empty() && *m != "default"), json: json_mode.unwrap_or(false) },
            cancel.clone(), move |d| { let _ = app2.emit("llm", json!({"requestId": rid, "delta": d})); }).await
    } else {
        llm::chat(&provider, &system, &messages, max_tokens.unwrap_or(8000), json_mode.unwrap_or(false), quick.unwrap_or(false), &cancel, |d| {
            let _ = app.emit("llm", json!({"requestId": rid, "delta": d}));
        }).await
    };
    state.done_with(&request_id, &cancel);
    r.map_err(e)
}

/// A conversation as one message, for agents that take a single prompt.
fn flatten(messages: &[llm::Message]) -> String {
    if messages.len() == 1 { return messages[0].content.clone(); }
    let mut out = String::from("以下是到目前为止的对话，请回答最后一个问题。\n\n"); // i18n-ignore (sent to the model)
    for m in messages { out.push_str(if m.role == "assistant" { "【助手】\n" } else { "【用户】\n" }); out.push_str(&m.content); out.push_str("\n\n"); } // i18n-ignore
    out
}

/// The listener's YouTube subscriptions, read with the given browser's sign-in (import.rs).
#[tauri::command]
async fn youtube_subscriptions(state: State<'_, AppState>, browser: String) -> Res<Vec<import::Channel>> {
    if !matches!(browser.as_str(), "chrome" | "safari" | "firefox" | "edge" | "brave" | "chromium" | "opera" | "vivaldi") { return Err("不支持这个浏览器".into()); }
    let bin = asr::ytdlp(&state.data).await.map_err(e)?;
    import::youtube_subscriptions(&bin, &browser).await.map_err(e)
}

/// Opens System Settings where the listener lets SubCast read a browser's data (macOS).
#[tauri::command]
fn open_privacy_settings() -> Res<()> { import::open_privacy_settings().map_err(e) }

/// Writes every subscription with a feed to an OPML file in Downloads and returns its path.
#[tauri::command]
fn export_opml(state: State<'_, AppState>) -> Res<String> {
    let shows = db::list_shows(&state.db.lock().unwrap()).map_err(e)?;
    let list: Vec<(String, String)> = shows.into_iter().filter(|s| s.feed_url.starts_with("http")).map(|s| (s.title, s.feed_url)).collect();
    let dir = dirs::download_dir().or_else(dirs::home_dir).ok_or("找不到下载文件夹")?;
    let path = dir.join(format!("SubCast-{}.opml", chrono::Local::now().format("%Y%m%d")));
    std::fs::write(&path, import::opml(&list)).map_err(e)?;
    Ok(path.to_string_lossy().into())
}

/// Local AI agents on this computer (Claude Code, Codex, Qwen Code…) and whether they are logged in.
#[tauri::command]
async fn local_agents() -> Vec<agents::Found> { agents::detect().await }

/// Starts an agent once to see whether it can answer (ready / needsLogin), without a model call.
#[tauri::command]
async fn probe_agent(kind: String) -> Res<String> {
    let k = agents::Kind::parse(&kind).ok_or("未知的本机 AI")?;
    let path = agents::path_of(k).ok_or("没有找到")?;
    agents::probe(k, &path).await.map(str::to_string).map_err(e)
}

/// The models a local agent says it supports (for the 「问答里可选的模型」 checklist).
#[tauri::command]
async fn agent_models(kind: String) -> Res<Vec<agents::AgentModel>> {
    let k = agents::Kind::parse(&kind).ok_or("未知的本机 AI")?;
    let path = agents::path_of(k).ok_or("没有找到")?;
    agents::models(k, &path).await.map_err(e)
}

/// In-app updates (appupdate.rs): check, download from the faster source, install and restart.
#[tauri::command]
async fn app_update_check(app: AppHandle) -> Res<Option<appupdate::Found>> { appupdate::check(&app).await.map_err(e) }

#[tauri::command]
async fn app_update_download(app: AppHandle) -> Res<()> { appupdate::download(&app).await.map_err(e) }

#[tauri::command]
fn app_update_install(app: AppHandle) -> Res<()> { appupdate::install(&app).map_err(e) }

/// Starts an agent in the background (opening Q&A), so the first answer doesn't wait for it.
#[tauri::command]
async fn warm_agent(kind: String) {
    if let Some(k) = agents::Kind::parse(&kind) { if let Some(p) = agents::path_of(k) { agents::warm(k, &p).await; } }
}

/// Machine translation (Google, free) with a persistent cache; used for titles, show notes, and subtitles
/// when no model of the user's own is set up.
#[tauri::command]
async fn translate_texts(state: State<'_, AppState>, texts: Vec<String>, target: Option<String>) -> Res<Vec<String>> {
    let tl = target.unwrap_or_else(|| "zh-CN".into());
    let keys: Vec<String> = texts.iter().map(|t| short_hash(&format!("{tl}|{t}"), 24)).collect();
    let mut out: Vec<Option<String>> = {
        let conn = state.db.lock().unwrap();
        keys.iter().map(|k| db::tr_get(&conn, k).ok().flatten()).collect()
    };
    let missing: Vec<usize> = (0..texts.len()).filter(|&i| out[i].is_none() && !texts[i].trim().is_empty()).collect();
    if !missing.is_empty() {
        let lines: Vec<String> = missing.iter().map(|&i| texts[i].clone()).collect();
        let done = translate::translate_lines(&lines, &tl).await.map_err(e)?;
        let conn = state.db.lock().unwrap();
        let tx = conn.unchecked_transaction().map_err(e)?;
        for (j, &i) in missing.iter().enumerate() {
            if let Some(t) = done.get(j) { let _ = db::tr_put(&tx, &keys[i], t); out[i] = Some(t.clone()); }
        }
        tx.commit().map_err(e)?;
    }
    Ok(out.into_iter().map(Option::unwrap_or_default).collect())
}

/// The native menu's own items, in the interface language: [settings].
#[tauri::command]
fn set_menu_labels(app: AppHandle, labels: Vec<String>) -> Res<()> {
    let Some(menu) = app.menu() else { return Ok(()) };
    for sub in menu.items().map_err(e)? {
        let Some(sub) = sub.as_submenu() else { continue };
        if let Some(item) = sub.get("settings").and_then(|i| i.as_menuitem().cloned()) {
            if let Some(l) = labels.first() { item.set_text(l).map_err(e)?; }
        }
    }
    Ok(())
}

// ---- app ------------------------------------------------------------------------------------------------

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--asr-worker") {
        std::process::exit(asr::worker_main(&args[2..]));
    }
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_process::init())
        .setup(|app| {
            #[cfg(desktop)]
            app.handle().plugin(tauri_plugin_updater::Builder::new().build())?;
            ytlogin::init(app.handle());
            let data = app.path().app_data_dir()?;
            std::fs::create_dir_all(&data)?;
            if let Ok(res) = app.path().resource_dir() { let _ = asr::VCRT_DIR.set(res.join("vcrt")); }
            let conn = db::open(&data.join("subcast.db"))?;
            conn.execute("INSERT OR IGNORE INTO shows(id,kind,title,feed_url,updated_at) VALUES('single','single','单独打开','',0)", [])?;
            if let Some(s) = db::get_setting(&conn, "app")?.and_then(|r| serde_json::from_str::<Value>(&r).ok()) {
                let _ = net::set_proxy(s["proxy"].as_str().unwrap_or(""));
                *asr::YT_BROWSER.write().unwrap() = s["ytCookiesBrowser"].as_str().unwrap_or("").to_string();
            }
            // Release builds serve the interface over http://localhost (YouTube refuses tauri://), from a hot
            // update when one is active (ota.rs), else from the files built into the app.
            #[cfg(not(debug_assertions))]
            let (ui, ui_port): (ota::Active, u16) = {
                let active = std::sync::Arc::new(std::sync::RwLock::new(ota::boot(&data)));
                let port = serve_ui(app.handle(), active.clone());
                (active, port)
            };
            #[cfg(not(debug_assertions))]
            let ui_data = data.clone();
            if let Some(v) = db::get_setting(&conn, "remote-config")?.and_then(|r| serde_json::from_str::<Value>(&r).ok()) { apply_config(&v); }
            app.manage(AppState { db: Mutex::new(conn), data, jobs: Mutex::new(HashMap::new()) });
            #[cfg(debug_assertions)]
            let url = tauri::WebviewUrl::App("index.html".into());
            #[cfg(not(debug_assertions))]
            let url = tauri::WebviewUrl::External(format!("http://localhost:{ui_port}").parse().unwrap());
            let builder = tauri::WebviewWindowBuilder::new(app, "main", url)
                .title("SubCast")
                .inner_size(1360.0, 880.0)
                .min_inner_size(980.0, 640.0);
            // macOS: no title bar; the traffic lights sit inside the sidebar and the page draws its own drag areas.
            #[cfg(target_os = "macos")]
            let builder = builder
                .title_bar_style(tauri::TitleBarStyle::Overlay)
                .hidden_title(true)
                .traffic_light_position(tauri::LogicalPosition::new(18.0, 22.0));
            builder.build()?;
            // A hot-updated interface that hasn't said it rendered within READY_SECS is dropped for good.
            #[cfg(not(debug_assertions))]
            {
                let handle = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(ota::READY_SECS)).await;
                    if ota::give_up(&ui_data, &ui) {
                        if let Some(w) = handle.get_webview_window("main") { let _ = w.eval("location.reload()"); }
                    }
                });
            }
            // The stock macOS menu has no Settings item, so ⌘, did nothing.
            let menu = tauri::menu::Menu::default(app.handle())?;
            if let Some(first) = menu.items()?.first().and_then(|m| m.as_submenu().cloned()) {
                let item = tauri::menu::MenuItem::with_id(app, "settings", "设置…", true, Some("CmdOrCtrl+,"))?;
                let sep = tauri::menu::PredefinedMenuItem::separator(app)?;
                first.insert_items(&[&sep, &item], 1)?;
            }
            app.set_menu(menu)?;
            Ok(())
        })
        .on_menu_event(|app, event| {
            if event.id() == "settings" { let _ = app.emit("open-settings", ()); }
        })
        .invoke_handler(tauri::generate_handler![
            add_subscription, refresh_show, refresh_all, list_shows, list_episodes, recent_episodes, get_episode, remove_show,
            set_progress, set_watched, mark_show_watched, search_podcasts, search_show, load_transcript, fetch_official_transcript, transcribe, cancel_job, asr_models,
            download_model, delete_model, get_blob, set_blob, get_settings, set_settings, set_api_key, has_api_key, llm_chat, translate_texts, shared_transcript, shared_exists, shared_guide, share_guide, asr_elsewhere, log_listen, listening_stats, rescan_models, youtube_subscriptions, open_privacy_settings, export_opml, local_agents, probe_agent, agent_models, warm_agent, share_translation, ui_ready, ui_check, refresh_config, youtube_stream, prepare_youtube, youtube_sign_in, youtube_signed_in, youtube_sign_out, youtube_tracks, use_youtube_track, youtube_channel_feed, qiaomu_detail, preview_show, list_models, tts_speak, cached_show, save_image, learn_clip, save_text, export_anki, set_menu_labels,
            app_update_check, app_update_download, app_update_install,
            shadow_start, shadow_stop, shadow_clear, shadow_ready, shadow_hear, open_mic_settings,
            download_episode, list_downloads, delete_download, clear_downloads, align_audio, save_time_map, list_audio_cache, clear_audio_cache, storage_info, open_data_folder, reveal_download, clear_temp, full_notes, tts_synthesize, tts_system_voices, clear_tts_cache, local_tts_status, local_tts_install, local_tts_delete,
            video_clip_env, video_clip_warm, video_clip_prepare, video_clip_render, reveal_file
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod live {
    //! Network tests against real services: `cargo test -- --ignored --nocapture`.
    use super::*;
    #[tokio::test]
    #[ignore]
    async fn qiaomu_rss_sources() {
        let ids = ["nexttoken", "bannatie", "latentspace", "podscribe-acquired", "podscribe-pivot", "podscribe-freakonomics-radio", "podscribe-invest-like-the-best", "podscribe-the-diary-of-a-ceo-with-steven-bartlett"];
        for id in ids {
            let ch = crate::qiaomu::rss_channel(id).await.unwrap();
            let playable = |x: &Episode| x.audio_url.is_some() || x.video_id.is_some();
            println!("{id}: {} eps={} playable={} video={} deep={} cover={}", ch.show.title, ch.episodes.len(), ch.episodes.iter().filter(|x| playable(x)).count(), ch.episodes.iter().filter(|x| x.video_id.is_some()).count(), ch.episodes.iter().filter(|x| x.deep).count(), ch.show.cover.is_some());
            if id == "nexttoken" { assert!(ch.episodes.iter().filter(|x| x.video_id.is_some()).count() * 2 >= ch.episodes.len(), "nexttoken episodes should mostly have the YouTube video"); }
            assert!(!id.starts_with("podscribe-") || ch.episodes.iter().all(|x| x.transcript_url.is_some()), "{id}: podscribe episodes need a transcript");
            assert!(id.starts_with("podscribe-") || ch.episodes.iter().all(playable), "{id}: unplayable entries should be skipped");
            if id.starts_with("podscribe-") {
                let pool = crate::qiaomu::borrowable(id, &ch.show.title).await;
                for x in ch.episodes.iter().take(6) {
                    let m = crate::qiaomu::find_audio(&pool, &x.title, x.published).map(|b| b.title.chars().take(60).collect::<String>());
                    println!("  {:<62} -> {:?}", x.title.chars().take(60).collect::<String>(), m);
                }
            }
            let Some(e) = ch.episodes.iter().find(|x| x.deep) else { continue };
            let rw = crate::qiaomu::rss_rewrite(&e.guid).await.unwrap();
            assert!(rw.is_some(), "{id}: rewriteReady entry has no rewrite");
            println!("  rewrite={}", rw.as_ref().map(|h| h.chars().take(120).collect::<String>()).unwrap_or_default());
            if let Some(u) = &e.transcript_url { let s = crate::qiaomu::podscribe_transcript(u).await.unwrap(); println!("  segs={} {:?}", s.len(), s.get(1).map(|x| (x.start, x.text.chars().take(40).collect::<String>()))); }
        }
    }
    #[tokio::test]
    #[ignore]
    async fn qiaomu_channels() {
        for id in ["lex", "nopriors", "zhangxiaojun"] {
            let ch = crate::qiaomu::channel(id).await.unwrap();
            let ep = &ch.episodes[0];
            println!("{id}: {} | cover={:?} | eps={} | zh={} | first={} video={:?}", ch.show.title, ch.show.cover, ch.episodes.len(), ch.zh_titles.len(), ep.title, ep.video_id);
            let d = crate::qiaomu::detail(ep.link.as_ref().unwrap()).await.unwrap();
            println!("  audio={:?} deep={} transcript={}", d.audio, d.deep_read.as_ref().map(|h| h.len()).unwrap_or(0), d.transcript.as_ref().map(|t| t.len()).unwrap_or(0));
            if let Some(t) = &d.transcript { let segs = crate::qiaomu::transcript_segments(t, ep.duration.unwrap_or(3600.0)); println!("  segs={} first={:?}", segs.len(), segs.first().map(|s| s.text.chars().take(80).collect::<String>())); }
        }
    }
    #[tokio::test]
    #[ignore]
    async fn sources() {
        for input in ["https://www.youtube.com/@DwarkeshPatel", "https://podcasts.apple.com/us/podcast/no-priors/id1668002688", "https://www.xiaoyuzhoufm.com/podcast/5e5c52c9418a84a04625e6cc", "https://feeds.megaphone.fm/nopriors"] {
            let (show, eps) = match feeds::resolve(input).await.unwrap() {
                feeds::Source::Feed(u) => feeds::fetch_show(&u, "podcast").await.unwrap(),
                feeds::Source::Xiaoyuzhou(u) => feeds::xiaoyuzhou_show(&u).await.unwrap(),
                _ => panic!("unexpected source for {input}"),
            };
            println!("{input} -> {} [{}] {} episodes; first: {:?} audio={:?} video={:?} transcript={:?}", show.title, show.kind, eps.len(), eps[0].title, eps[0].audio_url.is_some(), eps[0].video_id, eps[0].transcript_url);
            assert!(!eps.is_empty());
        }
        let r = feeds::search_podcasts("lex fridman", "US").await.unwrap();
        println!("search: {} results, first {}", r.len(), r[0].title);
    }
    #[tokio::test]
    #[ignore]
    async fn discover_picks() {
        let inputs = [
            "https://www.xiaoyuzhoufm.com/podcast/5e4ee557418a84a0466737b7", "https://www.xiaoyuzhoufm.com/podcast/5e2831ed418a84a046231c00",
            "https://www.xiaoyuzhoufm.com/podcast/5f72b66083c34e85dd14fde9", "https://www.xiaoyuzhoufm.com/podcast/5e4515bd418a84a046e2b11a",
            "https://www.xiaoyuzhoufm.com/podcast/611719d3cb0b82e1df0ad29e", "https://www.xiaoyuzhoufm.com/podcast/6022a180ef5fdaddc30bb101",
            "https://www.xiaoyuzhoufm.com/podcast/61dd99a47b29652ff572257b", "https://www.xiaoyuzhoufm.com/podcast/61358d971c5d56efe5bcb5d2",
            "https://www.xiaoyuzhoufm.com/podcast/5e74b52c418a84a046ecaceb", "https://www.xiaoyuzhoufm.com/podcast/5e280faf418a84a0461fbd39",
            "https://www.xiaoyuzhoufm.com/podcast/646d6bfa53a5e5ea14e69c7c", "https://www.xiaoyuzhoufm.com/podcast/64379296ff8a107611a67208",
            "https://www.xiaoyuzhoufm.com/podcast/6627fda4b56459544087d86a", "https://www.xiaoyuzhoufm.com/podcast/68981df29e7bcd326eb91d88",
            "https://www.xiaoyuzhoufm.com/podcast/5e285856418a84a04627b7be", "https://www.xiaoyuzhoufm.com/podcast/62694abdb221dd5908417d1e",
            "https://www.youtube.com/@hubermanlab", "https://www.youtube.com/@MachineLearningStreetTalk", "https://www.youtube.com/@Bg2Pod",
            "https://www.youtube.com/@MyFirstMillionPod", "https://www.youtube.com/@ChrisWillx", "https://www.youtube.com/@timferriss",
            "https://www.youtube.com/@Alex.kantrowitz", "https://www.youtube.com/@tkppodcast", "https://www.youtube.com/@cheekypint",
            "https://www.youtube.com/@PeterAttiaMD", "https://www.youtube.com/@peterdiamandis", "https://www.youtube.com/@EveryInc",
        ];
        let mut bad = vec![];
        for input in inputs {
            let r = match feeds::resolve(input).await {
                Ok(feeds::Source::Feed(u)) => feeds::fetch_show(&u, "podcast").await,
                Ok(feeds::Source::Xiaoyuzhou(u)) => feeds::xiaoyuzhou_show(&u).await,
                Ok(_) => Err(anyhow::anyhow!("unexpected source")),
                Err(err) => Err(err),
            };
            match r {
                Ok((show, eps)) => {
                    let newest = eps.iter().filter_map(|x| x.published).max().unwrap_or(0);
                    let days = (now_ms() - newest) / 86_400_000;
                    let playable = eps.iter().filter(|x| x.audio_url.is_some() || x.video_id.is_some()).count();
                    println!("{input}\n  -> {} | eps={} playable={} newest={}d ago | cover={:?}", show.title, eps.len(), playable, days, show.cover);
                    if eps.is_empty() || playable == 0 { bad.push(input); }
                }
                Err(err) => { println!("{input}\n  !! {err}"); bad.push(input); }
            }
        }
        assert!(bad.is_empty(), "failed: {bad:?}");
    }
    #[tokio::test]
    #[ignore]
    async fn first_stream_reports_the_download() {
        let data = std::env::temp_dir().join(format!("subcast-ytdlp-{}", uuid::Uuid::new_v4()));
        let mut seen = 0.0f64;
        let (a, b) = tokio::join!(asr::ytdlp_with(&data, |got, total| seen = total.map_or(0.0, |t| got as f64 / t as f64)), asr::ytdlp(&data));
        assert_eq!(a.unwrap(), b.unwrap());
        assert!(seen > 0.99, "progress reached {seen}");
        let url = asr::with_ytdlp(&data, |b| async move { asr::youtube_stream_url(&b, "tBMlXQIrYK4", true, true).await }).await.unwrap();
        assert!(url.contains("m3u8") || url.contains("hls_"));
        let mp4 = asr::with_ytdlp(&data, |b| async move { asr::youtube_stream_url(&b, "tBMlXQIrYK4", false, true).await }).await.unwrap();
        assert!(mp4.contains("googlevideo.com/videoplayback"), "{mp4}");
        let _ = std::fs::remove_dir_all(&data);
    }
    #[tokio::test]
    #[ignore]
    async fn ytdlp_stays_current() {
        let data = std::env::temp_dir().join(format!("subcast-ytdlp-{}", uuid::Uuid::new_v4()));
        let bin = asr::ytdlp(&data).await.unwrap();
        assert!(bin.starts_with(&data), "uses its own copy");
        std::fs::remove_file(data.join("bin/yt-dlp.checked")).ok();
        // Freshly installed from the latest release: the check finds nothing newer.
        assert!(!asr::update_ytdlp(&data, false).await.unwrap());
        // Checked just now: no second network call within the day.
        assert!(!asr::update_ytdlp(&data, false).await.unwrap());
        let url = asr::with_ytdlp(&data, |b| async move { asr::youtube_stream_url(&b, "dQw4w9WgXcQ", true, true).await }).await.unwrap();
        println!("stream: {}", &url[..url.len().min(80)]);
        let _ = std::fs::remove_dir_all(&data);
    }
    /// Another program holding the port on IPv6 only (as the iOS Simulator's app did) must not win "localhost".
    #[test]
    fn ui_port_skips_a_port_taken_on_ipv6() {
        let base = 45_000 + (std::process::id() % 1000) as u16 * 10;
        let Ok(_other) = std::net::TcpListener::bind(("::1", base)) else { return }; // no IPv6 here
        let (port, listeners) = super::bind_ui_port(base).expect("a free port");
        assert_eq!(port, base + 1);
        assert_eq!(listeners.len(), 2);
        let (port2, _) = super::bind_ui_port(base + 5).expect("a free port");
        assert_eq!(port2, base + 5);
    }
    /// Every port the interface may move to is allowed to call the app's commands.
    #[test]
    fn every_ui_port_is_allowed() {
        let caps = include_str!("../capabilities/default.json");
        for p in super::LOCAL_PORT..super::LOCAL_PORT + super::UI_PORTS {
            assert!(caps.contains(&format!("\"http://localhost:{p}\"")), "port {p} missing from capabilities");
        }
    }
    /// SUBCAST_DB: a copy of a real library. Every transcribed episode in it should come back from the shared library.
    #[tokio::test]
    #[ignore]
    async fn shared_library_hits() {
        let conn = rusqlite::Connection::open(std::env::var("SUBCAST_DB").unwrap()).unwrap();
        let ids: Vec<(String, String)> = conn.prepare("SELECT episode_id, source FROM transcripts WHERE source NOT LIKE 'youtube%'").unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(Result::unwrap).collect();
        for (id, source) in ids {
            let Some(ep) = db::get_episode(&conn, &id).unwrap() else { continue };
            let hit = shared::lookup(&shared::keys(&ep)).await;
            println!("{source:>14} {:<40} shared: {}", ep.title.chars().take(40).collect::<String>(),
                hit.map(|h| format!("{} lines, zh {}", h.segments.len(), h.translation.map(|t| t.items.len()).unwrap_or(0))).unwrap_or("none".into()));
        }
    }
    /// SUBCAST_DB: a copy of a real library.
    #[test]
    #[ignore]
    fn search_inside_a_show() {
        let conn = rusqlite::Connection::open(std::env::var("SUBCAST_DB").unwrap()).unwrap();
        let shows: Vec<(String, String)> = conn.prepare("SELECT s.id, s.title FROM shows s WHERE EXISTS(SELECT 1 FROM transcripts t JOIN episodes e ON e.id=t.episode_id WHERE e.show_id=s.id)").unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().map(Result::unwrap).collect();
        for (id, title) in shows.iter().take(4) {
            for q in ["AI", "黄仁勋", "agent model", "the"] {
                let t0 = std::time::Instant::now();
                let hits = super::search_episodes(&conn, id, q).unwrap();
                let first = hits.first().map(|h| format!("{} [{}] {}", h.field, h.time.map(|t| t as i64).unwrap_or(-1), h.snippet)).unwrap_or_default();
                println!("{title} / {q}: {} hits in {:?}; {first}", hits.len(), t0.elapsed());
            }
        }
    }
    #[tokio::test]
    #[ignore]
    async fn chinese_show_opens_in_chinese() {
        let extra = std::env::var("YT_IDS").unwrap_or_default();
        for id in ["tBMlXQIrYK4", "1NHPDzpfR6Y", "2hJkprFnzhY"].into_iter().chain(extra.split_whitespace()) {
            let info = youtube::video_info(id).await.unwrap();
            let Some(t) = youtube::choose(&info.tracks, info.original_language.as_deref(), youtube::script_hint(&info.title)) else { println!("{id}: no captions"); continue };
            let zh = youtube::official_chinese(&info.tracks, &t.language).map(|z| z.language.clone());
            let langs: Vec<String> = info.tracks.iter().map(|t| format!("{}{}", t.language, if t.auto { "*" } else { "" })).collect();
            println!("{id} {} → {} official_zh={zh:?} tracks={langs:?}", info.title, t.language);
            if id == "tBMlXQIrYK4" { assert!(t.language.starts_with("zh")); }
            if let Some(z) = youtube::official_chinese(&info.tracks, &t.language) {
                let orig = transcript::resegment(transcript::clean(youtube::fetch_track(t).await.unwrap()));
                let items = youtube::align(&orig, &transcript::clean(youtube::fetch_track(z).await.unwrap()));
                println!("  coverage {}/{}; {:?} / {:?}", items.iter().filter(|x| x.is_some()).count(), items.len(), orig[3].text, items[3]);
            }
        }
    }
    #[tokio::test]
    #[ignore]
    async fn captions() {
        let info = youtube::video_info("dQw4w9WgXcQ").await.unwrap();
        let t = youtube::choose(&info.tracks, info.original_language.as_deref(), youtube::script_hint(&info.title)).unwrap();
        let segs = transcript::resegment(transcript::clean(youtube::fetch_track(t).await.unwrap()));
        println!("{} lang={} auto={} {} blocks; {:?}", info.title, t.language, t.auto, segs.len(), &segs[..2]);
        assert!(segs.len() > 5);
    }
    #[tokio::test]
    #[ignore]
    async fn qwen3_pipeline() {
        // SUBCAST_TEST_DIR holds clip.mp3 and qwen3-asr-0.6b-q8_0.gguf.
        let dir = std::path::PathBuf::from(std::env::var("SUBCAST_TEST_DIR").unwrap());
        let data = dir.join("data");
        let flag = std::sync::Arc::new(AtomicBool::new(false));
        let bin = match asr::find_crispasr(&data) { Some(b) => b, None => asr::install_crispasr(&data, &flag, |_, _| {}).await.unwrap() };
        let wav = dir.join("t16k.wav");
        asr::write_wav(&wav, &asr::decode_16k_mono(&dir.join("clip.mp3")).unwrap()).unwrap();
        let ctx = asr::AsrContext { background: std::env::var("SUBCAST_TEST_CONTEXT").unwrap_or_default(), ..Default::default() };
        let segs = asr::run_crispasr(&bin, &dir.join("qwen3-asr-0.6b-q8_0.gguf"), &wav, &std::env::var("SUBCAST_TEST_LANG").unwrap_or("auto".into()), &ctx, flag, |p| println!("progress {p}")).await.unwrap();
        for s in &segs { println!("[{:.1}-{:.1}] {}", s.start, s.end, s.text); }
        assert!(segs.len() >= 2);
    }
}
