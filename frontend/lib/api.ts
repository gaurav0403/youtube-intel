const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8001";

async function fetchAPI<T>(path: string): Promise<T> {
  const resp = await fetch(`${API_URL}${path}`);
  if (!resp.ok) throw new Error(`API error: ${resp.status}`);
  return resp.json();
}

// ─── Types ───────────────────────────────────────────────────────────────────

export interface VideoItem {
  video_id: string;
  title: string;
  channel_title: string;
  channel_id: string;
  published_at: string;
  thumbnail: string;
  view_count: number;
  like_count: number;
  comment_count: number;
  duration: string;
  has_transcript: boolean;
}

export interface NarrativeAngle {
  title: string;
  sentiment: "positive" | "negative" | "neutral" | "mixed";
  video_count: number;
  description: string;
  key_claims: string[];
  top_videos: {
    video_id: string;
    title: string;
    channel: string;
    views: number;
    why: string;
  }[];
}

export interface ChannelAnalysis {
  channel_name: string;
  channel_id: string;
  subscriber_count: number;
  videos_on_topic: number;
  bias: string;
  influence_score: "high" | "medium" | "low";
}

export interface YouTubeReport {
  id?: number;
  topic: string;
  hours: number;
  generated_at: string;
  video_count: number;
  videos: VideoItem[];
  analysis: {
    executive_summary: string;
    total_views: number;
    narrative_angles: NarrativeAngle[];
    channel_analysis: ChannelAnalysis[];
    comment_sentiment: {
      positive_pct: number;
      negative_pct: number;
      neutral_pct: number;
      top_themes: string[];
    };
    key_claims_tracked: {
      claim: string;
      videos_making_claim: number;
      assessment: string;
    }[];
    trending_signals: {
      velocity: string;
      peak_period: string;
      prediction: string;
    };
    related_topics: string[];
    error?: string;
    raw?: string;
  } | null;
  youtube_units_used: number;
  gemini_cost_usd: number;
  error?: string | null;
}

export interface YouTubeReportSummary {
  id: number;
  topic: string;
  hours: number;
  generated_at: string;
  video_count: number;
  gemini_cost_usd: number;
  youtube_units_used: number;
  has_error: boolean;
}

// ─── Channel types ──────────────────────────────────────────────────────────

export interface WatchedChannel {
  id: number;
  channel_id: string;
  channel_name: string;
  subscriber_count: number;
  thumbnail: string;
  is_active: boolean;
  added_at: string;
  last_checked_at: string | null;
  video_count: number;
  category: string | null;
}

export interface ChannelCategory {
  category: string;
  count: number;
}

export interface ChannelVideoItem {
  id: number;
  video_id: string;
  channel_id: string;
  channel_name?: string;
  title: string;
  published_at: string;
  thumbnail: string;
  view_count: number;
  topic_classification: string;
  summary: string;
  detected_at: string;
}

// ─── Monitoring report types ────────────────────────────────────────────────

export interface MonitoringNarrative {
  title: string;
  description: string;
  sentiment: "positive" | "negative" | "neutral" | "mixed";
  video_count: number;
  total_views: number;
  channels_pushing: string[];
  categories_involved: string[];
  key_claims: string[];
  top_videos: { video_id: string; title: string; channel: string; views: number; why: string }[];
}

export interface MonitoringGroupAnalysis {
  group: string;
  channel_count: number;
  video_count: number;
  total_views: number;
  dominant_topic: string;
  framing: string;
  bias_signal: string;
  notable_channels: { name: string; videos: number; stance: string }[];
}

export interface MonitoringAnalysis {
  headline: string;
  executive_summary: string;
  total_views: number;
  narrative_angles: MonitoringNarrative[];
  group_analysis: MonitoringGroupAnalysis[];
  key_claims_tracked: {
    claim: string;
    videos_making_claim: number;
    channels: string[];
    assessment: string;
  }[];
  sentiment_overview: {
    overall: string;
    pro_government_pct: number;
    critical_pct: number;
    neutral_analytical_pct: number;
    most_polarizing_topic: string;
  };
  trending_signals: {
    velocity: string;
    peak_period: string;
    prediction: string;
  };
  emerging_stories: { topic: string; early_signals: string; channels_covering: string[] }[];
  notable_absences: string;
  error?: string;
  raw?: string;
}

export interface MonitoringVideo {
  video_id: string;
  title: string;
  channel_id: string;
  channel_name: string;
  category: string;
  subscriber_count: number;
  published_at: string;
  thumbnail: string;
  view_count: number;
  like_count: number;
  comment_count: number;
  topic_classification: string;
  summary: string;
}

export interface MonitoringReport {
  id?: number;
  hours: number;
  generated_at: string;
  video_count: number;
  channel_count: number;
  videos: MonitoringVideo[];
  analysis: MonitoringAnalysis | null;
  gemini_cost_usd: number;
  youtube_units_used?: number;
  error?: string | null;
}

export interface MonitoringReportSummary {
  id: number;
  hours: number;
  generated_at: string;
  video_count: number;
  channel_count: number;
  gemini_cost_usd: number;
  has_error: boolean;
}

// ─── Narrative State types ──────────────────────────────────────────────────

export interface NarrativeStateInfo {
  active: boolean;
  id?: number;
  state_type?: string;
  created_at?: string;
  updated_at?: string;
  window_start?: string;
  window_end?: string;
  total_videos_processed?: number;
  total_channels?: number;
  incremental_updates?: number;
  narrative_count?: number;
  total_gemini_cost_usd?: number;
  total_haiku_cost_usd?: number;
  last_video_id?: number;
  is_active?: boolean;
  error?: string;
}

export interface BatchBuildResult {
  status?: string;
  state_id?: number;
  videos_processed?: number;
  narratives?: number;
  chunks?: number;
  gemini_cost_usd?: number;
  haiku_cost_usd?: number;
  error?: string;
}

// ─── API functions ───────────────────────────────────────────────────────────

async function postAPI<T>(path: string, body: Record<string, unknown> = {}): Promise<T> {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(body)) params.append(k, String(v));
  const resp = await fetch(`${API_URL}${path}?${params.toString()}`, { method: "POST" });
  if (!resp.ok) throw new Error(`API error: ${resp.status}`);
  return resp.json();
}

export const api = {
  // Reports
  getYouTubeReport: (topic: string, hours = 168) =>
    fetchAPI<YouTubeReport>(`/api/youtube/report/${encodeURIComponent(topic)}?hours=${hours}`),
  getYouTubeReports: (limit = 50, offset = 0) =>
    fetchAPI<YouTubeReportSummary[]>(`/api/youtube/reports?limit=${limit}&offset=${offset}`),
  getYouTubeReportById: (id: number) =>
    fetchAPI<YouTubeReport>(`/api/youtube/reports/${id}`),

  // Channels
  addChannel: (channelUrl: string, category?: string) =>
    postAPI<WatchedChannel & { error?: string; status?: string }>("/api/channels/add", { channel_url: channelUrl, ...(category ? { category } : {}) }),
  getChannels: (category?: string) =>
    fetchAPI<WatchedChannel[]>(`/api/channels${category ? `?category=${encodeURIComponent(category)}` : ""}`),
  getCategories: () =>
    fetchAPI<ChannelCategory[]>("/api/channels/categories"),
  removeChannel: (channelId: string) =>
    fetch(`${API_URL}/api/channels/${channelId}`, { method: "DELETE" }).then(r => r.json()),
  getChannelVideos: (channelId: string, limit = 50) =>
    fetchAPI<ChannelVideoItem[]>(`/api/channels/${channelId}/videos?limit=${limit}`),
  getActivityFeed: (limit = 50) =>
    fetchAPI<ChannelVideoItem[]>(`/api/channels/feed?limit=${limit}`),
  pollChannels: () =>
    postAPI<{ channels_checked: number; new_videos: number }>("/api/channels/poll"),

  // Monitoring Reports
  generateMonitoringReport: (hours = 24, formatPass = false) =>
    postAPI<MonitoringReport & { state_based?: boolean; state_id?: number }>("/api/monitoring/generate", { hours, format_pass: formatPass }),
  getMonitoringReports: (limit = 20) =>
    fetchAPI<MonitoringReportSummary[]>(`/api/monitoring/reports?limit=${limit}`),
  getMonitoringReportById: (id: number) =>
    fetchAPI<MonitoringReport>(`/api/monitoring/reports/${id}`),

  // Narrative State
  getNarrativeState: () =>
    fetchAPI<NarrativeStateInfo>("/api/monitoring/state"),
  getNarrativeStateById: (id: number) =>
    fetchAPI<NarrativeStateInfo & { state_json?: Record<string, unknown> }>(`/api/monitoring/state/${id}`),
  triggerBatchBuild: (hours = 24) =>
    postAPI<BatchBuildResult>("/api/monitoring/batch", { hours }),
};
