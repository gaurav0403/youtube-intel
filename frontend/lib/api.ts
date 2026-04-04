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

// ─── API functions ───────────────────────────────────────────────────────────

export const api = {
  getYouTubeReport: (topic: string, hours = 168) =>
    fetchAPI<YouTubeReport>(`/api/youtube/report/${encodeURIComponent(topic)}?hours=${hours}`),
  getYouTubeReports: (limit = 50, offset = 0) =>
    fetchAPI<YouTubeReportSummary[]>(`/api/youtube/reports?limit=${limit}&offset=${offset}`),
  getYouTubeReportById: (id: number) =>
    fetchAPI<YouTubeReport>(`/api/youtube/reports/${id}`),
};
