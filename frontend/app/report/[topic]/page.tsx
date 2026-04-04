"use client";

import { useEffect, useState } from "react";
import { useParams, useSearchParams } from "next/navigation";
import Link from "next/link";
import { api, type YouTubeReport, type NarrativeAngle } from "@/lib/api";
import { formatIST, formatNumber } from "@/lib/utils";
import {
  ArrowLeft, Play, Brain, AlertTriangle, TrendingUp, TrendingDown,
  Minus, Video, MessageSquare, Download, Clock, Eye, ThumbsUp,
  ChevronDown, ChevronUp, Search, CheckCircle, XCircle, HelpCircle,
  BarChart2, Users, Zap,
} from "lucide-react";

// ─── constants ────────────────────────────────────────────────────────────────

const SENT_BG: Record<string, string> = {
  positive: "bg-emerald-100 text-emerald-700",
  negative: "bg-red-100 text-red-700",
  neutral: "bg-gray-100 text-gray-600",
  mixed: "bg-amber-100 text-amber-700",
};

const BIAS_COLOR: Record<string, string> = {
  "pro-government": "bg-orange-100 text-orange-700",
  "pro-opposition": "bg-blue-100 text-blue-700",
  neutral: "bg-gray-100 text-gray-600",
  independent: "bg-green-100 text-green-700",
  sensationalist: "bg-red-100 text-red-700",
};

const ASSESS_ICON: Record<string, typeof CheckCircle> = {
  Verified: CheckCircle,
  Misleading: XCircle,
  "Partially True": HelpCircle,
  Unverified: HelpCircle,
};

const ASSESS_COLOR: Record<string, string> = {
  Verified: "text-emerald-600",
  Misleading: "text-red-600",
  "Partially True": "text-amber-600",
  Unverified: "text-gray-500",
};

// ─── Loading view ─────────────────────────────────────────────────────────────

const STEPS = [
  "Searching YouTube for videos...",
  "Fetching video details...",
  "Extracting transcripts...",
  "Analyzing comments...",
  "Running Gemini analysis...",
];

function LoadingView({ topic }: { topic: string }) {
  const [step, setStep] = useState(0);
  useEffect(() => {
    const timers = [2000, 5000, 10000, 18000].map((ms, i) =>
      setTimeout(() => setStep(i + 1), ms)
    );
    return () => timers.forEach(clearTimeout);
  }, []);
  return (
    <div className="flex flex-col items-center justify-center min-h-[60vh] gap-6">
      <div className="relative">
        <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-red-500 to-red-700 flex items-center justify-center shadow-lg">
          <Play className="w-8 h-8 text-white" />
        </div>
        <div className="absolute -inset-1 rounded-2xl bg-red-400/20 animate-ping" />
      </div>
      <div className="text-center">
        <h2 className="text-lg font-semibold text-gray-800 mb-1">Analyzing &ldquo;{topic}&rdquo;</h2>
        <p className="text-sm text-gray-400">This takes 30–60 seconds</p>
      </div>
      <div className="w-full max-w-xs space-y-2">
        {STEPS.map((s, i) => (
          <div key={s} className={`flex items-center gap-2.5 text-sm transition-all duration-500 ${i <= step ? "opacity-100" : "opacity-25"}`}>
            <div className={`w-5 h-5 rounded-full flex items-center justify-center shrink-0 text-xs ${
              i < step ? "bg-red-500 text-white" : i === step ? "bg-red-100 border-2 border-red-400" : "bg-gray-100 border border-gray-200"
            }`}>
              {i < step ? "✓" : <span className={i === step ? "text-red-500 font-bold" : "text-gray-400"}>{i + 1}</span>}
            </div>
            <span className={i === step ? "text-red-700 font-medium" : i < step ? "text-gray-400 line-through" : "text-gray-400"}>{s}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── subcomponents ────────────────────────────────────────────────────────────

function SentimentBar({ label, pct, color }: { label: string; pct: number; color: string }) {
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-gray-600">
        <span className="capitalize">{label}</span><span className="font-semibold">{pct}%</span>
      </div>
      <div className="h-2.5 bg-gray-100 rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-700 ${color}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function VideoCard({ v }: { v: NarrativeAngle["top_videos"][number] }) {
  return (
    <a
      href={`https://youtube.com/watch?v=${v.video_id}`}
      target="_blank"
      rel="noopener noreferrer"
      className="flex gap-3 bg-white border border-gray-100 rounded-lg p-3 hover:border-gray-300 transition-colors"
    >
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        src={`https://img.youtube.com/vi/${v.video_id}/mqdefault.jpg`}
        alt=""
        className="w-32 h-[72px] object-cover rounded shrink-0"
      />
      <div className="min-w-0 flex-1">
        <p className="text-xs font-semibold text-gray-800 line-clamp-2">{v.title}</p>
        <p className="text-[10px] text-gray-500 mt-0.5">{v.channel} · {formatNumber(v.views)} views</p>
        <p className="text-[10px] text-indigo-600 mt-1">{v.why}</p>
      </div>
    </a>
  );
}

function NarrativeCard({ angle, idx }: { angle: NarrativeAngle; idx: number }) {
  const [open, setOpen] = useState(idx === 0);
  const sentBg = SENT_BG[angle.sentiment] || SENT_BG.neutral;

  return (
    <div className="border border-gray-200 rounded-xl overflow-hidden bg-white">
      <button
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-start justify-between gap-3 p-4 text-left hover:bg-gray-50 transition-colors"
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-1">
            <span className="text-xs font-bold text-gray-400">#{idx + 1}</span>
            <span className="text-sm font-semibold text-gray-800">{angle.title}</span>
            <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium capitalize ${sentBg}`}>{angle.sentiment}</span>
            <span className="text-[10px] text-gray-400">{angle.video_count} videos</span>
          </div>
          {!open && <p className="text-xs text-gray-500 truncate">{angle.description}</p>}
        </div>
        {open ? <ChevronUp className="w-4 h-4 text-gray-400 shrink-0 mt-0.5" /> : <ChevronDown className="w-4 h-4 text-gray-400 shrink-0 mt-0.5" />}
      </button>
      {open && (
        <div className="px-4 pb-4 space-y-3">
          <p className="text-sm text-gray-600 leading-relaxed">{angle.description}</p>
          {angle.key_claims?.length > 0 && (
            <div className="space-y-1">
              <div className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Key Claims</div>
              <ul className="text-xs text-gray-600 space-y-1">
                {angle.key_claims.map((c, i) => (
                  <li key={i} className="flex items-start gap-1.5">
                    <span className="text-red-400 mt-0.5">-</span> {c}
                  </li>
                ))}
              </ul>
            </div>
          )}
          {angle.top_videos?.length > 0 && (
            <div className="space-y-2">
              <div className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Top Videos</div>
              {angle.top_videos.map((v, i) => (
                <VideoCard key={i} v={v} />
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Print styles ─────────────────────────────────────────────────────────────

const PRINT_STYLES = `
@media print {
  html, body { height: auto !important; overflow: visible !important; background: white !important; margin: 0 !important; }
  body > div, body > div > div { height: auto !important; overflow: visible !important; display: block !important; position: static !important; }
  * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
  button, .no-print { display: none !important; }
  a { color: inherit !important; text-decoration: none !important; }
  .bg-white.border { break-inside: avoid; page-break-inside: avoid; }
}
`;

// ─── Main page ────────────────────────────────────────────────────────────────

export default function YouTubeReportPage() {
  const params = useParams();
  const searchParams = useSearchParams();
  const topic = decodeURIComponent(params.topic as string);
  const hours = Number(searchParams.get("hours")) || 168;

  const [data, setData] = useState<YouTubeReport | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    setData(null);
    const reportId = searchParams.get("id");
    const promise = reportId
      ? api.getYouTubeReportById(Number(reportId))
      : api.getYouTubeReport(topic, hours);
    promise.then(setData).catch(() => {}).finally(() => setLoading(false));
  }, [topic, hours, searchParams]);

  if (loading) return <LoadingView topic={topic} />;

  const aiBroken = !data?.analysis || (!data.analysis.executive_summary && data.analysis.error);

  if (!data || aiBroken) {
    return (
      <div className="space-y-4 max-w-2xl">
        <style dangerouslySetInnerHTML={{ __html: PRINT_STYLES }} />
        <Link href="/" className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-900">
          <ArrowLeft className="w-4 h-4" /> Back
        </Link>
        <div className="bg-red-50 border border-red-200 rounded-xl p-6 text-center">
          <AlertTriangle className="w-8 h-8 mx-auto mb-2 text-red-400" />
          <p className="font-medium text-red-700">{data?.error || data?.analysis?.error || "No data found."}</p>
        </div>
      </div>
    );
  }

  const ai = data.analysis;
  const totalViews = ai?.total_views || data.videos.reduce((s, v) => s + v.view_count, 0);

  return (
    <div className="space-y-5">
      <style dangerouslySetInnerHTML={{ __html: PRINT_STYLES }} />

      {/* Back + PDF */}
      <div className="flex items-center gap-3 no-print">
        <Link href="/" className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-900">
          <ArrowLeft className="w-4 h-4" /> Back
        </Link>
        <button
          onClick={() => setTimeout(() => window.print(), 200)}
          className="ml-auto flex items-center gap-1.5 px-3 py-2 bg-white border border-gray-200 hover:border-gray-300 rounded-lg text-xs text-gray-600 hover:text-gray-900"
        >
          <Download className="w-3.5 h-3.5" /> Download PDF
        </button>
      </div>

      {/* Hero */}
      <div className="bg-gradient-to-r from-red-600 to-red-800 rounded-2xl p-6 text-white">
        <div className="flex items-center gap-2 mb-1">
          <Play className="w-5 h-5 text-red-200" />
          <span className="text-xs text-red-200 font-medium uppercase tracking-wide">YouTube Intel · Narrative Report</span>
        </div>
        <h1 className="text-2xl font-bold mb-1">&ldquo;{topic}&rdquo;</h1>
        <div className="flex items-center gap-3 text-xs text-red-200 flex-wrap">
          <span>{data.video_count} videos · {formatNumber(totalViews)} total views</span>
          <span>·</span>
          <span>Last {hours}h</span>
          <span>·</span>
          <span>${data.gemini_cost_usd.toFixed(4)} · {data.youtube_units_used} YT units</span>
          <span>·</span>
          <span><Clock className="w-3 h-3 inline mr-0.5" />{formatIST(data.generated_at)} IST</span>
        </div>
      </div>

      {/* Executive Summary */}
      {ai?.executive_summary && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-3">
            <Brain className="w-4 h-4 text-red-500" /> Executive Summary
          </h2>
          <p className="text-sm text-gray-700 leading-relaxed">{ai!.executive_summary}</p>
        </div>
      )}

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Video className="w-4 h-4 text-red-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{data.video_count}</div>
          <div className="text-xs text-gray-500">Videos</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Eye className="w-4 h-4 text-blue-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{formatNumber(totalViews)}</div>
          <div className="text-xs text-gray-500">Total Views</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <BarChart2 className="w-4 h-4 text-purple-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{ai?.narrative_angles?.length || 0}</div>
          <div className="text-xs text-gray-500">Narrative Angles</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Users className="w-4 h-4 text-green-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{ai?.channel_analysis?.length || 0}</div>
          <div className="text-xs text-gray-500">Channels</div>
        </div>
      </div>

      {/* Narrative Angles */}
      {(ai?.narrative_angles?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <BarChart2 className="w-4 h-4 text-red-500" /> Narrative Angles
          </h2>
          <div className="space-y-2">
            {ai!.narrative_angles.map((angle, i) => (
              <NarrativeCard key={i} angle={angle} idx={i} />
            ))}
          </div>
        </div>
      )}

      {/* Comment Sentiment + Trending Signals */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        {ai?.comment_sentiment && (
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
              <MessageSquare className="w-4 h-4 text-blue-500" /> Comment Sentiment
            </h2>
            <div className="space-y-3 mb-4">
              <SentimentBar label="Positive" pct={ai!.comment_sentiment.positive_pct} color="bg-emerald-500" />
              <SentimentBar label="Negative" pct={ai!.comment_sentiment.negative_pct} color="bg-red-500" />
              <SentimentBar label="Neutral" pct={ai!.comment_sentiment.neutral_pct} color="bg-gray-400" />
            </div>
            {ai!.comment_sentiment.top_themes?.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {ai!.comment_sentiment.top_themes.map((t) => (
                  <span key={t} className="text-[11px] px-2 py-0.5 bg-gray-100 text-gray-600 rounded-full">{t}</span>
                ))}
              </div>
            )}
          </div>
        )}
        {ai?.trending_signals && (
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
              <Zap className="w-4 h-4 text-amber-500" /> Trending Signals
            </h2>
            <div className="space-y-3">
              <div className={`rounded-xl border p-3 ${
                ai.trending_signals.velocity === "rising" ? "bg-emerald-50 border-emerald-200" :
                ai.trending_signals.velocity === "declining" ? "bg-gray-50 border-gray-200" : "bg-blue-50 border-blue-200"
              }`}>
                <div className="flex items-center gap-2 mb-1">
                  {ai!.trending_signals.velocity === "rising" ? <TrendingUp className="w-4 h-4 text-emerald-600" /> :
                   ai.trending_signals.velocity === "declining" ? <TrendingDown className="w-4 h-4 text-gray-500" /> :
                   <Minus className="w-4 h-4 text-blue-600" />}
                  <span className="text-sm font-semibold capitalize">{ai!.trending_signals.velocity}</span>
                </div>
                <p className="text-xs text-gray-600">Peak: {ai!.trending_signals.peak_period}</p>
              </div>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                <div className="text-[10px] text-blue-500 font-semibold uppercase tracking-wide mb-1">Prediction</div>
                <p className="text-xs text-gray-700">{ai!.trending_signals.prediction}</p>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Channel Analysis */}
      {(ai?.channel_analysis?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Users className="w-4 h-4 text-green-500" /> Channel Analysis
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-[10px] text-gray-400 uppercase border-b border-gray-100">
                  <th className="text-left py-2 px-2">Channel</th>
                  <th className="text-right py-2 px-2">Subscribers</th>
                  <th className="text-right py-2 px-2">Videos</th>
                  <th className="text-left py-2 px-2">Bias</th>
                  <th className="text-left py-2 px-2">Influence</th>
                </tr>
              </thead>
              <tbody>
                {ai!.channel_analysis.map((ch, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                    <td className="py-2 px-2">
                      <a
                        href={`https://youtube.com/channel/${ch.channel_id}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-xs font-medium text-blue-600 hover:underline"
                      >
                        {ch.channel_name}
                      </a>
                    </td>
                    <td className="py-2 px-2 text-right text-xs text-gray-500">{formatNumber(ch.subscriber_count)}</td>
                    <td className="py-2 px-2 text-right text-xs text-gray-500">{ch.videos_on_topic}</td>
                    <td className="py-2 px-2">
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium capitalize ${BIAS_COLOR[ch.bias] || "bg-gray-100 text-gray-600"}`}>
                        {ch.bias}
                      </span>
                    </td>
                    <td className="py-2 px-2">
                      <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium capitalize ${
                        ch.influence_score === "high" ? "bg-red-100 text-red-700" :
                        ch.influence_score === "medium" ? "bg-amber-100 text-amber-700" : "bg-gray-100 text-gray-600"
                      }`}>
                        {ch.influence_score}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Key Claims */}
      {(ai?.key_claims_tracked?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Search className="w-4 h-4 text-purple-500" /> Key Claims Tracked
          </h2>
          <div className="space-y-2">
            {ai!.key_claims_tracked.map((claim, i) => {
              const AssessIcon = ASSESS_ICON[claim.assessment] || HelpCircle;
              const assessColor = ASSESS_COLOR[claim.assessment] || "text-gray-500";
              return (
                <div key={i} className="flex items-start gap-3 bg-gray-50 rounded-lg p-3">
                  <AssessIcon className={`w-4 h-4 shrink-0 mt-0.5 ${assessColor}`} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-700">{claim.claim}</p>
                    <div className="flex items-center gap-2 mt-1">
                      <span className="text-[10px] text-gray-400">{claim.videos_making_claim} video{claim.videos_making_claim !== 1 ? "s" : ""}</span>
                      <span className={`text-[10px] font-medium ${assessColor}`}>{claim.assessment}</span>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Related Topics */}
      {(ai?.related_topics?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-3">
            <Search className="w-4 h-4 text-gray-400" /> Related Topics
          </h2>
          <div className="flex flex-wrap gap-2">
            {ai!.related_topics.map((t) => (
              <Link
                key={t}
                href={`/report/${encodeURIComponent(t)}?hours=${hours}`}
                className="text-xs px-3 py-1.5 bg-gray-50 hover:bg-red-50 hover:text-red-700 border border-gray-200 hover:border-red-200 rounded-full text-gray-600 transition-colors"
              >
                {t}
              </Link>
            ))}
          </div>
        </div>
      )}

      {/* All Videos */}
      {data.videos?.length > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Video className="w-4 h-4 text-red-500" /> All Videos ({data.videos.length})
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            {data.videos.slice(0, 20).map((v) => (
              <a
                key={v.video_id}
                href={`https://youtube.com/watch?v=${v.video_id}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex gap-3 border border-gray-100 rounded-lg p-3 hover:border-gray-300 transition-colors"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                  src={v.thumbnail || `https://img.youtube.com/vi/${v.video_id}/mqdefault.jpg`}
                  alt=""
                  className="w-36 aspect-video object-cover rounded shrink-0"
                />
                <div className="min-w-0 flex-1">
                  <p className="text-xs font-semibold text-gray-800 line-clamp-2">{v.title}</p>
                  <p className="text-[10px] text-gray-500 mt-0.5">{v.channel_title}</p>
                  <div className="flex items-center gap-2 mt-1.5 text-[10px] text-gray-400">
                    <span className="flex items-center gap-0.5"><Eye className="w-3 h-3" />{formatNumber(v.view_count)}</span>
                    <span className="flex items-center gap-0.5"><ThumbsUp className="w-3 h-3" />{formatNumber(v.like_count)}</span>
                    <span className="flex items-center gap-0.5"><MessageSquare className="w-3 h-3" />{formatNumber(v.comment_count)}</span>
                    {v.has_transcript && <span className="text-emerald-500">CC</span>}
                  </div>
                </div>
              </a>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
