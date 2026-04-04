"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import {
  api,
  type MonitoringReport,
  type MonitoringReportSummary,
  type MonitoringVideo,
  type MonitoringNarrative,
  type MonitoringGroupAnalysis,
  type FramingDivergence,
  type NarrativeStateInfo,
} from "@/lib/api";
import { formatNumber, formatIST } from "@/lib/utils";
import {
  ArrowLeft, Loader2, Zap, Clock, AlertTriangle, TrendingUp, TrendingDown,
  Minus, Eye, ThumbsUp, MessageSquare, ExternalLink, ChevronDown, ChevronUp,
  BarChart2, Users, Flame, Search, Download, Brain, Play, Video,
  CheckCircle, XCircle, HelpCircle, Network, GitCompareArrows,
} from "lucide-react";

const GROUP_ORDER = [
  "Mainstream Media",
  "Independent & Digital",
  "Regional",
  "Specialist & Policy",
] as const;

// ─── constants ───────────────────────────────────────────────────────────────

const HOUR_OPTIONS = [
  { label: "24h", value: 24 },
  { label: "48h", value: 48 },
  { label: "72h", value: 72 },
];

const SENT_BG: Record<string, string> = {
  positive: "bg-emerald-100 text-emerald-700",
  negative: "bg-red-100 text-red-700",
  neutral: "bg-gray-100 text-gray-600",
  mixed: "bg-amber-100 text-amber-700",
};

const BIAS_COLOR: Record<string, string> = {
  "pro-government": "bg-orange-100 text-orange-700",
  critical: "bg-blue-100 text-blue-700",
  neutral: "bg-gray-100 text-gray-600",
  mixed: "bg-amber-100 text-amber-700",
};

const ASSESS_ICON: Record<string, typeof CheckCircle> = {
  Verified: CheckCircle,
  Misleading: XCircle,
  "Partially True": HelpCircle,
  Unverified: HelpCircle,
  Contested: HelpCircle,
};

const ASSESS_COLOR: Record<string, string> = {
  Verified: "text-emerald-600",
  Misleading: "text-red-600",
  "Partially True": "text-amber-600",
  Unverified: "text-gray-500",
  Contested: "text-purple-600",
};

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

// ─── Loading View ────────────────────────────────────────────────────────────

const STEPS_LEGACY = [
  "Pulling videos from 100+ channels...",
  "Fetching view counts & engagement...",
  "Extracting transcripts...",
  "Running Gemini narrative analysis...",
];

const STEPS_STATE = [
  "Checking narrative state...",
  "Formatting report from state...",
  "Generating polished analysis...",
];

function LoadingView({ hasState }: { hasState: boolean }) {
  const [step, setStep] = useState(0);
  const steps = hasState ? STEPS_STATE : STEPS_LEGACY;
  const timings = hasState ? [1000, 3000] : [3000, 8000, 15000];
  useEffect(() => {
    const timers = timings.map((ms, i) =>
      setTimeout(() => setStep(i + 1), ms)
    );
    return () => timers.forEach(clearTimeout);
  }, []);
  return (
    <div className="flex flex-col items-center justify-center py-16 gap-6">
      <div className="relative">
        <div className="w-14 h-14 rounded-2xl bg-gradient-to-br from-red-500 to-red-700 flex items-center justify-center shadow-lg">
          <Play className="w-7 h-7 text-white" />
        </div>
        <div className="absolute -inset-1 rounded-2xl bg-red-400/20 animate-ping" />
      </div>
      <div className="w-full max-w-xs space-y-2">
        {steps.map((s, i) => (
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
      <p className="text-xs text-gray-400">{hasState ? "This takes 5–10 seconds" : "This takes 30–60 seconds"}</p>
    </div>
  );
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

function SentimentBar({ label, pct, color }: { label: string; pct: number; color: string }) {
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-xs text-gray-600">
        <span>{label}</span><span className="font-semibold">{pct}%</span>
      </div>
      <div className="h-2.5 bg-gray-100 rounded-full overflow-hidden">
        <div className={`h-full rounded-full transition-all duration-700 ${color}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}

function NarrativeCard({ angle, idx }: { angle: MonitoringNarrative; idx: number }) {
  const [open, setOpen] = useState(idx === 0);
  const [showAllClaims, setShowAllClaims] = useState(false);
  const sentBg = SENT_BG[angle.sentiment] || SENT_BG.neutral;
  const CLAIMS_COLLAPSED = 5;
  const claims = angle.key_claims || [];
  const visibleClaims = showAllClaims ? claims : claims.slice(0, CLAIMS_COLLAPSED);
  const hasMoreClaims = claims.length > CLAIMS_COLLAPSED;
  const groupCount = angle.group_count ?? angle.categories_involved?.length ?? 0;
  const groupChipColor =
    groupCount >= 4
      ? "bg-emerald-100 text-emerald-700"
      : groupCount >= 2
      ? "bg-blue-100 text-blue-700"
      : groupCount === 1
      ? "bg-amber-100 text-amber-700"
      : "bg-gray-100 text-gray-500";

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
            {groupCount > 0 && (
              <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium flex items-center gap-0.5 ${groupChipColor}`}>
                <Network className="w-2.5 h-2.5" /> {groupCount}/4 groups
              </span>
            )}
            <span className="text-[10px] text-gray-400">{angle.video_count} videos · {formatNumber(angle.total_views || 0)} views</span>
          </div>
          {!open && <p className="text-xs text-gray-500 truncate">{angle.description}</p>}
        </div>
        {open ? <ChevronUp className="w-4 h-4 text-gray-400 shrink-0 mt-0.5" /> : <ChevronDown className="w-4 h-4 text-gray-400 shrink-0 mt-0.5" />}
      </button>
      {open && (
        <div className="px-4 pb-4 space-y-3">
          <p className="text-sm text-gray-600 leading-relaxed">{angle.description}</p>

          {/* Channels */}
          {angle.channels_pushing?.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {angle.channels_pushing.map((ch) => (
                <span key={ch} className="text-[10px] px-2 py-0.5 bg-red-50 text-red-600 rounded-full">{ch}</span>
              ))}
            </div>
          )}

          {/* Key Claims */}
          {claims.length > 0 && (
            <div className="space-y-1">
              <div className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">
                Key Claims {claims.length > 0 && <span className="text-gray-400 font-normal normal-case">({claims.length})</span>}
              </div>
              <ul className="text-xs text-gray-600 space-y-1">
                {visibleClaims.map((c, i) => (
                  <li key={i} className="flex items-start gap-1.5"><span className="text-red-400 mt-0.5">-</span> {c}</li>
                ))}
              </ul>
              {hasMoreClaims && (
                <button
                  onClick={() => setShowAllClaims((v) => !v)}
                  className="text-[11px] text-red-600 hover:text-red-700 font-medium mt-1 no-print"
                >
                  {showAllClaims ? "Show less" : `Show all ${claims.length} claims`}
                </button>
              )}
            </div>
          )}

          {/* Top Videos */}
          {angle.top_videos?.length > 0 && (
            <div className="space-y-2">
              <div className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">Top Videos</div>
              {angle.top_videos.map((v) => (
                <a
                  key={v.video_id}
                  href={`https://youtube.com/watch?v=${v.video_id}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex gap-3 bg-gray-50 border border-gray-100 rounded-lg p-3 hover:border-gray-300 transition-colors"
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
                    {v.why && <p className="text-[10px] text-indigo-600 mt-1">{v.why}</p>}
                  </div>
                </a>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function GroupPulseCard({ group }: { group: MonitoringGroupAnalysis }) {
  const biasBg = BIAS_COLOR[group.bias_signal] || BIAS_COLOR.neutral;
  const notable = (group.notable_channels || []).slice(0, 3);
  return (
    <div className="border border-gray-200 rounded-xl p-4 bg-white flex flex-col gap-2.5">
      <div className="flex items-start justify-between gap-2">
        <h3 className="text-sm font-semibold text-gray-800 leading-tight">{group.group}</h3>
        <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium capitalize shrink-0 ${biasBg}`}>
          {group.bias_signal}
        </span>
      </div>
      <div className="text-[11px] text-gray-500">
        {group.video_count} videos · {formatNumber(group.total_views || 0)} views · {group.channel_count} channels
      </div>
      {group.dominant_topic && (
        <div>
          <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wide mb-0.5">Dominant</div>
          <p className="text-xs text-gray-700 leading-snug line-clamp-2">{group.dominant_topic}</p>
        </div>
      )}
      {group.framing && (
        <p className="text-[11px] text-gray-500 leading-snug line-clamp-3">{group.framing}</p>
      )}
      {notable.length > 0 && (
        <div className="pt-1 border-t border-gray-100">
          <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wide mb-1">Top channels</div>
          <div className="space-y-0.5">
            {notable.map((ch) => (
              <div key={ch.name} className="flex items-center gap-1.5 text-[11px]">
                <span className="font-medium text-gray-700 truncate">{ch.name}</span>
                <span className="text-gray-400 shrink-0">({ch.videos})</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function FramingDivergenceSection({ data }: { data: FramingDivergence }) {
  const [showSilo, setShowSilo] = useState(false);
  const universalCount = data.universal?.length ?? 0;
  const majorityCount = data.majority?.length ?? 0;
  const siloCount = data.silo?.length ?? 0;
  const topDivergent = data.top_divergent || [];

  // If nothing to show, hide entire section
  if (!topDivergent.length && !universalCount && !majorityCount && !siloCount) {
    return null;
  }

  return (
    <div className="bg-white border border-gray-200 rounded-2xl p-5">
      <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
        <GitCompareArrows className="w-4 h-4 text-indigo-500" /> Framing Divergence
      </h2>

      {/* Convergence counts */}
      <div className="flex flex-wrap gap-2 mb-4">
        <div className="text-[11px] px-2.5 py-1 bg-emerald-50 border border-emerald-200 rounded-full">
          <span className="font-semibold text-emerald-700">{universalCount}</span>
          <span className="text-emerald-600"> universal</span>
        </div>
        <div className="text-[11px] px-2.5 py-1 bg-blue-50 border border-blue-200 rounded-full">
          <span className="font-semibold text-blue-700">{majorityCount}</span>
          <span className="text-blue-600"> majority</span>
        </div>
        <div className="text-[11px] px-2.5 py-1 bg-amber-50 border border-amber-200 rounded-full">
          <span className="font-semibold text-amber-700">{siloCount}</span>
          <span className="text-amber-600"> silo</span>
        </div>
      </div>

      {/* Top divergent narratives */}
      {topDivergent.length > 0 && (
        <div className="space-y-3">
          <div className="text-[10px] font-semibold text-gray-500 uppercase tracking-wide">
            Top Divergent Stories — same story, different framing
          </div>
          {topDivergent.map((d, i) => (
            <div key={i} className="border border-gray-200 rounded-xl p-3 bg-gray-50">
              <div className="flex items-start justify-between gap-2 mb-2">
                <p className="text-sm font-semibold text-gray-800">{d.title}</p>
                <span className="text-[10px] text-gray-400 shrink-0">{formatNumber(d.total_views || 0)} views</span>
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                {GROUP_ORDER.map((g) => {
                  const cell = d.groups?.[g];
                  if (!cell) {
                    return (
                      <div key={g} className="rounded-lg bg-white border border-dashed border-gray-200 p-2">
                        <div className="text-[10px] text-gray-400 truncate">{g}</div>
                        <div className="text-[11px] text-gray-300 mt-0.5">—</div>
                      </div>
                    );
                  }
                  const biasBg = BIAS_COLOR[cell.bias] || BIAS_COLOR.neutral;
                  return (
                    <div key={g} className="rounded-lg bg-white border border-gray-200 p-2">
                      <div className="text-[10px] text-gray-500 truncate">{g}</div>
                      <div className="flex items-center gap-1 mt-0.5 flex-wrap">
                        <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium capitalize ${biasBg}`}>
                          {cell.bias}
                        </span>
                        <span className="text-[10px] text-gray-400">{cell.videos}v</span>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Silo stories expander */}
      {siloCount > 0 && (
        <div className="mt-4 pt-3 border-t border-gray-100">
          <button
            onClick={() => setShowSilo((v) => !v)}
            className="text-[11px] text-indigo-600 hover:text-indigo-700 font-medium no-print flex items-center gap-1"
          >
            {showSilo ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
            {showSilo ? "Hide" : "Show"} silo stories ({siloCount})
          </button>
          {showSilo && (
            <ul className="mt-2 space-y-1">
              {(data.silo || []).map((s, i) => (
                <li key={i} className="text-xs text-gray-600 flex items-center gap-2">
                  <span className="w-1 h-1 rounded-full bg-amber-400 shrink-0" />
                  <span className="truncate flex-1">{s.title}</span>
                  <span className="text-[10px] text-gray-400 shrink-0">{formatNumber(s.total_views || 0)}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Videos Table ────────────────────────────────────────────────────────────

type SortKey = "views" | "likes" | "comments" | "recent";

function VideosTable({ videos }: { videos: MonitoringVideo[] }) {
  const [sort, setSort] = useState<SortKey>("views");
  const [showAll, setShowAll] = useState(false);

  const sorted = [...videos].sort((a, b) => {
    if (sort === "views") return b.view_count - a.view_count;
    if (sort === "likes") return b.like_count - a.like_count;
    if (sort === "comments") return b.comment_count - a.comment_count;
    return new Date(b.published_at).getTime() - new Date(a.published_at).getTime();
  });

  const displayed = showAll ? sorted : sorted.slice(0, 20);

  return (
    <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
      <div className="px-5 py-4 border-b border-gray-100 flex items-center justify-between">
        <h2 className="font-semibold text-gray-800 flex items-center gap-2">
          <Video className="w-4 h-4 text-red-500" /> All Videos ({videos.length})
        </h2>
        <div className="flex gap-1 bg-gray-100 rounded-lg p-0.5 no-print">
          {([
            { key: "views" as SortKey, label: "Views", icon: Eye },
            { key: "likes" as SortKey, label: "Likes", icon: ThumbsUp },
            { key: "comments" as SortKey, label: "Comments", icon: MessageSquare },
            { key: "recent" as SortKey, label: "Recent", icon: Clock },
          ]).map(({ key, label, icon: Icon }) => (
            <button
              key={key}
              onClick={() => setSort(key)}
              className={`flex items-center gap-1 px-2.5 py-1.5 rounded-md text-[10px] font-medium transition-colors ${
                sort === key ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
              }`}
            >
              <Icon className="w-3 h-3" /> {label}
            </button>
          ))}
        </div>
      </div>
      <div className="divide-y divide-gray-50">
        {displayed.map((v, i) => (
          <a
            key={v.video_id}
            href={`https://youtube.com/watch?v=${v.video_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-3 px-5 py-3 hover:bg-gray-50 transition-colors"
          >
            <span className="text-xs text-gray-300 w-5 text-right shrink-0">{i + 1}</span>
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src={v.thumbnail || `https://img.youtube.com/vi/${v.video_id}/mqdefault.jpg`}
              alt=""
              className="w-24 h-[54px] object-cover rounded shrink-0"
            />
            <div className="flex-1 min-w-0">
              <p className="text-xs font-medium text-gray-800 line-clamp-1">{v.title}</p>
              <div className="flex items-center gap-2 mt-0.5">
                <span className="text-[10px] text-gray-500">{v.channel_name}</span>
                <span className="text-[10px] px-1.5 py-0.5 bg-gray-100 text-gray-500 rounded">{v.category}</span>
              </div>
            </div>
            <div className="flex items-center gap-4 shrink-0 text-[10px] text-gray-400">
              <span className="flex items-center gap-0.5 w-16 justify-end"><Eye className="w-3 h-3" />{formatNumber(v.view_count)}</span>
              <span className="flex items-center gap-0.5 w-12 justify-end"><ThumbsUp className="w-3 h-3" />{formatNumber(v.like_count)}</span>
              <span className="flex items-center gap-0.5 w-12 justify-end"><MessageSquare className="w-3 h-3" />{formatNumber(v.comment_count)}</span>
              <ExternalLink className="w-3 h-3 text-gray-300" />
            </div>
          </a>
        ))}
      </div>
      {videos.length > 20 && !showAll && (
        <button
          onClick={() => setShowAll(true)}
          className="w-full py-3 text-xs text-red-600 hover:text-red-700 font-medium border-t border-gray-100 no-print"
        >
          Show all {videos.length} videos
        </button>
      )}
    </div>
  );
}

// ─── Report View ─────────────────────────────────────────────────────────────

function ReportView({ report }: { report: MonitoringReport }) {
  const ai = report.analysis!;
  const totalViews = ai.total_views || report.videos.reduce((s, v) => s + v.view_count, 0);

  return (
    <div className="space-y-5">
      <style dangerouslySetInnerHTML={{ __html: PRINT_STYLES }} />

      {/* PDF button */}
      <div className="flex justify-end no-print">
        <button
          onClick={() => setTimeout(() => window.print(), 200)}
          className="flex items-center gap-1.5 px-3 py-2 bg-white border border-gray-200 hover:border-gray-300 rounded-lg text-xs text-gray-600 hover:text-gray-900"
        >
          <Download className="w-3.5 h-3.5" /> Download PDF
        </button>
      </div>

      {/* Hero */}
      <div className="bg-gradient-to-r from-red-600 to-red-800 rounded-2xl p-6 text-white">
        <div className="flex items-center gap-2 mb-1">
          <Play className="w-5 h-5 text-red-200" />
          <span className="text-xs text-red-200 font-medium uppercase tracking-wide">Channel Monitoring · Narrative Report</span>
        </div>
        <h1 className="text-xl font-bold mb-1">{ai.headline}</h1>
        <div className="flex items-center gap-3 text-xs text-red-200 flex-wrap">
          <span>{report.video_count} videos · {formatNumber(totalViews)} views · {report.channel_count} channels</span>
          <span>·</span>
          <span>Last {report.hours}h</span>
          <span>·</span>
          <span>${report.gemini_cost_usd.toFixed(4)}</span>
          <span>·</span>
          <span><Clock className="w-3 h-3 inline mr-0.5" />{formatIST(report.generated_at)} IST</span>
        </div>
      </div>

      {/* Executive Brief — prose + key judgments */}
      <div className="bg-white border border-gray-200 rounded-2xl p-5">
        <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-3">
          <Brain className="w-4 h-4 text-red-500" /> Executive Brief
        </h2>
        <p className="text-sm text-gray-700 leading-relaxed">{ai.executive_summary}</p>
        {(ai.key_judgments?.length ?? 0) > 0 && (
          <ul className="mt-3 space-y-1.5">
            {ai.key_judgments!.map((j, i) => (
              <li key={i} className="flex gap-2 text-sm">
                <span className="text-red-500 font-bold shrink-0">▸</span>
                <span className="text-gray-700">{j}</span>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Video className="w-4 h-4 text-red-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{report.video_count}</div>
          <div className="text-xs text-gray-500">Videos</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Eye className="w-4 h-4 text-blue-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{formatNumber(totalViews)}</div>
          <div className="text-xs text-gray-500">Total Views</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <BarChart2 className="w-4 h-4 text-purple-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{ai.narrative_angles?.length || 0}</div>
          <div className="text-xs text-gray-500">Narratives</div>
        </div>
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-center">
          <Users className="w-4 h-4 text-green-500 mx-auto mb-1" />
          <div className="text-xl font-bold text-gray-800">{report.channel_count}</div>
          <div className="text-xs text-gray-500">Channels</div>
        </div>
      </div>

      {/* Group Pulse — what each group is doing (4-col grid) */}
      {(ai.group_analysis?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Users className="w-4 h-4 text-green-500" /> Group Pulse
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
            {ai.group_analysis.map((g, i) => (
              <GroupPulseCard key={i} group={g} />
            ))}
          </div>
        </div>
      )}

      {/* Framing Divergence — how the 4 groups diverge on the same stories */}
      {ai.framing_divergence && (
        <FramingDivergenceSection data={ai.framing_divergence} />
      )}

      {/* Sentiment + Trending side by side */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
        {ai.sentiment_overview && (
          <div className="bg-white border border-gray-200 rounded-2xl p-5">
            <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
              <BarChart2 className="w-4 h-4 text-blue-500" /> Sentiment Breakdown
            </h2>
            <div className="space-y-3 mb-3">
              <SentimentBar label="Pro-Government" pct={ai.sentiment_overview.pro_government_pct} color="bg-orange-500" />
              <SentimentBar label="Critical" pct={ai.sentiment_overview.critical_pct} color="bg-blue-500" />
              <SentimentBar label="Neutral / Analytical" pct={ai.sentiment_overview.neutral_analytical_pct} color="bg-gray-400" />
            </div>
            {ai.sentiment_overview.most_polarizing_topic && (
              <p className="text-xs text-gray-500">Most polarizing: <span className="font-medium text-gray-700">{ai.sentiment_overview.most_polarizing_topic}</span></p>
            )}
          </div>
        )}
        {ai.trending_signals && (
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
                  {ai.trending_signals.velocity === "rising" ? <TrendingUp className="w-4 h-4 text-emerald-600" /> :
                   ai.trending_signals.velocity === "declining" ? <TrendingDown className="w-4 h-4 text-gray-500" /> :
                   <Minus className="w-4 h-4 text-blue-600" />}
                  <span className="text-sm font-semibold capitalize">{ai.trending_signals.velocity}</span>
                </div>
                <p className="text-xs text-gray-600">Peak: {ai.trending_signals.peak_period}</p>
              </div>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                <div className="text-[10px] text-blue-500 font-semibold uppercase tracking-wide mb-1">Prediction</div>
                <p className="text-xs text-gray-700">{ai.trending_signals.prediction}</p>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Narrative Angles */}
      {(ai.narrative_angles?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Flame className="w-4 h-4 text-red-500" /> Narrative Angles
          </h2>
          <div className="space-y-2">
            {ai.narrative_angles.map((angle, i) => (
              <NarrativeCard key={i} angle={angle} idx={i} />
            ))}
          </div>
        </div>
      )}

      {/* Key Claims */}
      {(ai.key_claims_tracked?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <Search className="w-4 h-4 text-purple-500" /> Key Claims Tracked
          </h2>
          <div className="space-y-2">
            {ai.key_claims_tracked.map((claim, i) => {
              const AssessIcon = ASSESS_ICON[claim.assessment] || HelpCircle;
              const assessColor = ASSESS_COLOR[claim.assessment] || "text-gray-500";
              return (
                <div key={i} className="flex items-start gap-3 bg-gray-50 rounded-lg p-3">
                  <AssessIcon className={`w-4 h-4 shrink-0 mt-0.5 ${assessColor}`} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-700">{claim.claim}</p>
                    <div className="flex items-center gap-2 mt-1 flex-wrap">
                      <span className="text-[10px] text-gray-400">{claim.videos_making_claim} videos</span>
                      <span className={`text-[10px] font-medium ${assessColor}`}>{claim.assessment}</span>
                      {claim.channels?.map((ch) => (
                        <span key={ch} className="text-[10px] px-1.5 py-0.5 bg-white border border-gray-200 rounded text-gray-500">{ch}</span>
                      ))}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Emerging Stories */}
      {(ai.emerging_stories?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <TrendingUp className="w-4 h-4 text-red-500" /> Emerging Stories
          </h2>
          <div className="space-y-2">
            {ai.emerging_stories.map((s, i) => (
              <div key={i} className="bg-gray-50 rounded-lg p-3">
                <div className="text-sm font-medium text-gray-800">{s.topic}</div>
                <p className="text-xs text-gray-600 mt-1">{s.early_signals}</p>
                <div className="flex gap-1 mt-1.5 flex-wrap">
                  {s.channels_covering.map((ch) => (
                    <span key={ch} className="text-[10px] px-2 py-0.5 bg-red-50 text-red-600 rounded-full">{ch}</span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Notable Absences */}
      {ai.notable_absences && (
        <div className="bg-amber-50 border border-amber-200 rounded-2xl p-5">
          <h2 className="font-semibold text-amber-800 flex items-center gap-2 mb-2">
            <AlertTriangle className="w-4 h-4 text-amber-500" /> Notable Absences
          </h2>
          <p className="text-sm text-amber-700">{ai.notable_absences}</p>
        </div>
      )}

      {/* All Videos sorted */}
      {report.videos.length > 0 && (
        <VideosTable videos={report.videos} />
      )}
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────────

export default function MonitoringPage() {
  const [hours, setHours] = useState(24);
  const [report, setReport] = useState<MonitoringReport | null>(null);
  const [pastReports, setPastReports] = useState<MonitoringReportSummary[]>([]);
  const [generating, setGenerating] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [stateInfo, setStateInfo] = useState<NarrativeStateInfo | null>(null);
  const [buildingBatch, setBuildingBatch] = useState(false);

  useEffect(() => {
    Promise.all([
      api.getMonitoringReports(20).then(setPastReports).catch(() => {}),
      api.getNarrativeState().then(setStateInfo).catch(() => {}),
    ]).finally(() => setLoading(false));
  }, []);

  const handleGenerate = async () => {
    setGenerating(true);
    setError("");
    setReport(null);
    try {
      const r = await api.generateMonitoringReport(hours);
      if (r.error && !r.analysis) {
        setError(r.error);
      } else {
        setReport(r);
        api.getMonitoringReports(20).then(setPastReports).catch(() => {});
        api.getNarrativeState().then(setStateInfo).catch(() => {});
      }
    } catch {
      setError("Failed to generate report");
    } finally {
      setGenerating(false);
    }
  };

  const handleBatchBuild = async () => {
    setBuildingBatch(true);
    setError("");
    try {
      const result = await api.triggerBatchBuild(hours);
      if (result.error) {
        setError(result.error);
      } else {
        api.getNarrativeState().then(setStateInfo).catch(() => {});
      }
    } catch {
      setError("Failed to build batch state");
    } finally {
      setBuildingBatch(false);
    }
  };

  const handleLoadReport = async (id: number) => {
    setLoading(true);
    setError("");
    try {
      const r = await api.getMonitoringReportById(id);
      setReport(r);
    } catch {
      setError("Failed to load report");
    } finally {
      setLoading(false);
    }
  };

  const hasReport = report?.analysis && ("narrative_angles" in report.analysis || "dominant_narratives" in report.analysis);

  return (
    <div className="space-y-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Link href="/" className="text-gray-400 hover:text-gray-600">
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
              <Flame className="w-6 h-6 text-red-500" /> Monitoring Report
            </h1>
            <p className="text-sm text-gray-500 mt-0.5">
              Narrative analysis across all tracked channels
            </p>
          </div>
        </div>
      </div>

      {/* State Status */}
      {stateInfo?.active && (
        <div className="bg-emerald-50 border border-emerald-200 rounded-2xl p-4 no-print">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 bg-emerald-500 rounded-full animate-pulse" />
              <span className="text-sm font-medium text-emerald-800">Narrative State Active</span>
              <span className="text-xs text-emerald-600">
                {stateInfo.total_videos_processed} videos | {stateInfo.narrative_count} narratives | {stateInfo.incremental_updates} updates
              </span>
            </div>
            <div className="flex items-center gap-3 text-xs text-emerald-600">
              <span>Cost: ${((stateInfo.total_gemini_cost_usd || 0) + (stateInfo.total_haiku_cost_usd || 0)).toFixed(4)}</span>
              {stateInfo.updated_at && <span>Updated: {formatIST(stateInfo.updated_at)} IST</span>}
            </div>
          </div>
        </div>
      )}

      {/* Generate controls */}
      <div className="bg-white border border-gray-200 rounded-2xl p-5 no-print">
        <div className="flex items-center gap-3">
          <span className="text-sm font-medium text-gray-700">Time window:</span>
          <div className="flex gap-1 bg-gray-100 rounded-xl p-1">
            {HOUR_OPTIONS.map((o) => (
              <button
                key={o.value}
                onClick={() => setHours(o.value)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  hours === o.value ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
                }`}
              >
                {o.label}
              </button>
            ))}
          </div>
          <div className="ml-auto flex items-center gap-2">
            <button
              onClick={handleBatchBuild}
              disabled={buildingBatch || generating}
              className="flex items-center gap-2 px-4 py-2.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-40 text-gray-700 rounded-xl text-sm font-medium transition-colors"
            >
              {buildingBatch ? <Loader2 className="w-4 h-4 animate-spin" /> : <Brain className="w-4 h-4" />}
              {buildingBatch ? "Building..." : "Rebuild State"}
            </button>
            <button
              onClick={handleGenerate}
              disabled={generating}
              className="flex items-center gap-2 px-5 py-2.5 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
            >
              {generating ? <Loader2 className="w-4 h-4 animate-spin" /> : <Zap className="w-4 h-4" />}
              {generating ? "Analyzing..." : "Generate Report"}
            </button>
          </div>
        </div>
        {error && (
          <p className="text-sm text-red-500 mt-3 flex items-center gap-1">
            <AlertTriangle className="w-4 h-4" /> {error}
          </p>
        )}
      </div>

      {/* Loading state */}
      {generating && <LoadingView hasState={!!stateInfo?.active} />}

      {/* Report */}
      {!generating && hasReport && <ReportView report={report!} />}

      {/* Past Reports (show when no active report) */}
      {!generating && !hasReport && (
        <div className="bg-white border border-gray-200 rounded-2xl">
          <div className="px-5 py-4 border-b border-gray-100">
            <h2 className="font-semibold text-gray-800 flex items-center gap-2">
              <Clock className="w-4 h-4 text-gray-400" /> Previous Reports
            </h2>
          </div>
          <div className="divide-y divide-gray-50">
            {loading ? (
              <div className="px-5 py-8 text-center text-sm text-gray-400">Loading...</div>
            ) : pastReports.length === 0 ? (
              <div className="px-5 py-8 text-center text-sm text-gray-400">
                No reports yet. Select a time window and generate your first report.
              </div>
            ) : (
              pastReports.map((r) => (
                <button
                  key={r.id}
                  onClick={() => handleLoadReport(r.id)}
                  className="w-full flex items-center gap-4 px-5 py-3 hover:bg-gray-50 transition-colors text-left"
                >
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="text-sm font-medium text-gray-800">Last {r.hours}h Report</span>
                      <span className="text-xs text-gray-400">{r.video_count} videos · {r.channel_count} channels</span>
                      {r.has_error && <AlertTriangle className="w-3 h-3 text-amber-500" />}
                    </div>
                    <div className="text-xs text-gray-400 mt-0.5">
                      {formatIST(r.generated_at)} IST · ${r.gemini_cost_usd.toFixed(4)}
                    </div>
                  </div>
                  <ExternalLink className="w-4 h-4 text-gray-300 shrink-0" />
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
