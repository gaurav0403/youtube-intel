"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import Link from "next/link";
import {
  api,
  type MonitoringReport,
  type MonitoringReportSummary,
} from "@/lib/api";
import { formatNumber, formatIST, timeAgo } from "@/lib/utils";
import {
  ArrowLeft,
  Loader2,
  Zap,
  Clock,
  AlertTriangle,
  TrendingUp,
  Eye,
  Tag,
  ExternalLink,
  ChevronDown,
  ChevronUp,
  BarChart3,
  Newspaper,
  Users,
  Flame,
} from "lucide-react";

const HOUR_OPTIONS = [
  { label: "24h", value: 24 },
  { label: "48h", value: 48 },
  { label: "72h", value: 72 },
];

export default function MonitoringPage() {
  const [hours, setHours] = useState(24);
  const [report, setReport] = useState<MonitoringReport | null>(null);
  const [pastReports, setPastReports] = useState<MonitoringReportSummary[]>([]);
  const [generating, setGenerating] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    api
      .getMonitoringReports(20)
      .then(setPastReports)
      .catch(() => {})
      .finally(() => setLoading(false));
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
        // Refresh past reports
        api.getMonitoringReports(20).then(setPastReports).catch(() => {});
      }
    } catch {
      setError("Failed to generate report");
    } finally {
      setGenerating(false);
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

  const ai = report?.analysis;

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
              <Newspaper className="w-6 h-6 text-red-500" /> Monitoring Report
            </h1>
            <p className="text-sm text-gray-500 mt-0.5">
              What are your tracked channels saying right now?
            </p>
          </div>
        </div>
      </div>

      {/* Generate controls */}
      <div className="bg-white border border-gray-200 rounded-2xl p-5">
        <div className="flex items-center gap-3">
          <span className="text-sm font-medium text-gray-700">Time window:</span>
          <div className="flex gap-1 bg-gray-100 rounded-xl p-1">
            {HOUR_OPTIONS.map((o) => (
              <button
                key={o.value}
                onClick={() => setHours(o.value)}
                className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                  hours === o.value
                    ? "bg-white text-gray-900 shadow-sm"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                {o.label}
              </button>
            ))}
          </div>
          <button
            onClick={handleGenerate}
            disabled={generating}
            className="ml-auto flex items-center gap-2 px-5 py-2.5 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
          >
            {generating ? (
              <>
                <Loader2 className="w-4 h-4 animate-spin" />
                Analyzing...
              </>
            ) : (
              <>
                <Zap className="w-4 h-4" />
                Generate Report
              </>
            )}
          </button>
        </div>
        {generating && (
          <p className="text-xs text-gray-400 mt-3">
            Fetching transcripts and analyzing narratives across all channels. This may take 30-60 seconds...
          </p>
        )}
        {error && (
          <p className="text-sm text-red-500 mt-3 flex items-center gap-1">
            <AlertTriangle className="w-4 h-4" /> {error}
          </p>
        )}
      </div>

      {/* Report Content */}
      {report && ai && !("error" in ai && ai.error && !ai.headline) ? (
        <ReportView report={report} />
      ) : (
        /* Past Reports */
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
                No monitoring reports yet. Select a time window and generate your first report.
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
                      <span className="text-sm font-medium text-gray-800">
                        Last {r.hours}h Report
                      </span>
                      <span className="text-xs text-gray-400">
                        {r.video_count} videos from {r.channel_count} channels
                      </span>
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

function ReportView({ report }: { report: MonitoringReport }) {
  const ai = report.analysis!;

  return (
    <div className="space-y-6">
      {/* Headline + Summary */}
      <div className="bg-gradient-to-br from-red-50 to-white border border-red-100 rounded-2xl p-6">
        <div className="flex items-center gap-2 text-xs text-gray-400 mb-2">
          <Clock className="w-3 h-3" />
          Last {report.hours}h · {report.video_count} videos · {report.channel_count} channels · {formatIST(report.generated_at)} IST
        </div>
        <h2 className="text-xl font-bold text-gray-900 mb-3">{ai.headline}</h2>
        <p className="text-sm text-gray-600 leading-relaxed">{ai.executive_summary}</p>
      </div>

      {/* Sentiment Overview */}
      {ai.sentiment_overview && (
        <div className="bg-white border border-gray-200 rounded-2xl p-5">
          <h3 className="font-semibold text-gray-800 flex items-center gap-2 mb-4">
            <BarChart3 className="w-4 h-4 text-red-500" /> Sentiment Overview
          </h3>
          <div className="grid grid-cols-4 gap-4 mb-3">
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-900 capitalize">{ai.sentiment_overview.overall}</div>
              <div className="text-xs text-gray-500">Overall Tone</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600">{ai.sentiment_overview.pro_government_pct}%</div>
              <div className="text-xs text-gray-500">Pro-Government</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-red-600">{ai.sentiment_overview.critical_pct}%</div>
              <div className="text-xs text-gray-500">Critical</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-gray-500">{ai.sentiment_overview.neutral_pct}%</div>
              <div className="text-xs text-gray-500">Neutral</div>
            </div>
          </div>
          {/* Sentiment bar */}
          <div className="flex h-3 rounded-full overflow-hidden">
            <div className="bg-green-500" style={{ width: `${ai.sentiment_overview.pro_government_pct}%` }} />
            <div className="bg-red-500" style={{ width: `${ai.sentiment_overview.critical_pct}%` }} />
            <div className="bg-gray-300" style={{ width: `${ai.sentiment_overview.neutral_pct}%` }} />
          </div>
          {ai.sentiment_overview.most_polarizing_topic && (
            <p className="text-xs text-gray-500 mt-3">
              Most polarizing: <span className="font-medium text-gray-700">{ai.sentiment_overview.most_polarizing_topic}</span>
            </p>
          )}
        </div>
      )}

      {/* Dominant Narratives */}
      {(ai.dominant_narratives?.length ?? 0) > 0 && (
        <div className="space-y-4">
          <h3 className="font-semibold text-gray-800 flex items-center gap-2">
            <Flame className="w-4 h-4 text-red-500" /> Dominant Narratives
          </h3>
          {ai.dominant_narratives.map((n, i) => (
            <NarrativeCard key={i} narrative={n} index={i} />
          ))}
        </div>
      )}

      {/* Category Breakdown */}
      {(ai.category_breakdown?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="font-semibold text-gray-800 flex items-center gap-2">
              <Tag className="w-4 h-4 text-red-500" /> Category Breakdown
            </h3>
          </div>
          <div className="divide-y divide-gray-50">
            {ai.category_breakdown.map((cat) => (
              <div key={cat.category} className="px-5 py-3">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-800">{cat.category}</span>
                  <span className="text-xs text-gray-400">{cat.video_count} videos</span>
                </div>
                <p className="text-xs text-gray-600 mt-1">{cat.primary_focus}</p>
                {cat.notable_framing && (
                  <p className="text-xs text-gray-400 mt-0.5 italic">{cat.notable_framing}</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Channel Highlights */}
      {(ai.channel_highlights?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="font-semibold text-gray-800 flex items-center gap-2">
              <Users className="w-4 h-4 text-red-500" /> Channel Highlights
            </h3>
          </div>
          <div className="divide-y divide-gray-50">
            {ai.channel_highlights.map((ch) => (
              <div key={ch.channel_name} className="px-5 py-3">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-gray-800">{ch.channel_name}</span>
                    <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded-full text-xs">{ch.category}</span>
                  </div>
                  <span className="text-xs text-gray-400">{ch.videos_published} videos</span>
                </div>
                <p className="text-xs text-gray-600 mt-1">
                  <span className="font-medium">Focus:</span> {ch.primary_topic}
                </p>
                <p className="text-xs text-gray-500 mt-0.5">
                  <span className="font-medium">Stance:</span> {ch.stance}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Emerging Stories */}
      {(ai.emerging_stories?.length ?? 0) > 0 && (
        <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
          <div className="px-5 py-4 border-b border-gray-100">
            <h3 className="font-semibold text-gray-800 flex items-center gap-2">
              <TrendingUp className="w-4 h-4 text-red-500" /> Emerging Stories
            </h3>
          </div>
          <div className="divide-y divide-gray-50">
            {ai.emerging_stories.map((s, i) => (
              <div key={i} className="px-5 py-3">
                <div className="text-sm font-medium text-gray-800">{s.topic}</div>
                <p className="text-xs text-gray-600 mt-1">{s.early_signals}</p>
                <div className="flex gap-1 mt-1.5 flex-wrap">
                  {s.channels_covering.map((ch) => (
                    <span key={ch} className="text-xs px-2 py-0.5 bg-red-50 text-red-600 rounded-full">
                      {ch}
                    </span>
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
          <h3 className="font-semibold text-amber-800 flex items-center gap-2 mb-2">
            <AlertTriangle className="w-4 h-4 text-amber-500" /> Notable Absences
          </h3>
          <p className="text-sm text-amber-700">{ai.notable_absences}</p>
        </div>
      )}

      {/* Videos Grid */}
      {report.videos.length > 0 && (
        <VideoGrid videos={report.videos} />
      )}
    </div>
  );
}

function NarrativeCard({
  narrative,
  index,
}: {
  narrative: MonitoringReport["analysis"] extends null ? never : NonNullable<MonitoringReport["analysis"]>["dominant_narratives"][0];
  index: number;
}) {
  const [expanded, setExpanded] = useState(index === 0);
  const n = narrative;

  const sentimentColor = {
    positive: "bg-green-100 text-green-700",
    negative: "bg-red-100 text-red-700",
    neutral: "bg-gray-100 text-gray-600",
    mixed: "bg-amber-100 text-amber-700",
  }[n.sentiment] || "bg-gray-100 text-gray-600";

  return (
    <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full px-5 py-4 flex items-center gap-3 text-left hover:bg-gray-50 transition-colors"
      >
        <span className="text-lg font-bold text-red-500 w-6">#{index + 1}</span>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-gray-800">{n.title}</span>
            <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${sentimentColor}`}>
              {n.sentiment}
            </span>
            <span className="text-xs text-gray-400">{n.video_count} videos</span>
          </div>
        </div>
        {expanded ? (
          <ChevronUp className="w-4 h-4 text-gray-400" />
        ) : (
          <ChevronDown className="w-4 h-4 text-gray-400" />
        )}
      </button>

      {expanded && (
        <div className="px-5 pb-5 space-y-4">
          <p className="text-sm text-gray-600">{n.description}</p>

          {/* Channels pushing */}
          <div>
            <div className="text-xs font-medium text-gray-500 mb-1.5">Channels pushing this narrative</div>
            <div className="flex gap-1.5 flex-wrap">
              {n.channels_pushing.map((ch) => (
                <span key={ch} className="text-xs px-2.5 py-1 bg-red-50 text-red-600 rounded-full">
                  {ch}
                </span>
              ))}
            </div>
          </div>

          {/* Key claims */}
          {n.key_claims.length > 0 && (
            <div>
              <div className="text-xs font-medium text-gray-500 mb-1.5">Key Claims</div>
              <ul className="space-y-1">
                {n.key_claims.map((c, i) => (
                  <li key={i} className="text-xs text-gray-600 flex gap-2">
                    <span className="text-red-400 mt-0.5">-</span>
                    {c}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Top videos */}
          {n.top_videos.length > 0 && (
            <div>
              <div className="text-xs font-medium text-gray-500 mb-1.5">Top Videos</div>
              <div className="space-y-2">
                {n.top_videos.map((v) => (
                  <a
                    key={v.video_id}
                    href={`https://youtube.com/watch?v=${v.video_id}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="flex items-center gap-3 p-2 rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    <Image
                      src={`https://i.ytimg.com/vi/${v.video_id}/mqdefault.jpg`}
                      alt={v.title}
                      width={120}
                      height={68}
                      className="w-[120px] h-[68px] rounded-md object-cover shrink-0"
                      unoptimized
                    />
                    <div className="flex-1 min-w-0">
                      <div className="text-xs font-medium text-gray-800 line-clamp-2">{v.title}</div>
                      <div className="text-xs text-gray-400 mt-0.5">
                        {v.channel} · {formatNumber(v.views)} views
                      </div>
                    </div>
                    <ExternalLink className="w-3 h-3 text-gray-300 shrink-0" />
                  </a>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function VideoGrid({ videos }: { videos: MonitoringReport["videos"] }) {
  const [showAll, setShowAll] = useState(false);
  const displayed = showAll ? videos : videos.slice(0, 12);

  return (
    <div className="space-y-3">
      <h3 className="font-semibold text-gray-800 flex items-center gap-2">
        <Eye className="w-4 h-4 text-red-500" /> All Videos ({videos.length})
      </h3>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
        {displayed.map((v) => (
          <a
            key={v.video_id}
            href={`https://youtube.com/watch?v=${v.video_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="bg-white border border-gray-200 rounded-xl overflow-hidden hover:shadow-md transition-shadow"
          >
            <Image
              src={v.thumbnail || `https://i.ytimg.com/vi/${v.video_id}/hqdefault.jpg`}
              alt={v.title}
              width={320}
              height={180}
              className="w-full aspect-video object-cover"
              unoptimized
            />
            <div className="p-3">
              <div className="text-xs font-medium text-gray-800 line-clamp-2">{v.title}</div>
              <div className="text-xs text-gray-400 mt-1">{v.channel_name}</div>
              <div className="flex items-center gap-2 mt-1.5">
                {v.topic_classification && v.topic_classification !== "unknown" && (
                  <span className="text-xs px-1.5 py-0.5 bg-red-50 text-red-600 rounded">
                    {v.topic_classification}
                  </span>
                )}
                {v.view_count > 0 && (
                  <span className="text-xs text-gray-400">{formatNumber(v.view_count)}</span>
                )}
              </div>
            </div>
          </a>
        ))}
      </div>
      {videos.length > 12 && !showAll && (
        <button
          onClick={() => setShowAll(true)}
          className="w-full py-2 text-sm text-red-600 hover:text-red-700 font-medium"
        >
          Show all {videos.length} videos
        </button>
      )}
    </div>
  );
}
