"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { api, type YouTubeReportSummary } from "@/lib/api";
import { formatIST } from "@/lib/utils";
import {
  Search, Play, Clock, ArrowRight, AlertTriangle,
  Video, MessageSquare, Brain, TrendingUp, Radio,
} from "lucide-react";

const TIME_OPTIONS = [
  { label: "24h", value: 24 },
  { label: "48h", value: 48 },
  { label: "7 days", value: 168 },
  { label: "30 days", value: 720 },
];

export default function HomePage() {
  const router = useRouter();
  const [topic, setTopic] = useState("");
  const [hours, setHours] = useState(168);
  const [reports, setReports] = useState<YouTubeReportSummary[]>([]);
  const [loadingReports, setLoadingReports] = useState(true);

  useEffect(() => {
    api
      .getYouTubeReports(50)
      .then(setReports)
      .catch(() => {})
      .finally(() => setLoadingReports(false));
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const t = topic.trim();
    if (t) router.push(`/report/${encodeURIComponent(t)}?hours=${hours}`);
  };

  return (
    <div className="space-y-8">
      {/* Hero */}
      <div className="text-center py-8">
        <div className="inline-flex items-center gap-3 mb-4">
          <div className="w-12 h-12 rounded-2xl bg-gradient-to-br from-red-500 to-red-700 flex items-center justify-center shadow-lg">
            <Play className="w-6 h-6 text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900">YouTube Intel</h1>
        </div>
        <p className="text-gray-500 max-w-lg mx-auto">
          Track how narratives spread across YouTube. Search a topic, analyze transcripts and comments, get structured intelligence.
        </p>
      </div>

      {/* Search */}
      <form onSubmit={handleSubmit} className="max-w-2xl mx-auto">
        <div className="flex gap-3">
          <div className="flex-1 relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              value={topic}
              onChange={(e) => setTopic(e.target.value)}
              placeholder="Enter topic (e.g. Modi Tea Garden, Waqf Bill)"
              className="w-full pl-10 pr-4 py-3 bg-white border border-gray-200 rounded-xl text-sm text-gray-900 placeholder-gray-400 focus:border-red-400 focus:outline-none focus:ring-2 focus:ring-red-100"
            />
          </div>
          <select
            value={hours}
            onChange={(e) => setHours(Number(e.target.value))}
            className="px-3 py-3 bg-white border border-gray-200 rounded-xl text-sm text-gray-700 focus:border-red-400 focus:outline-none"
          >
            {TIME_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>{o.label}</option>
            ))}
          </select>
          <button
            type="submit"
            disabled={!topic.trim()}
            className="px-6 py-3 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
          >
            Analyze
          </button>
        </div>
      </form>

      {/* Quick Links */}
      <div className="max-w-2xl mx-auto space-y-3">
        <Link
          href="/monitoring"
          className="flex items-center gap-4 bg-white border border-red-200 rounded-xl p-4 hover:bg-red-50 transition-colors"
        >
          <div className="w-10 h-10 rounded-xl bg-red-100 flex items-center justify-center">
            <TrendingUp className="w-5 h-5 text-red-600" />
          </div>
          <div className="flex-1">
            <div className="text-sm font-semibold text-gray-800">Monitoring Report</div>
            <div className="text-xs text-gray-500">What are your 70+ channels saying right now? Narrative analysis across all tracked channels.</div>
          </div>
          <ArrowRight className="w-4 h-4 text-gray-300" />
        </Link>
        <Link
          href="/channels"
          className="flex items-center gap-4 bg-white border border-gray-200 rounded-xl p-4 hover:bg-gray-50 transition-colors"
        >
          <div className="w-10 h-10 rounded-xl bg-red-50 flex items-center justify-center">
            <Radio className="w-5 h-5 text-red-500" />
          </div>
          <div className="flex-1">
            <div className="text-sm font-semibold text-gray-800">Channel Monitor</div>
            <div className="text-xs text-gray-500">Track YouTube channels, auto-detect new videos, classify topics</div>
          </div>
          <ArrowRight className="w-4 h-4 text-gray-300" />
        </Link>
      </div>

      {/* Features */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 max-w-3xl mx-auto">
        {[
          { icon: Video, label: "Video Discovery", desc: "Find all videos on a topic" },
          { icon: MessageSquare, label: "Transcript Analysis", desc: "Extract and analyze speech" },
          { icon: Brain, label: "Narrative Mapping", desc: "Classify angles and bias" },
          { icon: TrendingUp, label: "Trend Signals", desc: "Track velocity and reach" },
        ].map(({ icon: Icon, label, desc }) => (
          <div key={label} className="bg-white border border-gray-200 rounded-xl p-4 text-center">
            <Icon className="w-5 h-5 text-red-500 mx-auto mb-2" />
            <div className="text-sm font-semibold text-gray-800">{label}</div>
            <div className="text-xs text-gray-500 mt-0.5">{desc}</div>
          </div>
        ))}
      </div>

      {/* Previous Reports */}
      <div className="bg-white border border-gray-200 rounded-2xl max-w-3xl mx-auto">
        <div className="px-5 py-4 border-b border-gray-100">
          <h2 className="font-semibold text-gray-800 flex items-center gap-2">
            <Clock className="w-4 h-4 text-gray-400" /> Previous Reports
          </h2>
        </div>
        <div className="divide-y divide-gray-50">
          {loadingReports ? (
            <div className="px-5 py-8 text-center text-sm text-gray-400">Loading...</div>
          ) : reports.length === 0 ? (
            <div className="px-5 py-8 text-center text-sm text-gray-400">
              No reports yet. Enter a topic above to generate your first report.
            </div>
          ) : (
            reports.map((r) => (
              <Link
                key={r.id}
                href={`/report/${encodeURIComponent(r.topic)}?id=${r.id}&hours=${r.hours}`}
                className="flex items-center gap-4 px-5 py-3 hover:bg-gray-50 transition-colors"
              >
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-sm font-medium text-gray-800">{r.topic}</span>
                    <span className="text-xs text-gray-400">{r.video_count} videos</span>
                    {r.has_error && <AlertTriangle className="w-3 h-3 text-amber-500" />}
                  </div>
                  <div className="text-xs text-gray-400 mt-0.5">
                    Last {r.hours}h · {formatIST(r.generated_at)} IST · ${r.gemini_cost_usd.toFixed(4)}
                  </div>
                </div>
                <ArrowRight className="w-4 h-4 text-gray-300 shrink-0" />
              </Link>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
