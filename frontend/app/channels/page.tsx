"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import { api, type WatchedChannel, type ChannelVideoItem } from "@/lib/api";
import { formatNumber, timeAgo } from "@/lib/utils";
import {
  Plus, Trash2, RefreshCw, Radio, Eye, Tag,
  ArrowLeft, ExternalLink, Loader2,
} from "lucide-react";
import Link from "next/link";

export default function ChannelsPage() {
  const [channels, setChannels] = useState<WatchedChannel[]>([]);
  const [feed, setFeed] = useState<ChannelVideoItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [addUrl, setAddUrl] = useState("");
  const [adding, setAdding] = useState(false);
  const [polling, setPolling] = useState(false);
  const [error, setError] = useState("");
  const [tab, setTab] = useState<"channels" | "feed">("channels");

  const loadData = async () => {
    try {
      const [ch, f] = await Promise.all([api.getChannels(), api.getActivityFeed(50)]);
      setChannels(ch);
      setFeed(f);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadData(); }, []);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!addUrl.trim()) return;
    setAdding(true);
    setError("");
    try {
      const res = await api.addChannel(addUrl.trim());
      if ("error" in res && res.error) {
        setError(res.error);
      } else {
        setAddUrl("");
        await loadData();
      }
    } catch {
      setError("Failed to add channel");
    } finally {
      setAdding(false);
    }
  };

  const handleRemove = async (channelId: string) => {
    await api.removeChannel(channelId);
    await loadData();
  };

  const handlePoll = async () => {
    setPolling(true);
    try {
      const res = await api.pollChannels();
      alert(`Checked ${res.channels_checked} channels, found ${res.new_videos} new videos`);
      await loadData();
    } catch {
      alert("Poll failed");
    } finally {
      setPolling(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Link href="/" className="text-gray-400 hover:text-gray-600">
            <ArrowLeft className="w-5 h-5" />
          </Link>
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
              <Radio className="w-6 h-6 text-red-500" /> Channel Monitor
            </h1>
            <p className="text-sm text-gray-500 mt-0.5">
              Track YouTube channels and auto-detect new videos
            </p>
          </div>
        </div>
        <button
          onClick={handlePoll}
          disabled={polling || channels.length === 0}
          className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${polling ? "animate-spin" : ""}`} />
          {polling ? "Polling..." : "Poll Now"}
        </button>
      </div>

      {/* Add Channel */}
      <form onSubmit={handleAdd} className="flex gap-3">
        <div className="flex-1 relative">
          <Plus className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            value={addUrl}
            onChange={(e) => setAddUrl(e.target.value)}
            placeholder="Add channel — paste URL, @handle, or channel ID"
            className="w-full pl-10 pr-4 py-3 bg-white border border-gray-200 rounded-xl text-sm text-gray-900 placeholder-gray-400 focus:border-red-400 focus:outline-none focus:ring-2 focus:ring-red-100"
          />
        </div>
        <button
          type="submit"
          disabled={!addUrl.trim() || adding}
          className="px-5 py-3 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
        >
          {adding ? <Loader2 className="w-4 h-4 animate-spin" /> : "Add"}
        </button>
      </form>
      {error && <p className="text-sm text-red-500">{error}</p>}

      {/* Tabs */}
      <div className="flex gap-1 bg-gray-100 rounded-xl p-1 max-w-xs">
        <button
          onClick={() => setTab("channels")}
          className={`flex-1 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            tab === "channels" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500"
          }`}
        >
          Channels ({channels.length})
        </button>
        <button
          onClick={() => setTab("feed")}
          className={`flex-1 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            tab === "feed" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500"
          }`}
        >
          Feed ({feed.length})
        </button>
      </div>

      {loading ? (
        <div className="text-center py-12 text-sm text-gray-400">Loading...</div>
      ) : tab === "channels" ? (
        /* Channels List */
        <div className="space-y-3">
          {channels.length === 0 ? (
            <div className="bg-white border border-gray-200 rounded-2xl p-8 text-center text-sm text-gray-400">
              No channels yet. Add a YouTube channel above to start monitoring.
            </div>
          ) : (
            channels.filter(c => c.is_active).map((ch) => (
              <div
                key={ch.channel_id}
                className="bg-white border border-gray-200 rounded-xl p-4 flex items-center gap-4"
              >
                {ch.thumbnail && (
                  <Image
                    src={ch.thumbnail}
                    alt={ch.channel_name}
                    width={48}
                    height={48}
                    className="w-12 h-12 rounded-full"
                    unoptimized
                  />
                )}
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <a
                      href={`https://youtube.com/channel/${ch.channel_id}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm font-semibold text-gray-800 hover:text-red-600 flex items-center gap-1"
                    >
                      {ch.channel_name}
                      <ExternalLink className="w-3 h-3" />
                    </a>
                    <span className="text-xs text-gray-400">
                      {formatNumber(ch.subscriber_count)} subscribers
                    </span>
                  </div>
                  <div className="text-xs text-gray-400 mt-0.5">
                    {ch.video_count} videos tracked
                    {ch.last_checked_at && <> · Last checked {timeAgo(ch.last_checked_at)}</>}
                  </div>
                </div>
                <button
                  onClick={() => handleRemove(ch.channel_id)}
                  className="p-2 text-gray-300 hover:text-red-500 transition-colors"
                  title="Remove channel"
                >
                  <Trash2 className="w-4 h-4" />
                </button>
              </div>
            ))
          )}
        </div>
      ) : (
        /* Activity Feed */
        <div className="space-y-3">
          {feed.length === 0 ? (
            <div className="bg-white border border-gray-200 rounded-2xl p-8 text-center text-sm text-gray-400">
              No videos detected yet. Add channels and click &quot;Poll Now&quot;.
            </div>
          ) : (
            feed.map((v) => (
              <a
                key={v.video_id}
                href={`https://youtube.com/watch?v=${v.video_id}`}
                target="_blank"
                rel="noopener noreferrer"
                className="bg-white border border-gray-200 rounded-xl p-4 flex gap-4 hover:bg-gray-50 transition-colors block"
              >
                <Image
                  src={v.thumbnail || `https://i.ytimg.com/vi/${v.video_id}/hqdefault.jpg`}
                  alt={v.title}
                  width={160}
                  height={90}
                  className="w-40 h-[90px] rounded-lg object-cover shrink-0"
                  unoptimized
                />
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium text-gray-800 line-clamp-2">{v.title}</div>
                  <div className="text-xs text-gray-500 mt-1">{v.channel_name}</div>
                  <div className="flex items-center gap-3 mt-2">
                    {v.topic_classification && v.topic_classification !== "unknown" && (
                      <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-red-50 text-red-600 rounded-full text-xs">
                        <Tag className="w-3 h-3" /> {v.topic_classification}
                      </span>
                    )}
                    {v.view_count > 0 && (
                      <span className="text-xs text-gray-400 flex items-center gap-1">
                        <Eye className="w-3 h-3" /> {formatNumber(v.view_count)}
                      </span>
                    )}
                    <span className="text-xs text-gray-400">
                      {timeAgo(v.published_at)}
                    </span>
                  </div>
                  {v.summary && (
                    <p className="text-xs text-gray-500 mt-1 line-clamp-1">{v.summary}</p>
                  )}
                </div>
              </a>
            ))
          )}
        </div>
      )}
    </div>
  );
}
