"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import { api, type WatchedChannel, type ChannelVideoItem, type ChannelCategory } from "@/lib/api";
import { formatNumber, timeAgo } from "@/lib/utils";
import {
  Plus, Trash2, RefreshCw, Radio, Eye, Tag,
  ArrowLeft, ExternalLink, Loader2, Filter,
} from "lucide-react";
import Link from "next/link";

export default function ChannelsPage() {
  const [channels, setChannels] = useState<WatchedChannel[]>([]);
  const [categories, setCategories] = useState<ChannelCategory[]>([]);
  const [feed, setFeed] = useState<ChannelVideoItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [addUrl, setAddUrl] = useState("");
  const [addCategory, setAddCategory] = useState("");
  const [adding, setAdding] = useState(false);
  const [polling, setPolling] = useState(false);
  const [error, setError] = useState("");
  const [tab, setTab] = useState<"channels" | "feed">("channels");
  const [filterCategory, setFilterCategory] = useState<string | null>(null);

  const loadData = async () => {
    try {
      const [ch, cats, f] = await Promise.all([
        api.getChannels(filterCategory || undefined),
        api.getCategories(),
        api.getActivityFeed(50),
      ]);
      setChannels(ch);
      setCategories(cats);
      setFeed(f);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadData(); }, [filterCategory]);

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!addUrl.trim()) return;
    setAdding(true);
    setError("");
    try {
      const res = await api.addChannel(addUrl.trim(), addCategory || undefined);
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

  // Group channels by category
  const grouped = channels.reduce<Record<string, WatchedChannel[]>>((acc, ch) => {
    const cat = ch.category || "Uncategorized";
    (acc[cat] = acc[cat] || []).push(ch);
    return acc;
  }, {});

  // Sort category keys: put specific ones first, then alphabetical
  const sortedCategories = Object.keys(grouped).sort((a, b) => {
    if (a === "Uncategorized") return 1;
    if (b === "Uncategorized") return -1;
    return a.localeCompare(b);
  });

  const totalChannels = channels.filter(c => c.is_active).length;

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
              {totalChannels} channels tracked across {categories.length} categories
            </p>
          </div>
        </div>
        <button
          onClick={handlePoll}
          disabled={polling || totalChannels === 0}
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
        <input
          type="text"
          value={addCategory}
          onChange={(e) => setAddCategory(e.target.value)}
          placeholder="Category"
          className="w-40 px-3 py-3 bg-white border border-gray-200 rounded-xl text-sm text-gray-700 focus:border-red-400 focus:outline-none"
        />
        <button
          type="submit"
          disabled={!addUrl.trim() || adding}
          className="px-5 py-3 bg-red-600 hover:bg-red-700 disabled:opacity-40 text-white rounded-xl text-sm font-medium transition-colors"
        >
          {adding ? <Loader2 className="w-4 h-4 animate-spin" /> : "Add"}
        </button>
      </form>
      {error && <p className="text-sm text-red-500">{error}</p>}

      {/* Tabs + Category Filter */}
      <div className="flex items-center justify-between">
        <div className="flex gap-1 bg-gray-100 rounded-xl p-1">
          <button
            onClick={() => setTab("channels")}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              tab === "channels" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500"
            }`}
          >
            Channels ({totalChannels})
          </button>
          <button
            onClick={() => setTab("feed")}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              tab === "feed" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500"
            }`}
          >
            Feed ({feed.length})
          </button>
        </div>

        {tab === "channels" && categories.length > 0 && (
          <div className="flex items-center gap-2">
            <Filter className="w-4 h-4 text-gray-400" />
            <div className="flex gap-1 flex-wrap">
              <button
                onClick={() => setFilterCategory(null)}
                className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                  !filterCategory ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-500 hover:bg-gray-200"
                }`}
              >
                All
              </button>
              {categories.map((cat) => (
                <button
                  key={cat.category}
                  onClick={() => setFilterCategory(cat.category === filterCategory ? null : cat.category)}
                  className={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${
                    filterCategory === cat.category ? "bg-red-100 text-red-700" : "bg-gray-100 text-gray-500 hover:bg-gray-200"
                  }`}
                >
                  {cat.category} ({cat.count})
                </button>
              ))}
            </div>
          </div>
        )}
      </div>

      {loading ? (
        <div className="text-center py-12 text-sm text-gray-400">Loading...</div>
      ) : tab === "channels" ? (
        /* Channels grouped by category */
        <div className="space-y-6">
          {totalChannels === 0 ? (
            <div className="bg-white border border-gray-200 rounded-2xl p-8 text-center text-sm text-gray-400">
              No channels yet. Add a YouTube channel above to start monitoring.
            </div>
          ) : filterCategory ? (
            /* Filtered: flat list */
            <div className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
              <div className="px-5 py-3 bg-gray-50 border-b border-gray-100">
                <h3 className="text-sm font-semibold text-gray-700">{filterCategory}</h3>
              </div>
              <div className="divide-y divide-gray-50">
                {channels.filter(c => c.is_active).map((ch) => (
                  <ChannelRow key={ch.channel_id} ch={ch} onRemove={handleRemove} />
                ))}
              </div>
            </div>
          ) : (
            /* Grouped by category */
            sortedCategories.map((cat) => (
              <div key={cat} className="bg-white border border-gray-200 rounded-2xl overflow-hidden">
                <div className="px-5 py-3 bg-gray-50 border-b border-gray-100 flex items-center justify-between">
                  <h3 className="text-sm font-semibold text-gray-700">{cat}</h3>
                  <span className="text-xs text-gray-400">{grouped[cat].filter(c => c.is_active).length} channels</span>
                </div>
                <div className="divide-y divide-gray-50">
                  {grouped[cat].filter(c => c.is_active).map((ch) => (
                    <ChannelRow key={ch.channel_id} ch={ch} onRemove={handleRemove} />
                  ))}
                </div>
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
                    <span className="text-xs text-gray-400">{timeAgo(v.published_at)}</span>
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

function ChannelRow({ ch, onRemove }: { ch: WatchedChannel; onRemove: (id: string) => void }) {
  return (
    <div className="flex items-center gap-4 px-5 py-3">
      {ch.thumbnail && (
        <Image
          src={ch.thumbnail}
          alt={ch.channel_name}
          width={36}
          height={36}
          className="w-9 h-9 rounded-full"
          unoptimized
        />
      )}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <a
            href={`https://youtube.com/channel/${ch.channel_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm font-medium text-gray-800 hover:text-red-600 flex items-center gap-1"
          >
            {ch.channel_name}
            <ExternalLink className="w-3 h-3 opacity-40" />
          </a>
          <span className="text-xs text-gray-400">
            {formatNumber(ch.subscriber_count)} subs
          </span>
        </div>
        <div className="text-xs text-gray-400 mt-0.5">
          {ch.video_count} videos tracked
          {ch.last_checked_at && <> · Checked {timeAgo(ch.last_checked_at)}</>}
        </div>
      </div>
      <button
        onClick={() => onRemove(ch.channel_id)}
        className="p-2 text-gray-300 hover:text-red-500 transition-colors"
        title="Remove channel"
      >
        <Trash2 className="w-4 h-4" />
      </button>
    </div>
  );
}
