using System;
using System.Collections.Generic;
using System.Web.Script.Serialization;

namespace Ehp.DataBridge.Tray
{
    internal sealed class RequestActivitySeries
    {
        internal readonly long FirstBucketUtcTicks;
        internal readonly long TrackingSinceUtcTicks;
        internal readonly long[] Counts;
        internal readonly long Total;

        internal RequestActivitySeries(long firstBucketUtcTicks,
            long trackingSinceUtcTicks, long[] counts)
        {
            FirstBucketUtcTicks = firstBucketUtcTicks;
            TrackingSinceUtcTicks = trackingSinceUtcTicks;
            Counts = counts;
            foreach (long count in counts)
                Total = count > Int64.MaxValue - Total ? Int64.MaxValue : Total + count;
        }
    }

    internal sealed class ActivityBucket
    {
        public long StartUtcTicks { get; set; }
        public long Count { get; set; }
    }

    internal sealed class ActivityDocument
    {
        public long TrackingSinceUtcTicks { get; set; }
        public List<ActivityBucket> Buckets { get; set; }
    }

    internal sealed class TunnelRequestActivity
    {
        internal const int BucketMinutes = 5;
        internal const int BucketCount = 48;
        private const long BucketTicks = TimeSpan.TicksPerMinute * BucketMinutes;
        private readonly SortedDictionary<long, long> buckets = new SortedDictionary<long, long>();
        private long? previousCounter;
        private long trackingSinceUtcTicks;

        private static long BucketStart(DateTime utcNow)
        {
            if (utcNow.Kind != DateTimeKind.Utc) utcNow = utcNow.ToUniversalTime();
            return utcNow.Ticks / BucketTicks * BucketTicks;
        }

        private void Prune(DateTime utcNow)
        {
            long current = BucketStart(utcNow);
            long first = current - (BucketCount - 1) * BucketTicks;
            var expired = new List<long>();
            foreach (var bucket in buckets)
                if (bucket.Key < first || bucket.Key > current) expired.Add(bucket.Key);
            foreach (long key in expired) buckets.Remove(key);
        }

        internal bool Observe(DateTime utcNow, long cumulativeRequests)
        {
            Prune(utcNow);
            if (cumulativeRequests < 0) return false;
            bool beganTracking = trackingSinceUtcTicks == 0;
            if (trackingSinceUtcTicks == 0) trackingSinceUtcTicks = utcNow.Ticks;
            long delta = 0;
            if (previousCounter.HasValue)
                delta = cumulativeRequests >= previousCounter.Value
                    ? cumulativeRequests - previousCounter.Value : cumulativeRequests;
            previousCounter = cumulativeRequests;
            if (delta == 0) return beganTracking;
            long key = BucketStart(utcNow);
            long existing;
            buckets.TryGetValue(key, out existing);
            buckets[key] = existing > Int64.MaxValue - delta ? Int64.MaxValue : existing + delta;
            return true;
        }

        internal RequestActivitySeries Snapshot(DateTime utcNow)
        {
            Prune(utcNow);
            long first = BucketStart(utcNow) - (BucketCount - 1) * BucketTicks;
            var counts = new long[BucketCount];
            for (int i = 0; i < counts.Length; i++)
                buckets.TryGetValue(first + i * BucketTicks, out counts[i]);
            return new RequestActivitySeries(first, trackingSinceUtcTicks, counts);
        }

        internal string ToJson()
        {
            var entries = new List<ActivityBucket>();
            foreach (var bucket in buckets)
                entries.Add(new ActivityBucket {
                    StartUtcTicks = bucket.Key, Count = bucket.Value
                });
            return new JavaScriptSerializer().Serialize(new ActivityDocument {
                TrackingSinceUtcTicks = trackingSinceUtcTicks, Buckets = entries
            });
        }

        internal static TunnelRequestActivity FromJson(string json, DateTime utcNow)
        {
            var activity = new TunnelRequestActivity();
            if (String.IsNullOrWhiteSpace(json)) return activity;
            try
            {
                var document = new JavaScriptSerializer().Deserialize<ActivityDocument>(json);
                if (document != null)
                {
                    if (document.TrackingSinceUtcTicks > 0
                        && document.TrackingSinceUtcTicks <= utcNow.Ticks)
                        activity.trackingSinceUtcTicks = document.TrackingSinceUtcTicks;
                    if (document.Buckets != null)
                        foreach (var entry in document.Buckets)
                            if (entry != null && entry.Count >= 0
                                && entry.StartUtcTicks > 0
                                && entry.StartUtcTicks % BucketTicks == 0)
                                activity.buckets[entry.StartUtcTicks] = entry.Count;
                }
            }
            catch (ArgumentException) { return new TunnelRequestActivity(); }
            catch (InvalidOperationException) { return new TunnelRequestActivity(); }
            activity.Prune(utcNow);
            return activity;
        }
    }
}
