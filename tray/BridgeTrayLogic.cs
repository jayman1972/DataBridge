using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Net;
using System.Threading;
using System.Web.Script.Serialization;

namespace Ehp.DataBridge.Tray
{
    internal sealed class TunnelSnapshot
    {
        internal readonly string Url;
        internal readonly long? RequestCount;

        internal TunnelSnapshot(string url, long? requestCount)
        {
            Url = url;
            RequestCount = requestCount;
        }
    }

    internal static class BridgeTrayLogic
    {
        internal static TunnelSnapshot FindTunnel(string json)
        {
            if (String.IsNullOrWhiteSpace(json)) return null;
            object parsed;
            try { parsed = new JavaScriptSerializer().DeserializeObject(json); }
            catch (ArgumentException) { return null; }
            var document = parsed as Dictionary<string, object>;
            if (document == null || !document.ContainsKey("tunnels")) return null;
            var tunnels = document["tunnels"] as IEnumerable;
            if (tunnels == null) return null;
            foreach (object entry in tunnels)
            {
                var tunnel = entry as Dictionary<string, object>;
                if (tunnel == null) continue;
                var config = tunnel.ContainsKey("config")
                    ? tunnel["config"] as Dictionary<string, object> : null;
                string address = config != null && config.ContainsKey("addr")
                    ? Convert.ToString(config["addr"]) : "";
                string url = tunnel.ContainsKey("public_url")
                    ? Convert.ToString(tunnel["public_url"]) : "";
                if (address.EndsWith(":5000", StringComparison.OrdinalIgnoreCase)
                    && url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                {
                    long? requestCount = null;
                    var metrics = tunnel.ContainsKey("metrics")
                        ? tunnel["metrics"] as Dictionary<string, object> : null;
                    var http = metrics != null && metrics.ContainsKey("http")
                        ? metrics["http"] as Dictionary<string, object> : null;
                    if (http != null && http.ContainsKey("count"))
                    {
                        try
                        {
                            long count = Convert.ToInt64(http["count"]);
                            if (count >= 0) requestCount = count;
                        }
                        catch (InvalidCastException) { }
                        catch (FormatException) { }
                        catch (OverflowException) { }
                    }
                    return new TunnelSnapshot(url.TrimEnd('/'), requestCount);
                }
            }
            return null;
        }

        internal static bool IsBridgeResponse(int code)
        {
            // A PSC/Bloomberg 503 still proves the tunnel reached the Flask app.
            return code == 200 || code == 500 || code == 503;
        }

        internal static bool IsTunnelUrlSynchronized(string json, string url)
        {
            if (String.IsNullOrWhiteSpace(json) || String.IsNullOrWhiteSpace(url))
                return false;
            try
            {
                var status = new JavaScriptSerializer().DeserializeObject(
                    json.TrimStart('\uFEFF', ' ', '\r', '\n', '\t'))
                    as Dictionary<string, object>;
                return status != null && status.ContainsKey("url")
                    && status.ContainsKey("updated")
                    && String.Equals(Convert.ToString(status["url"]).TrimEnd('/'),
                        url.TrimEnd('/'), StringComparison.OrdinalIgnoreCase)
                    && status["updated"] is bool && (bool)status["updated"];
            }
            catch (ArgumentException) { return false; }
        }

        internal static int RetrySeconds(int attempts)
        {
            if (attempts <= 0) return 0;
            if (attempts == 1) return 5;
            if (attempts == 2) return 15;
            return 60;
        }

        internal static void SelfTest()
        {
            Assert(ServicePointManager.SecurityProtocol == SecurityProtocolType.Tls12,
                "Use TLS 1.2 for public tunnel checks");
            using (var testIcon = TrayIconFactory.Create(Color.FromArgb(48, 211, 177)))
                Assert(testIcon.Width == 32 && testIcon.Height == 32,
                    "Create a 32-pixel tray icon");
            string tunnels = "{\"tunnels\":["
                + "{\"public_url\":\"http://wrong\",\"config\":{\"addr\":\"localhost:5000\"}},"
                + "{\"public_url\":\"https://wrong.example\",\"config\":{\"addr\":\"localhost:4000\"}},"
                + "{\"public_url\":\"https://bridge.example/\",\"config\":{\"addr\":\"http://localhost:5000\"},"
                + "\"metrics\":{\"http\":{\"count\":42}}}]}";
            var selected = FindTunnel(tunnels);
            Assert(selected != null && selected.Url == "https://bridge.example"
                && selected.RequestCount == 42, "Select the HTTPS port-5000 tunnel and request count");
            Assert(FindTunnel("not-json") == null, "Reject invalid tunnel JSON");
            Assert(FindTunnel("{\"tunnels\":[{\"public_url\":\"https://bridge.example\","
                + "\"config\":{\"addr\":\"localhost:5000\"}}]}").RequestCount == null,
                "Allow an unavailable request counter");
            var start = new DateTime(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
            var activity = new TunnelRequestActivity();
            Assert(activity.Observe(start, 10)
                && activity.Snapshot(start).Total == 0,
                "Seed the first counter without a spike");
            Assert(activity.Observe(start.AddMinutes(1), 16), "Count new tunnel requests");
            Assert(activity.Observe(start.AddMinutes(2), 2), "Count requests after a counter reset");
            Assert(activity.Snapshot(start.AddMinutes(3)).Counts[47] == 8,
                "Bucket requests into five-minute intervals");
            var restored = TunnelRequestActivity.FromJson(activity.ToJson(), start.AddMinutes(3));
            Assert(restored.Snapshot(start.AddMinutes(3)).Counts[47] == 8,
                "Restore the four-hour request history");
            Assert(restored.Snapshot(start.AddMinutes(3)).TrackingSinceUtcTicks == start.Ticks,
                "Preserve when request tracking began");
            Assert(!restored.Observe(start.AddMinutes(4), 4),
                "Do not double-count after tray restart");
            Assert(restored.Observe(start.AddMinutes(6), 7)
                && restored.Snapshot(start.AddMinutes(6)).Counts[47] == 3
                && restored.Snapshot(start.AddMinutes(6)).Counts[46] == 8,
                "Move request counts into the next five-minute bucket");
            Assert(restored.Snapshot(start.AddHours(4).AddMinutes(5)).Total == 0,
                "Prune activity outside the four-hour window");
            Assert(TunnelRequestActivity.FromJson("not-json", start).Snapshot(start).Total == 0,
                "Ignore a corrupt request history file");
            Assert(!IsBridgeResponse(401) && IsBridgeResponse(503), "Classify bridge response");
            Assert(IsTunnelUrlSynchronized(
                "\uFEFF{\"url\":\"https://bridge.example\",\"updated\":true}",
                "https://bridge.example/"), "Recognize synchronized URL");
            Assert(!IsTunnelUrlSynchronized(
                "{\"url\":\"https://bridge.example\",\"updated\":false}",
                "https://bridge.example"), "Reject failed secret sync");
            Assert(RetrySeconds(1) == 5 && RetrySeconds(2) == 15
                && RetrySeconds(20) == 60, "Bounded retry backoff");
            var clock = Stopwatch.StartNew();
            using (OwnedProcess child = OwnedProcess.Start("powershell.exe",
                "-NoProfile -NonInteractive -Command Start-Sleep -Seconds 30",
                Environment.CurrentDirectory, delegate(string ignored) { }))
            {
                Thread.Sleep(200);
            }
            Assert(clock.Elapsed.TotalSeconds < 6, "Stopping a child must not wait for its sleep");
            using (var form = new StatusForm("bridge-tray.log",
                delegate { }, delegate { }))
            {
                IntPtr handle = form.Handle;
                form.SetStatus("PSC ready");
                form.AllowClose();
            }
        }

        private static void Assert(bool condition, string message)
        {
            if (!condition) throw new InvalidOperationException(message);
        }
    }
}
