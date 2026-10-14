using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Web.Script.Serialization;

namespace Ehp.DataBridge.Tray
{
    internal static class BridgeTrayLogic
    {
        internal static string FindTunnelUrl(string json)
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
                    return url.TrimEnd('/');
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
            string tunnels = "{\"tunnels\":["
                + "{\"public_url\":\"http://wrong\",\"config\":{\"addr\":\"localhost:5000\"}},"
                + "{\"public_url\":\"https://wrong.example\",\"config\":{\"addr\":\"localhost:4000\"}},"
                + "{\"public_url\":\"https://bridge.example/\",\"config\":{\"addr\":\"http://localhost:5000\"}}]}";
            Assert(FindTunnelUrl(tunnels) == "https://bridge.example", "Select the HTTPS port-5000 tunnel");
            Assert(FindTunnelUrl("not-json") == null, "Reject invalid tunnel JSON");
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
