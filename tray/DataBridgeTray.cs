using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Windows.Forms;

namespace Ehp.DataBridge.Tray
{
    internal static class Program
    {
        [STAThread]
        private static int Main(string[] args)
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12;
            if (args.Length == 1 && args[0] == "--self-test")
            {
                try { BridgeTrayLogic.SelfTest(); return 0; }
                catch { return 1; }
            }
            bool created;
            using (var showStatus = new EventWaitHandle(false, EventResetMode.AutoReset,
                "Local\\EHPDataBridgeTrayShowStatus"))
            using (var mutex = new Mutex(true, "Local\\EHPDataBridgeTray", out created))
            {
                if (!created)
                {
                    showStatus.Set();
                    return 0;
                }
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new BridgeTrayContext(showStatus));
                GC.KeepAlive(mutex);
            }
            return 0;
        }
    }

    internal static class TrayIconFactory
    {
        [DllImport("user32.dll", SetLastError = true)]
        private static extern bool DestroyIcon(IntPtr handle);

        internal static Icon Create(Color accent)
        {
            using (var bitmap = new Bitmap(32, 32))
            using (var graphics = Graphics.FromImage(bitmap))
            {
                graphics.Clear(Color.Transparent);
                graphics.SmoothingMode = SmoothingMode.AntiAlias;
                using (var background = new SolidBrush(Color.FromArgb(29, 40, 47)))
                    graphics.FillEllipse(background, 1, 1, 30, 30);
                using (var rim = new Pen(Color.FromArgb(110, 135, 145), 1.5f))
                    graphics.DrawEllipse(rim, 1.5f, 1.5f, 29, 29);
                using (var span = new Pen(accent, 4f))
                {
                    span.StartCap = LineCap.Round;
                    span.EndCap = LineCap.Round;
                    graphics.DrawBezier(span, new PointF(7, 21), new PointF(11, 9),
                        new PointF(21, 9), new PointF(25, 21));
                }
                using (var deck = new Pen(Color.White, 2.5f))
                {
                    deck.StartCap = LineCap.Round;
                    deck.EndCap = LineCap.Round;
                    graphics.DrawLine(deck, 7, 23, 25, 23);
                }
                using (var node = new SolidBrush(Color.White))
                    graphics.FillEllipse(node, 13.5f, 9.5f, 5, 5);

                IntPtr handle = bitmap.GetHicon();
                try
                {
                    using (var borrowed = Icon.FromHandle(handle))
                        return (Icon)borrowed.Clone();
                }
                finally { DestroyIcon(handle); }
            }
        }
    }

    internal sealed class BridgeTrayContext : ApplicationContext
    {
        private readonly string repository;
        private readonly string logDirectory;
        private readonly string logPath;
        private readonly string tunnelStatusPath;
        private readonly string activityPath;
        private readonly object logLock = new object();
        private readonly object processLock = new object();
        private readonly TunnelRequestActivity requestActivity;
        private readonly StatusForm form;
        private readonly NotifyIcon icon;
        private readonly Icon checkingIcon;
        private readonly Icon healthyIcon;
        private readonly Icon attentionIcon;
        private readonly System.Threading.Timer timer;
        private readonly RegisteredWaitHandle showStatusRegistration;
        private Icon currentIcon;
        private OwnedProcess bridge;
        private OwnedProcess tunnel;
        private int polling;
        private int failures;
        private int tunnelMissing;
        private bool alerted;
        private bool exiting;
        private bool activitySaveFailed;
        private DateTime startedAt;
        private DateTime nextBridgeStart;
        private DateTime nextTunnelStart;
        private int bridgeAttempts;
        private int tunnelAttempts;

        internal BridgeTrayContext(EventWaitHandle showStatus)
        {
            repository = Path.GetFullPath(Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, "..", ".."));
            logDirectory = Path.Combine(Environment.GetFolderPath(
                Environment.SpecialFolder.LocalApplicationData), "EHP", "DataBridgeTray");
            Directory.CreateDirectory(logDirectory);
            logPath = Path.Combine(logDirectory, "bridge-tray.log");
            tunnelStatusPath = Path.Combine(logDirectory, "tunnel-sync.json");
            activityPath = Path.Combine(logDirectory, "tunnel-requests.json");
            RotateLog();
            try
            {
                requestActivity = File.Exists(activityPath)
                    ? TunnelRequestActivity.FromJson(File.ReadAllText(activityPath), DateTime.UtcNow)
                    : new TunnelRequestActivity();
            }
            catch (IOException error)
            {
                requestActivity = new TunnelRequestActivity();
                Log("Could not load tunnel request history: " + error.Message);
            }
            catch (UnauthorizedAccessException error)
            {
                requestActivity = new TunnelRequestActivity();
                Log("Could not load tunnel request history: " + error.Message);
            }
            startedAt = DateTime.UtcNow;
            form = new StatusForm(logPath, Restart, ExitApp);
            IntPtr unused = form.Handle;
            checkingIcon = TrayIconFactory.Create(Color.FromArgb(255, 187, 72));
            healthyIcon = TrayIconFactory.Create(Color.FromArgb(48, 211, 177));
            attentionIcon = TrayIconFactory.Create(Color.FromArgb(255, 104, 99));
            icon = new NotifyIcon();
            currentIcon = checkingIcon;
            icon.Icon = currentIcon;
            form.Icon = currentIcon;
            icon.Text = "EHP DataBridge starting";
            icon.Visible = true;
            var menu = new ContextMenuStrip();
            menu.Items.Add("Show status", null, delegate { ShowStatus(); });
            menu.Items.Add("Restart bridge and tunnel", null, delegate { Restart(); });
            menu.Items.Add("Open logs", null, delegate { OpenLogs(); });
            menu.Items.Add("Exit", null, delegate { ExitApp(); });
            icon.ContextMenuStrip = menu;
            icon.DoubleClick += delegate { ShowStatus(); };
            Log("Tray started. Repository=" + repository);
            showStatusRegistration = ThreadPool.RegisterWaitForSingleObject(showStatus,
                delegate(object state, bool timedOut)
                {
                    if (exiting) return;
                    try { form.BeginInvoke(new MethodInvoker(ShowStatus)); }
                    catch (InvalidOperationException) { }
                }, null, Timeout.Infinite, false);
            timer = new System.Threading.Timer(Poll, null, 0, 15000);
        }

        private void RotateLog()
        {
            if (!File.Exists(logPath) || new FileInfo(logPath).Length < 10 * 1024 * 1024)
                return;
            string previous = logPath + ".1";
            if (File.Exists(previous)) File.Delete(previous);
            File.Move(logPath, previous);
        }

        private void Log(string message)
        {
            lock (logLock)
            {
                File.AppendAllText(logPath,
                    DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " " + message + Environment.NewLine);
            }
        }

        private static bool PortOpen(int port)
        {
            using (var client = new TcpClient())
            {
                try
                {
                    IAsyncResult pending = client.BeginConnect("127.0.0.1", port, null, null);
                    if (!pending.AsyncWaitHandle.WaitOne(750)) return false;
                    client.EndConnect(pending);
                    return true;
                }
                catch (SocketException) { return false; }
                catch (ObjectDisposedException) { return false; }
            }
        }

        private int HttpCode(string url, out string body)
        {
            body = "";
            try
            {
                var request = (HttpWebRequest)WebRequest.Create(url);
                request.Method = "GET";
                request.Proxy = null;
                request.Timeout = 4000;
                request.ReadWriteTimeout = 4000;
                request.Headers["ngrok-skip-browser-warning"] = "1";
                using (var response = (HttpWebResponse)request.GetResponse())
                using (var reader = new StreamReader(response.GetResponseStream()))
                {
                    body = reader.ReadToEnd();
                    return (int)response.StatusCode;
                }
            }
            catch (WebException error)
            {
                var response = error.Response as HttpWebResponse;
                if (response == null) return 0;
                using (response) return (int)response.StatusCode;
            }
            catch (UriFormatException) { return 0; }
        }

        private TunnelSnapshot ReadTunnel()
        {
            string body;
            if (HttpCode("http://127.0.0.1:4040/api/tunnels", out body) != 200)
                return null;
            return BridgeTrayLogic.FindTunnel(body);
        }

        private void SaveRequestActivity()
        {
            string temporary = activityPath + ".tmp";
            try
            {
                File.WriteAllText(temporary, requestActivity.ToJson());
                if (File.Exists(activityPath)) File.Replace(temporary, activityPath, null);
                else File.Move(temporary, activityPath);
                activitySaveFailed = false;
            }
            catch (IOException error) { LogActivitySaveError(error); }
            catch (UnauthorizedAccessException error) { LogActivitySaveError(error); }
        }

        private void LogActivitySaveError(Exception error)
        {
            if (!activitySaveFailed) Log("Could not save tunnel request history: " + error.Message);
            activitySaveFailed = true;
        }

        private void StartBridge(List<string> issues)
        {
            lock (processLock)
            {
                if (bridge != null && !bridge.HasExited) return;
                if (bridge != null) { bridge.Dispose(); bridge = null; }
                if (DateTime.UtcNow < nextBridgeStart) return;
                if (PortOpen(5000))
                {
                    issues.Add("Another process already owns port 5000. Close the manual bridge window.");
                    return;
                }
                try
                {
                    string python = Environment.GetEnvironmentVariable("DATA_BRIDGE_PYTHON_EXE");
                    if (String.IsNullOrWhiteSpace(python)) python = "python.exe";
                    bridge = OwnedProcess.Start(python, "data_bridge_runtime.py", repository,
                        delegate(string line) { Log("[bridge] " + line); });
                    bridgeAttempts = 0;
                    Log("Started DataBridge PID=" + bridge.Id);
                }
                catch (Exception error)
                {
                    bridgeAttempts++;
                    nextBridgeStart = DateTime.UtcNow.AddSeconds(
                        BridgeTrayLogic.RetrySeconds(bridgeAttempts));
                    issues.Add("Could not start Python bridge: " + error.Message);
                    Log("Bridge start failed: " + error);
                }
            }
        }

        private void StartTunnel(List<string> issues)
        {
            lock (processLock)
            {
                if (tunnel != null && !tunnel.HasExited) return;
                if (tunnel != null) { tunnel.Dispose(); tunnel = null; }
                if (DateTime.UtcNow < nextTunnelStart) return;
                if (bridge == null || bridge.HasExited || !PortOpen(5000))
                {
                    issues.Add("Tunnel is waiting for the supervised bridge to listen on port 5000.");
                    return;
                }
                if (PortOpen(4040))
                {
                    issues.Add("Another ngrok agent is running. Close the manual tunnel window.");
                    return;
                }
                try
                {
                    string script = Path.Combine(repository, "start-tunnel-ngrok.ps1");
                    if (File.Exists(tunnelStatusPath)) File.Delete(tunnelStatusPath);
                    tunnel = OwnedProcess.Start("powershell.exe",
                        "-NoProfile -NonInteractive -ExecutionPolicy Bypass -File \"" + script
                        + "\" -StatusFile \"" + tunnelStatusPath + "\"",
                        repository, delegate(string line) { Log("[tunnel] " + line); });
                    tunnelAttempts = 0;
                    Log("Started tunnel manager PID=" + tunnel.Id);
                }
                catch (Exception error)
                {
                    tunnelAttempts++;
                    nextTunnelStart = DateTime.UtcNow.AddSeconds(
                        BridgeTrayLogic.RetrySeconds(tunnelAttempts));
                    issues.Add("Could not start tunnel manager: " + error.Message);
                    Log("Tunnel start failed: " + error);
                }
            }
        }

        private void Poll(object state)
        {
            if (exiting || Interlocked.Exchange(ref polling, 1) != 0) return;
            try
            {
                var issues = new List<string>();
                StartBridge(issues);
                StartTunnel(issues);
                string body;
                int psc = HttpCode("http://127.0.0.1:5000/sggg/health", out body);
                int bloomberg = HttpCode("http://127.0.0.1:5000/health", out body);
                TunnelSnapshot tunnelSnapshot = ReadTunnel();
                string url = tunnelSnapshot == null ? null : tunnelSnapshot.Url;
                DateTime sampledAt = DateTime.UtcNow;
                if (tunnelSnapshot != null && tunnelSnapshot.RequestCount.HasValue
                    && requestActivity.Observe(sampledAt, tunnelSnapshot.RequestCount.Value))
                    SaveRequestActivity();
                RequestActivitySeries activitySeries = requestActivity.Snapshot(sampledAt);
                int publicCode = url == null ? 0 : HttpCode(url + "/sggg/health", out body);
                bool synchronized = false;
                if (url != null && File.Exists(tunnelStatusPath))
                {
                    try
                    {
                        synchronized = BridgeTrayLogic.IsTunnelUrlSynchronized(
                            File.ReadAllText(tunnelStatusPath), url);
                    }
                    catch (IOException) { }
                }
                if (psc != 200) issues.Add("PSC/VPN is unavailable (local HTTP " + psc + ").");
                if (bloomberg != 200) issues.Add("Bloomberg is unavailable (local HTTP " + bloomberg + ").");
                if (url == null || !BridgeTrayLogic.IsBridgeResponse(publicCode))
                    issues.Add("The public tunnel is unavailable.");
                else if (!synchronized)
                    issues.Add("The public tunnel URL has not been synchronized to all configured destinations.");

                bool bridgeRunning;
                lock (processLock)
                {
                    bridgeRunning = bridge != null && !bridge.HasExited;
                    if (tunnel != null && !tunnel.HasExited && url == null)
                    {
                        tunnelMissing++;
                        if (tunnelMissing >= 4)
                        {
                            Log("Tunnel missing for four checks; restarting the tunnel manager.");
                            tunnel.Dispose();
                            tunnel = null;
                            tunnelMissing = 0;
                            nextTunnelStart = DateTime.UtcNow.AddSeconds(5);
                        }
                    }
                    else tunnelMissing = 0;
                }

                if (issues.Count == 0) failures = 0;
                else failures++;
                bool attention = failures >= 2
                    && DateTime.UtcNow.Subtract(startedAt).TotalSeconds >= 30;
                string text = issues.Count == 0
                    ? "All connections are healthy."
                    : String.Join(Environment.NewLine, issues.ToArray());
                text += Environment.NewLine + Environment.NewLine
                    + "Bridge: " + (bridgeRunning ? "running" : "not owned")
                    + " | PSC: " + psc + " | Bloomberg: " + bloomberg
                    + Environment.NewLine + "Tunnel: " + (url ?? "not found")
                    + " | Public HTTP: " + publicCode
                    + " | URL sync: " + (synchronized ? "complete" : "pending/failed");
                if (form.IsHandleCreated && !form.IsDisposed)
                    form.BeginInvoke((MethodInvoker)delegate {
                        UpdateUi(text, attention, issues.Count == 0, activitySeries);
                    });
            }
            catch (Exception error)
            {
                Log("Monitor error: " + error);
            }
            finally { Interlocked.Exchange(ref polling, 0); }
        }

        private void UpdateUi(string text, bool attention, bool healthy,
            RequestActivitySeries activitySeries)
        {
            if (exiting) return;
            form.SetStatus(text);
            form.SetActivity(activitySeries);
            Icon nextIcon = attention ? attentionIcon : healthy ? healthyIcon : checkingIcon;
            if (currentIcon != nextIcon)
            {
                currentIcon = nextIcon;
                icon.Icon = currentIcon;
                form.Icon = currentIcon;
            }
            icon.Text = attention ? "EHP DataBridge needs attention"
                : healthy ? "EHP DataBridge healthy" : "EHP DataBridge checking connections";
            if (attention && !alerted)
            {
                alerted = true;
                icon.BalloonTipTitle = "DataBridge needs attention";
                icon.BalloonTipText = "Open the status window to see which connection is down.";
                icon.ShowBalloonTip(10000);
                ShowStatus();
            }
            if (!attention) alerted = false;
        }

        private void ShowStatus()
        {
            if (form.IsDisposed) return;
            form.Show();
            form.WindowState = FormWindowState.Normal;
            form.BringToFront();
            form.Activate();
        }

        private void OpenLogs()
        {
            Process.Start("explorer.exe", "\"" + logDirectory + "\"");
        }

        private void Restart()
        {
            Log("User requested restart.");
            lock (processLock)
            {
                if (bridge != null) { bridge.Dispose(); bridge = null; }
                if (tunnel != null) { tunnel.Dispose(); tunnel = null; }
                nextBridgeStart = DateTime.MinValue;
                nextTunnelStart = DateTime.MinValue;
            }
            failures = 0;
            alerted = false;
            startedAt = DateTime.UtcNow;
            form.SetStatus("Restarting bridge and tunnel...");
            ThreadPool.QueueUserWorkItem(Poll);
        }

        private void ExitApp()
        {
            exiting = true;
            showStatusRegistration.Unregister(null);
            timer.Dispose();
            lock (processLock)
            {
                if (bridge != null) bridge.Dispose();
                if (tunnel != null) tunnel.Dispose();
            }
            Log("Tray exited; owned bridge and tunnel processes stopped.");
            icon.Visible = false;
            icon.Dispose();
            form.AllowClose();
            form.Close();
            checkingIcon.Dispose();
            healthyIcon.Dispose();
            attentionIcon.Dispose();
            ExitThread();
        }
    }

    internal sealed class StatusForm : Form
    {
        private readonly TextBox status;
        private readonly Label activityTotal;
        private readonly RequestActivityChart activityChart;
        private bool allowClose;

        internal StatusForm(string logPath, Action restart, Action exit)
        {
            Text = "EHP DataBridge";
            ClientSize = new Size(700, 470);
            MinimumSize = new Size(620, 460);
            StartPosition = FormStartPosition.CenterScreen;
            ShowInTaskbar = true;
            var label = new Label { Text = "PSC / Bloomberg / tunnel status",
                Left = 16, Top = 15, Width = 668, Height = 25,
                Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right,
                Font = new Font(SystemFonts.DefaultFont, FontStyle.Bold) };
            status = new TextBox { Left = 16, Top = 45, Width = 668, Height = 124,
                Anchor = AnchorStyles.Top | AnchorStyles.Left | AnchorStyles.Right,
                Multiline = true, ReadOnly = true, TabStop = false,
                ScrollBars = ScrollBars.Vertical,
                Text = "Starting..." };
            var activityTitle = new Label {
                Text = "Tunnel requests (health checks included)",
                Left = 16, Top = 185, Width = 350, Height = 22,
                Font = new Font(SystemFonts.DefaultFont, FontStyle.Bold)
            };
            activityTotal = new Label {
                Text = "0 in 4h", Left = 470, Top = 185, Width = 214, Height = 22,
                Anchor = AnchorStyles.Top | AnchorStyles.Right,
                TextAlign = ContentAlignment.MiddleRight
            };
            activityChart = new RequestActivityChart {
                Left = 16, Top = 212, Width = 668, Height = 178,
                Anchor = AnchorStyles.Top | AnchorStyles.Bottom
                    | AnchorStyles.Left | AnchorStyles.Right
            };
            var retry = new Button { Text = "Restart", Left = 16, Top = 426, Width = 100,
                Anchor = AnchorStyles.Bottom | AnchorStyles.Left };
            retry.Click += delegate { restart(); };
            var logs = new Button { Text = "Open logs", Left = 130, Top = 426, Width = 100,
                Anchor = AnchorStyles.Bottom | AnchorStyles.Left };
            logs.Click += delegate { Process.Start("explorer.exe",
                "\"" + Path.GetDirectoryName(logPath) + "\""); };
            var hide = new Button { Text = "Hide", Left = 470, Top = 426, Width = 100,
                Anchor = AnchorStyles.Bottom | AnchorStyles.Right };
            hide.Click += delegate { Hide(); };
            var quit = new Button { Text = "Exit", Left = 584, Top = 426, Width = 100,
                Anchor = AnchorStyles.Bottom | AnchorStyles.Right };
            quit.Click += delegate { exit(); };
            Controls.Add(label);
            Controls.Add(status);
            Controls.Add(activityTitle);
            Controls.Add(activityTotal);
            Controls.Add(activityChart);
            Controls.Add(retry);
            Controls.Add(logs);
            Controls.Add(hide);
            Controls.Add(quit);
            FormClosing += delegate(object sender, FormClosingEventArgs args)
            {
                if (!allowClose) { args.Cancel = true; Hide(); }
            };
        }

        internal void SetStatus(string value) { status.Text = value; }
        internal void SetActivity(RequestActivitySeries series)
        {
            if (series.TrackingSinceUtcTicks == 0)
                activityTotal.Text = "Waiting for data";
            else if (DateTime.UtcNow.Ticks - series.TrackingSinceUtcTicks
                < TimeSpan.TicksPerHour * 4)
                activityTotal.Text = series.Total.ToString("N0") + " since "
                    + new DateTime(series.TrackingSinceUtcTicks, DateTimeKind.Utc)
                        .ToLocalTime().ToString("h:mm tt");
            else activityTotal.Text = series.Total.ToString("N0") + " in 4h";
            activityChart.SetSeries(series);
        }
        internal void AllowClose() { allowClose = true; }
    }

    internal sealed class RequestActivityChart : Control
    {
        private readonly ToolTip tooltip = new ToolTip();
        private RequestActivitySeries series;
        private int hovered = -1;

        internal RequestActivityChart()
        {
            SetStyle(ControlStyles.AllPaintingInWmPaint | ControlStyles.OptimizedDoubleBuffer
                | ControlStyles.ResizeRedraw | ControlStyles.UserPaint, true);
            BackColor = SystemColors.Control;
            AccessibleName = "Tunnel requests over the last four hours";
        }

        internal void SetSeries(RequestActivitySeries value)
        {
            series = value;
            Invalidate();
        }

        private Rectangle PlotBounds()
        {
            return new Rectangle(40, 10, Math.Max(0, Width - 54), Math.Max(0, Height - 40));
        }

        protected override void OnPaint(PaintEventArgs e)
        {
            base.OnPaint(e);
            var plot = PlotBounds();
            if (plot.Width < 48 || plot.Height < 20) return;
            if (series == null || series.TrackingSinceUtcTicks == 0)
            {
                using (var untracked = new SolidBrush(Color.FromArgb(233, 237, 239)))
                    e.Graphics.FillRectangle(untracked, plot);
                using (var muted = new SolidBrush(Color.FromArgb(91, 104, 111)))
                {
                    const string waiting = "Waiting for tunnel data";
                    SizeF size = e.Graphics.MeasureString(waiting, Font);
                    e.Graphics.DrawString(waiting, Font, muted,
                        plot.Left + (plot.Width - size.Width) / 2,
                        plot.Top + (plot.Height - size.Height) / 2);
                }
                return;
            }
            long maximum = 1;
            foreach (long count in series.Counts)
                if (count > maximum) maximum = count;
            using (var grid = new Pen(Color.FromArgb(211, 219, 222)))
            using (var muted = new SolidBrush(Color.FromArgb(91, 104, 111)))
            using (var untracked = new SolidBrush(Color.FromArgb(233, 237, 239)))
            using (var bars = new SolidBrush(Color.FromArgb(45, 166, 143)))
            using (var highlight = new SolidBrush(Color.FromArgb(20, 119, 108)))
            {
                double untrackedBuckets = (series.TrackingSinceUtcTicks
                    - series.FirstBucketUtcTicks)
                    / (double)(TimeSpan.TicksPerMinute * TunnelRequestActivity.BucketMinutes);
                float untrackedWidth = (float)Math.Max(0,
                    Math.Min(plot.Width, untrackedBuckets * plot.Width
                        / TunnelRequestActivity.BucketCount));
                e.Graphics.FillRectangle(untracked, plot.Left, plot.Top,
                    untrackedWidth, plot.Height);
                for (int row = 0; row <= 2; row++)
                {
                    float y = plot.Top + row * plot.Height / 2f;
                    e.Graphics.DrawLine(grid, plot.Left, y, plot.Right, y);
                }
                string peak = maximum.ToString("N0");
                SizeF peakSize = e.Graphics.MeasureString(peak, Font);
                e.Graphics.DrawString(peak, Font, muted,
                    plot.Left - peakSize.Width - 5, plot.Top - 3);
                e.Graphics.DrawString("0", Font, muted,
                    plot.Left - 14, plot.Bottom - Font.Height + 3);
                float slot = plot.Width / (float)TunnelRequestActivity.BucketCount;
                for (int i = 0; i < series.Counts.Length; i++)
                {
                    if (series.Counts[i] == 0) continue;
                    float height = Math.Max(2f,
                        (float)(series.Counts[i] / (double)maximum * plot.Height));
                    e.Graphics.FillRectangle(i == hovered ? highlight : bars,
                        plot.Left + i * slot + 1, plot.Bottom - height,
                        Math.Max(2f, slot - 2), height);
                }
                string[] labels = { "4h ago", "3h", "2h", "1h", "now" };
                for (int i = 0; i < labels.Length; i++)
                {
                    SizeF size = e.Graphics.MeasureString(labels[i], Font);
                    float center = plot.Left + i * plot.Width / 4f;
                    float x = Math.Max(0, Math.Min(Width - size.Width, center - size.Width / 2));
                    e.Graphics.DrawString(labels[i], Font, muted, x, plot.Bottom + 5);
                }
            }
        }

        protected override void OnMouseMove(MouseEventArgs e)
        {
            base.OnMouseMove(e);
            var plot = PlotBounds();
            int index = e.X >= plot.Left && e.X < plot.Right
                && e.Y >= plot.Top && e.Y <= plot.Bottom && plot.Width > 0
                ? (int)((e.X - plot.Left) * (long)TunnelRequestActivity.BucketCount / plot.Width)
                : -1;
            if (index == hovered) return;
            hovered = index;
            if (index >= 0 && series != null)
            {
                var start = new DateTime(series.FirstBucketUtcTicks
                    + index * TimeSpan.TicksPerMinute * TunnelRequestActivity.BucketMinutes,
                    DateTimeKind.Utc).ToLocalTime();
                long bucketEndUtcTicks = series.FirstBucketUtcTicks
                    + (index + 1) * TimeSpan.TicksPerMinute
                        * TunnelRequestActivity.BucketMinutes;
                string value = series.TrackingSinceUtcTicks == 0
                    || series.TrackingSinceUtcTicks >= bucketEndUtcTicks
                    ? "not tracked" : series.Counts[index].ToString("N0") + " requests";
                tooltip.SetToolTip(this, start.ToString("MMM d, h:mm tt") + " - "
                    + start.AddMinutes(TunnelRequestActivity.BucketMinutes).ToString("h:mm tt")
                    + ": " + value);
            }
            else tooltip.SetToolTip(this, "");
            Invalidate();
        }

        protected override void OnMouseLeave(EventArgs e)
        {
            base.OnMouseLeave(e);
            hovered = -1;
            tooltip.SetToolTip(this, "");
            Invalidate();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing) tooltip.Dispose();
            base.Dispose(disposing);
        }
    }

    internal sealed class OwnedProcess : IDisposable
    {
        private readonly Process process;
        private IntPtr job;

        private OwnedProcess(Process process, IntPtr job)
        {
            this.process = process;
            this.job = job;
        }

        internal int Id { get { return process.Id; } }
        internal bool HasExited { get { return process.HasExited; } }

        internal static OwnedProcess Start(string executable, string arguments,
            string workingDirectory, Action<string> output)
        {
            var start = new ProcessStartInfo(executable, arguments);
            start.WorkingDirectory = workingDirectory;
            start.UseShellExecute = false;
            start.CreateNoWindow = true;
            start.WindowStyle = ProcessWindowStyle.Hidden;
            start.RedirectStandardOutput = true;
            start.RedirectStandardError = true;
            var process = new Process { StartInfo = start };
            IntPtr job = NativeJob.CreateKillOnClose();
            try
            {
                process.OutputDataReceived += delegate(object sender, DataReceivedEventArgs eventArgs)
                { if (eventArgs.Data != null) output(eventArgs.Data); };
                process.ErrorDataReceived += delegate(object sender, DataReceivedEventArgs eventArgs)
                { if (eventArgs.Data != null) output("ERROR " + eventArgs.Data); };
                if (!process.Start()) throw new InvalidOperationException("Process did not start.");
                if (!NativeJob.AssignProcessToJobObject(job, process.Handle))
                    throw new InvalidOperationException("Could not supervise the child process tree.");
                process.BeginOutputReadLine();
                process.BeginErrorReadLine();
                return new OwnedProcess(process, job);
            }
            catch
            {
                NativeJob.CloseHandle(job);
                if (!process.HasExited) process.Kill();
                process.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (job != IntPtr.Zero)
            {
                NativeJob.CloseHandle(job);
                job = IntPtr.Zero;
            }
            try { if (!process.HasExited) process.WaitForExit(3000); }
            catch (InvalidOperationException) { }
            process.Dispose();
        }
    }

    internal static class NativeJob
    {
        private const uint KillOnJobClose = 0x2000;

        [StructLayout(LayoutKind.Sequential)]
        private struct IoCounters
        {
            public ulong ReadOperationCount, WriteOperationCount, OtherOperationCount;
            public ulong ReadTransferCount, WriteTransferCount, OtherTransferCount;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct BasicLimit
        {
            public long PerProcessUserTimeLimit, PerJobUserTimeLimit;
            public uint LimitFlags;
            public UIntPtr MinimumWorkingSetSize, MaximumWorkingSetSize;
            public uint ActiveProcessLimit;
            public IntPtr Affinity;
            public uint PriorityClass, SchedulingClass;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct ExtendedLimit
        {
            public BasicLimit BasicLimitInformation;
            public IoCounters IoInfo;
            public UIntPtr ProcessMemoryLimit, JobMemoryLimit;
            public UIntPtr PeakProcessMemoryUsed, PeakJobMemoryUsed;
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
        private static extern IntPtr CreateJobObject(IntPtr attributes, string name);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool SetInformationJobObject(IntPtr job, int informationClass,
            ref ExtendedLimit information, uint length);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool CloseHandle(IntPtr handle);

        internal static IntPtr CreateKillOnClose()
        {
            IntPtr job = CreateJobObject(IntPtr.Zero, null);
            if (job == IntPtr.Zero) throw new InvalidOperationException("Could not create a process job.");
            var limits = new ExtendedLimit();
            limits.BasicLimitInformation.LimitFlags = KillOnJobClose;
            if (!SetInformationJobObject(job, 9, ref limits,
                (uint)Marshal.SizeOf(typeof(ExtendedLimit))))
            {
                CloseHandle(job);
                throw new InvalidOperationException("Could not configure process supervision.");
            }
            return job;
        }
    }
}
