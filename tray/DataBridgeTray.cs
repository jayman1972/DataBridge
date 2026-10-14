using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
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
            if (args.Length == 1 && args[0] == "--self-test")
            {
                try { BridgeTrayLogic.SelfTest(); return 0; }
                catch { return 1; }
            }
            bool created;
            using (var mutex = new Mutex(true, "Local\\EHPDataBridgeTray", out created))
            {
                if (!created) return 0;
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                Application.Run(new BridgeTrayContext());
                GC.KeepAlive(mutex);
            }
            return 0;
        }
    }

    internal sealed class BridgeTrayContext : ApplicationContext
    {
        private readonly string repository;
        private readonly string logDirectory;
        private readonly string logPath;
        private readonly string tunnelStatusPath;
        private readonly object logLock = new object();
        private readonly object processLock = new object();
        private readonly StatusForm form;
        private readonly NotifyIcon icon;
        private readonly System.Threading.Timer timer;
        private OwnedProcess bridge;
        private OwnedProcess tunnel;
        private int polling;
        private int failures;
        private int tunnelMissing;
        private bool alerted;
        private bool exiting;
        private DateTime startedAt;
        private DateTime nextBridgeStart;
        private DateTime nextTunnelStart;
        private int bridgeAttempts;
        private int tunnelAttempts;

        internal BridgeTrayContext()
        {
            repository = Path.GetFullPath(Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory, "..", ".."));
            logDirectory = Path.Combine(Environment.GetFolderPath(
                Environment.SpecialFolder.LocalApplicationData), "EHP", "DataBridgeTray");
            Directory.CreateDirectory(logDirectory);
            logPath = Path.Combine(logDirectory, "bridge-tray.log");
            tunnelStatusPath = Path.Combine(logDirectory, "tunnel-sync.json");
            RotateLog();
            startedAt = DateTime.UtcNow;
            form = new StatusForm(logPath, Restart, ExitApp);
            IntPtr unused = form.Handle;
            icon = new NotifyIcon();
            icon.Icon = SystemIcons.Application;
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

        private string TunnelUrl()
        {
            string body;
            if (HttpCode("http://127.0.0.1:4040/api/tunnels", out body) != 200)
                return null;
            return BridgeTrayLogic.FindTunnelUrl(body);
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
                string url = TunnelUrl();
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
                    form.BeginInvoke((MethodInvoker)delegate { UpdateUi(text, attention); });
            }
            catch (Exception error)
            {
                Log("Monitor error: " + error);
            }
            finally { Interlocked.Exchange(ref polling, 0); }
        }

        private void UpdateUi(string text, bool attention)
        {
            if (exiting) return;
            form.SetStatus(text);
            icon.Text = attention ? "EHP DataBridge needs attention" : "EHP DataBridge running";
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
            ExitThread();
        }
    }

    internal sealed class StatusForm : Form
    {
        private readonly TextBox status;
        private bool allowClose;

        internal StatusForm(string logPath, Action restart, Action exit)
        {
            Text = "EHP DataBridge";
            Width = 600;
            Height = 330;
            StartPosition = FormStartPosition.CenterScreen;
            ShowInTaskbar = true;
            var label = new Label { Text = "PSC / Bloomberg / tunnel status",
                Left = 16, Top = 15, Width = 540, Height = 25,
                Font = new Font(SystemFonts.DefaultFont, FontStyle.Bold) };
            status = new TextBox { Left = 16, Top = 45, Width = 550, Height = 180,
                Multiline = true, ReadOnly = true, ScrollBars = ScrollBars.Vertical,
                Text = "Starting..." };
            var retry = new Button { Text = "Restart", Left = 16, Top = 237, Width = 100 };
            retry.Click += delegate { restart(); };
            var logs = new Button { Text = "Open logs", Left = 130, Top = 237, Width = 100 };
            logs.Click += delegate { Process.Start("explorer.exe",
                "\"" + Path.GetDirectoryName(logPath) + "\""); };
            var hide = new Button { Text = "Hide", Left = 345, Top = 237, Width = 100 };
            hide.Click += delegate { Hide(); };
            var quit = new Button { Text = "Exit", Left = 460, Top = 237, Width = 100 };
            quit.Click += delegate { exit(); };
            Controls.Add(label);
            Controls.Add(status);
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
        internal void AllowClose() { allowClose = true; }
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
