using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace TransportR.Tests;

/// <summary>
/// A Redis for the state store tests: a container when Docker answers, which pins the version the
/// store is meant for, and otherwise a <c>redis-server</c> binary already on the machine, started on
/// a free port for the length of one test. Nothing is installed to make either available.
/// </summary>
/// <remarks>
/// The binary is whatever the machine has, so a test that passes here says less about the version
/// than one that passes in the container.
/// </remarks>
internal sealed class RedisServer : IAsyncDisposable
{
    private const string Image = "redis:7.0";

    private static readonly Lazy<bool> DockerAnswers = new(CheckDocker);
    private static readonly Lazy<string?> NativeBinary = new(FindBinary);

    private readonly RedisContainer? _container;
    private readonly Process? _process;
    private readonly string? _scratch;

    private RedisServer(string connectionString, RedisContainer? container, Process? process, string? scratch)
    {
        ConnectionString = connectionString;
        _container = container;
        _process = process;
        _scratch = scratch;
    }

    public string ConnectionString { get; }

    public static bool IsAvailable => DockerAnswers.Value || NativeBinary.Value != null;

    public static async Task<RedisServer> StartAsync()
    {
        if (DockerAnswers.Value)
        {
            var container = new RedisBuilder(Image).Build();
            await container.StartAsync();
            return new RedisServer(container.GetConnectionString(), container, null, null);
        }

        return await StartNativeAsync(NativeBinary.Value ?? throw new InvalidOperationException("Neither Docker nor a redis-server binary is available."));
    }

    private static async Task<RedisServer> StartNativeAsync(string binary)
    {
        var port = FreePort();
        var scratch = Directory.CreateTempSubdirectory("transportr-redis-").FullName;

        // Nothing is written outside the scratch directory, and nothing outlives the test.
        var start = new ProcessStartInfo(binary)
        {
            UseShellExecute = false,
            CreateNoWindow = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        foreach (var argument in new[]
                 {
                     "--port", port.ToString(), "--bind", "127.0.0.1", "--protected-mode", "no",
                     "--save", "", "--appendonly", "no", "--dir", scratch, "--logfile", Path.Combine(scratch, "redis.log"),
                 })
        {
            start.ArgumentList.Add(argument);
        }

        var process = Process.Start(start) ?? throw new InvalidOperationException($"{binary} did not start.");
        var server = new RedisServer($"127.0.0.1:{port}", null, process, scratch);

        // Connecting while it still binds is refused, and a client that meets that refusal may then
        // wait out its whole connect timeout on a server that is up: say when it is ready instead.
        var log = Path.Combine(scratch, "redis.log");
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!(File.Exists(log) && File.ReadAllText(log).Contains("Ready to accept connections")))
        {
            if (process.HasExited || DateTime.UtcNow > deadline)
            {
                var output = File.Exists(log) ? File.ReadAllText(log) : "(no log)";
                await server.DisposeAsync();
                throw new InvalidOperationException($"{binary} did not become ready:\n{output}");
            }

            await Task.Delay(5);
        }

        return server;
    }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }

        if (_process != null)
        {
            try
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync();
            }
            catch (InvalidOperationException)
            {
                // Already gone.
            }

            _process.Dispose();
        }

        if (_scratch != null)
        {
            try { Directory.Delete(_scratch, recursive: true); } catch (IOException) { }
        }
    }

    private static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    // The places scripts/lib/redis.sh looks, in the same order.
    private static string? FindBinary()
    {
        var candidates = new List<string>();

        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir != null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "TransportR.sln")))
            {
                candidates.Add(Path.Combine(dir.FullName, "tools", "redis-server"));
                break;
            }
        }

        candidates.AddRange((Environment.GetEnvironmentVariable("PATH") ?? "")
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Select(d => Path.Combine(d, "redis-server")));
        candidates.Add("/opt/homebrew/bin/redis-server");

        return candidates.FirstOrDefault(File.Exists);
    }

    private static bool CheckDocker()
    {
        try
        {
            var psi = new ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "info",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            };
            using var process = Process.Start(psi);
            process?.WaitForExit(2000);
            return process?.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}

/// <summary>Skips the test when there is neither a Docker daemon nor a <c>redis-server</c> to run it on.</summary>
public class RedisAvailableFactAttribute : FactAttribute
{
    public RedisAvailableFactAttribute()
    {
        if (!RedisServer.IsAvailable)
        {
            Skip = "Neither Docker nor a redis-server binary is available";
        }
    }
}
