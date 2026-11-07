using System.Globalization;

namespace TransportR.ClientBench;

internal sealed record Options(
    string HubUrl,
    string Mode,
    int Megabytes,
    int WarmupMegabytes,
    int Seed,
    int BatchBytes,
    int BatchItems,
    int SendWindow,
    int TimeoutSeconds)
{
    public const string Usage = """
        Usage: TransportR.ClientBench --hub <url> --mode items|bytes [options]

        Carries one in-memory transfer between a sender and a receiver of this process, through a hub that
        runs elsewhere, and prints the CPU time this process spent per MiB of batch body carried. The
        receiver checks every byte; any difference, or a transfer that fails, exits 1.

          --hub <url>             hub base URL, for instance http://localhost:5352
          --mode items|bytes      MessagePack items, or the byte-stream mode
          --megabytes <n>         MiB of batch body of the measured transfer (default 1024)
          --warmup-megabytes <n>  MiB of a first transfer, discarded, that lets the JIT settle (default 256)
          --seed <n>              seed of every pseudo-random byte (default 1)
          --batch-bytes <n>       bytes mode: batch size (default 262144)
          --batch-items <n>       items mode: items per batch, a divisor of 4096 (default 512)
          --window <n>            batches the sender may have in flight (default 8)
          --timeout-seconds <n>   bound on the whole run (default 600)
        Exit codes: 0 measured, 1 the run failed, 2 bad arguments.
        """;

    public static Options? Parse(string[] args, out string? error)
    {
        string? hub = null, mode = null;
        int megabytes = 1024, warmup = 256, seed = 1, batchBytes = 262144, batchItems = 512, window = 8, timeout = 600;
        error = null;

        for (var i = 0; i < args.Length; i++)
        {
            var name = args[i];
            if (i + 1 >= args.Length)
            {
                error = $"{name} needs a value.";
                return null;
            }

            var value = args[++i];
            switch (name)
            {
                case "--hub": hub = value; break;
                case "--mode": mode = value; break;
                case "--megabytes": if (!Int(value, out megabytes)) goto bad; break;
                case "--warmup-megabytes": if (!Int(value, out warmup)) goto bad; break;
                case "--seed": if (!Int(value, out seed)) goto bad; break;
                case "--batch-bytes": if (!Int(value, out batchBytes)) goto bad; break;
                case "--batch-items": if (!Int(value, out batchItems)) goto bad; break;
                case "--window": if (!Int(value, out window)) goto bad; break;
                case "--timeout-seconds": if (!Int(value, out timeout)) goto bad; break;
                default:
                    error = $"Unknown option {name}.";
                    return null;
            }

            continue;
        bad:
            error = $"{name} needs an integer, got '{value}'.";
            return null;
        }

        if (string.IsNullOrWhiteSpace(hub) || mode is not ("items" or "bytes"))
        {
            error = "--hub and --mode items|bytes are required.";
            return null;
        }

        if (megabytes <= 0 || warmup < 0 || batchBytes <= 0 || batchItems <= 0 || window <= 0 || timeout <= 0)
        {
            error = "Sizes, the window and the timeout must be positive (the warm-up may be 0).";
            return null;
        }

        return new Options(hub, mode, megabytes, warmup, seed, batchBytes, batchItems, window, timeout);
    }

    private static bool Int(string value, out int result)
        => int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out result);
}
