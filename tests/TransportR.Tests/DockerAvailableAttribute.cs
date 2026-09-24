using System.Diagnostics;

namespace TransportR.Tests;

/// <summary>
/// xUnit Fact attribute that skips the test if Docker is not available.
/// Checks Docker daemon connectivity via CLI.
/// </summary>
public class DockerAvailableFactAttribute : FactAttribute
{
    private static readonly Lazy<bool> _isDockerAvailable = new(CheckDockerAvailable);

    public DockerAvailableFactAttribute()
    {
        if (!_isDockerAvailable.Value)
        {
            Skip = "Docker is not available - skipping container-based test";
        }
    }

    private static bool CheckDockerAvailable()
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

/// <summary>
/// xUnit Theory attribute that skips the test if Docker is not available.
/// </summary>
public class DockerAvailableTheoryAttribute : TheoryAttribute
{
    private static readonly Lazy<bool> _isDockerAvailable = new(CheckDockerAvailable);

    public DockerAvailableTheoryAttribute()
    {
        if (!_isDockerAvailable.Value)
        {
            Skip = "Docker is not available - skipping container-based test";
        }
    }

    private static bool CheckDockerAvailable()
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
