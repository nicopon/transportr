using global::Docker.DotNet;

namespace TransportR.Tests;

/// <summary>
/// xUnit Fact attribute that skips the test if Docker is not available.
/// Uses Docker.DotNet to check Docker daemon connectivity.
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
            using var client = new DockerClientConfiguration().CreateClient();
            client.System.PingAsync().GetAwaiter().GetResult();
            return true;
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
            using var client = new DockerClientConfiguration().CreateClient();
            client.System.PingAsync().GetAwaiter().GetResult();
            return true;
        }
        catch
        {
            return false;
        }
    }
}
