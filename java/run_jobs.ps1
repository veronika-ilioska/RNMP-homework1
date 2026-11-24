<# Start the Java Flink pipeline. Use -Job analytics, events, or both. #>
[CmdletBinding()]
param(
    [ValidateSet('both', 'analytics', 'events')][string]$Job = 'both',
    [ValidateSet('replay', 'stream')][string]$Mode = 'replay',
    [ValidateSet('earliest', 'latest')][string]$StartOffset = 'earliest',
    [string]$InputTopic = 'github-events',
    [string]$AnalyticsTopic = 'github-user-sessions',
    [string]$EventsTopic = 'github-user-alerts',
    [ValidateRange(1, 1000000)][long]$SessionGapMinutes = 30,
    [ValidateRange(0, 100000000)][long]$WatermarkSeconds = 180,
    [ValidateRange(0, 100000000)][long]$IdleSeconds = 0,
    [ValidateRange(1, 100000000)][long]$QuietSeconds = 1800,
    [ValidateRange(1, 4)][int]$Parallelism = 1,
    [string]$BootstrapServers = 'kafka:9093',
    [string]$PythonDirectory = (Join-Path (Split-Path -Parent $PSScriptRoot) 'python'),
    [switch]$Build,
    [switch]$Detach
)

$ErrorActionPreference = 'Stop'
# Kafka runs in the course-based Python project; the parent file only runs Flink.
$workspaceDirectory = Split-Path -Parent $PSScriptRoot
$previousKafkaNetwork = $env:KAFKA_NETWORK
Push-Location -LiteralPath $workspaceDirectory
try {
    $courseCompose = Join-Path $PythonDirectory 'docker-compose.yml'
    if (-not (Test-Path -LiteralPath $courseCompose)) {
        throw 'The Python base must contain the course docker-compose.yml. Check -PythonDirectory.'
    }
    docker compose -f $courseCompose up -d --wait
    if ($LASTEXITCODE -ne 0) { throw 'Could not start course Kafka. Check Docker Desktop and whether another broker uses port 9092.' }
    if (-not $env:KAFKA_NETWORK) {
        $kafkaContainer = docker compose -f $courseCompose ps -q kafka
        if ($LASTEXITCODE -ne 0 -or -not $kafkaContainer) { throw 'Could not locate the course Kafka container.' }
        $kafkaDetails = docker inspect $kafkaContainer
        if ($LASTEXITCODE -ne 0) { throw 'Could not inspect the course Kafka network.' }
        $networks = @((($kafkaDetails | ConvertFrom-Json)[0]).NetworkSettings.Networks.PSObject.Properties.Name)
        if ($networks.Count -ne 1) { throw 'Set KAFKA_NETWORK to the Docker network shared with course Kafka.' }
        $env:KAFKA_NETWORK = $networks[0]
    }
    $flinkImage = 'hw1-github-java:2.3.0'
    $imageId = docker images --quiet $flinkImage
    if ($LASTEXITCODE -ne 0) { throw 'Could not query Docker. Start Docker Desktop first.' }
    if ($Build -or -not $imageId) {
        docker compose --profile processing build jobmanager
        if ($LASTEXITCODE -ne 0) { throw 'Java build failed.' }
    }
    docker compose --profile processing up -d --wait jobmanager taskmanager
    if ($LASTEXITCODE -ne 0) { throw 'Could not start the Flink services.' }

    $flinkArguments = @('compose', 'exec', '-T', 'jobmanager', 'flink', 'run')
    if ($Detach) { $flinkArguments += '-d' }
    $flinkArguments += @(
        '/opt/flink/usrlib/github-pipeline.jar',
        '--job', $Job.ToLowerInvariant(), '--mode', $Mode.ToLowerInvariant(),
        '--start-offset', $StartOffset.ToLowerInvariant(),
        '--input-topic', $InputTopic, '--analytics-topic', $AnalyticsTopic,
        '--events-topic', $EventsTopic, '--session-gap-minutes', "$SessionGapMinutes",
        '--watermark-seconds', "$WatermarkSeconds", '--idle-seconds', "$IdleSeconds", '--quiet-seconds', "$QuietSeconds",
        '--parallelism', "$Parallelism", '--bootstrap-servers', $BootstrapServers
    )
    & docker @flinkArguments
    if ($LASTEXITCODE -ne 0) { throw 'Java Flink job failed. See the error above and the Flink dashboard.' }
} finally {
    $env:KAFKA_NETWORK = $previousKafkaNetwork
    Pop-Location
}
