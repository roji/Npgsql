using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Microsoft.Extensions.Telemetry.Testing.Metering;

namespace Npgsql.Tests;

public class MetricsTests : TestBase
{
    const string MeterName = "Npgsql";

    [Test]
    public async Task MaxConnections()
    {
        await using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString)
        {
            Name = nameof(MaxConnections),
            ConnectionStringBuilder = { MaxPoolSize = 80 }
        }.Build();

        using var maxConnectionsCollector = new MetricCollector<int>(null, MeterName, "db.client.connections.max");
        maxConnectionsCollector.RecordObservableInstruments();
        var measurement = maxConnectionsCollector.GetMeasurementSnapshot().ForPool(nameof(MaxConnections)).Single();

        Assert.That(measurement.Value, Is.EqualTo(80));
    }

    [Test]
    public async Task ConnectionUsage()
    {
        await using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString) { Name = nameof(ConnectionUsage) }.Build();

        using var connectionUsageCollector = new MetricCollector<int>(null, MeterName, "db.client.connections.usage");

        AssertIdleUsed(0, 0);

        await using var conn1 = await dataSource.OpenConnectionAsync();
        AssertIdleUsed(0, 1);

        await using var conn2 = await dataSource.OpenConnectionAsync();
        AssertIdleUsed(0, 2);

        await conn1.CloseAsync();
        AssertIdleUsed(1, 1);

        await conn2.CloseAsync();
        AssertIdleUsed(2, 0);

        void AssertIdleUsed(int expectedIdle, int expectedUsed)
        {
            connectionUsageCollector.RecordObservableInstruments();

            var idleMeasurement = connectionUsageCollector.GetMeasurementSnapshot()
                .ContainsTags(new KeyValuePair<string, object?>("state", "idle"))
                .ForPool(nameof(ConnectionUsage))
                .Single();

            Assert.That(idleMeasurement.Value, Is.EqualTo(expectedIdle));

            var usedMeasurement = connectionUsageCollector.GetMeasurementSnapshot()
                .ContainsTags(new KeyValuePair<string, object?>("state", "used"))
                .ForPool(nameof(ConnectionUsage))
                .Single();

            Assert.That(usedMeasurement.Value, Is.EqualTo(expectedUsed));

            connectionUsageCollector.Clear();
        }
    }

    [Test]
    public async Task CommandsExecuting()
    {
        await using var dataSource = new NpgsqlDataSourceBuilder(ConnectionString) { Name = nameof(CommandsExecuting) }.Build();

        using var commandsExecutingCollector = new MetricCollector<int>(null, MeterName, "db.client.commands.executing");

        await using var command1 = dataSource.CreateCommand("SELECT pg_sleep(1000)");
        await using var command2 = dataSource.CreateCommand("SELECT pg_sleep(1000)");
        var cancellationTokenSource = new CancellationTokenSource();
        var commandTask1 = command1.ExecuteNonQueryAsync(cancellationTokenSource.Token);

        // await commandsExecutingCollector.WaitForMeasurementsAsync(1);

        var measurement = commandsExecutingCollector.GetMeasurementSnapshot().ForPool(nameof(CommandsExecuting)).Single();
        Assert.That(measurement.Value, Is.EqualTo(1));

        var commandTask2 = command2.ExecuteNonQueryAsync(cancellationTokenSource.Token);

        measurement = commandsExecutingCollector.GetMeasurementSnapshot().ForPool(nameof(CommandsExecuting)).Single();
        Assert.That(measurement.Value, Is.EqualTo(2));

        await cancellationTokenSource.CancelAsync();
        Assert.ThrowsAsync<OperationCanceledException>(() => commandTask1);
        Assert.ThrowsAsync<OperationCanceledException>(() => commandTask2);

        measurement = commandsExecutingCollector.GetMeasurementSnapshot().ForPool(nameof(CommandsExecuting)).Single();
        Assert.That(measurement.Value, Is.EqualTo(0));
    }
}

static class CollectedMeasurementExtensions
{
    public static IEnumerable<CollectedMeasurement<T>> ForPool<T>(
        this IEnumerable<CollectedMeasurement<T>> measurements,
        string poolName)
        where T : struct
        => measurements.Where(
            (Func<CollectedMeasurement<T>, bool>)(m => m.ContainsTags(new KeyValuePair<string, object?>("pool.name", poolName))));
}
