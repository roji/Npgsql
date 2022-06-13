using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace Npgsql;

/// <summary>
/// Provides a simple API for configuring and creating an <see cref="NpgsqlDataSource" />, from which database connections can be obtained.
/// </summary>
public class NpgsqlDataSourceBuilder
{
    ILoggerFactory? _loggerFactory;
    bool _sensitiveDataLoggingEnabled;

    readonly List<NpgsqlPreparedStatementHandle> _preparedStatements = new();
    int _preparedStatementIndex;

    /// <summary>
    /// A connection string builder that can be used to configured the connection string on the builder.
    /// </summary>
    public NpgsqlConnectionStringBuilder ConnectionStringBuilder { get; }

    /// <summary>
    /// Returns the connection string, as currently configured on the builder.
    /// </summary>
    public string ConnectionString => ConnectionStringBuilder.ToString();

    /// <summary>
    /// Constructs a new <see cref="NpgsqlDataSourceBuilder" />, optionally starting out from the given <paramref name="connectionString"/>.
    /// </summary>
    public NpgsqlDataSourceBuilder(string? connectionString = null)
        => ConnectionStringBuilder = new NpgsqlConnectionStringBuilder(connectionString);

    /// <summary>
    /// Sets the <see cref="ILoggerFactory" /> that will be used for logging.
    /// </summary>
    /// <param name="loggerFactory">The logger factory to be used.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public NpgsqlDataSourceBuilder UseLoggerFactory(ILoggerFactory? loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    /// Enables parameters to be included in logging. This includes potentially sensitive information from data sent to PostgreSQL.
    /// You should only enable this flag in development, or if you have the appropriate security measures in place based on the
    /// sensitivity of this data.
    /// </summary>
    /// <param name="parameterLoggingEnabled">If <see langword="true" />, then sensitive data is logged.</param>
    /// <returns>The same builder instance so that multiple calls can be chained.</returns>
    public NpgsqlDataSourceBuilder EnableParameterLogging(bool parameterLoggingEnabled = true)
    {
        _sensitiveDataLoggingEnabled = parameterLoggingEnabled;
        return this;
    }

#pragma warning disable RS0016
    /// <summary>
    /// Registers the given SQL and parameter types to be prepared on all connections handed out by this data source.
    /// A prepared statement will automatically be created whenever a physical connection is opened, and the handle returned by this
    /// method can be used on <see cref="NpgsqlCommand" /> to execute that statement.
    /// </summary>
    /// <returns>
    /// A prepared statement handle that can later be passed to <see cref="NpgsqlCommand" /> for execution.
    /// TODO: Adjust according to final API
    /// </returns>
    // TODO: Consider making this return NpgsqlDataSourceBuilder and return the handle via an out param?
    public NpgsqlPreparedStatementHandle Prepare(string sql)
    {
        var preparedStatement = new NpgsqlPreparedStatementHandle("_ds" + ++_preparedStatementIndex, sql);
        _preparedStatements.Add(preparedStatement);
        return preparedStatement;
    }

    // TODO: Need overload with parameter types (for ambiguous SQL). But what does it accept:
    // 1. (params) array of NpgsqlDbType
    // 2. (params) array of DataTypeName
    // 3. array of NpgsqlParameter, allowing mixing and matching?

#pragma warning restore RS0016

    /// <summary>
    /// Builds and returns an <see cref="NpgsqlDataSource" /> which is ready for use.
    /// </summary>
    public NpgsqlDataSource GetDataSource()
    {
        var connectionString = ConnectionStringBuilder.ToString();

        ConnectionStringBuilder.PostProcessAndValidate();

        var loggingConfiguration = _loggerFactory is null
            ? NpgsqlLoggingConfiguration.NullConfiguration
            : new NpgsqlLoggingConfiguration(_loggerFactory, _sensitiveDataLoggingEnabled);

        if (ConnectionStringBuilder.Host!.Contains(","))
        {
            if (ConnectionStringBuilder.Multiplexing)
                throw new NotSupportedException("Multiplexing is not supported with multiple hosts");
            if (ConnectionStringBuilder.ReplicationMode != ReplicationMode.Off)
                throw new NotSupportedException("Replication is not supported with multiple hosts");
            return new MultiHostDataSource(ConnectionStringBuilder, connectionString, loggingConfiguration, _preparedStatements);
        }

        return ConnectionStringBuilder.Multiplexing
            ? new MultiplexingDataSource(ConnectionStringBuilder, connectionString, loggingConfiguration, _preparedStatements)
            : ConnectionStringBuilder.Pooling
                ? new PoolingDataSource(ConnectionStringBuilder, connectionString, loggingConfiguration, _preparedStatements)
                : new UnpooledDataSource(ConnectionStringBuilder, connectionString, loggingConfiguration, _preparedStatements);
    }
}
