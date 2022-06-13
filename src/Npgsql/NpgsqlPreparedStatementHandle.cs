using System;

namespace Npgsql;

#pragma warning disable CS1591
#pragma warning disable RS0016

public class NpgsqlPreparedStatementHandle : IEquatable<NpgsqlPreparedStatementHandle>
{
    internal NpgsqlPreparedStatementHandle(string statementName, string sql)
    {
        StatementName = statementName;
        Sql = sql;
    }

    internal NpgsqlDataSource DataSource { get; set; } = default!;

    internal readonly string StatementName;
    internal readonly string Sql;

    public bool Equals(NpgsqlPreparedStatementHandle? other)
        => other is not null &&
           (ReferenceEquals(this, other) ||
            ReferenceEquals(DataSource, other.DataSource) && StatementName == other.StatementName);

    public override bool Equals(object? obj)
        => obj is NpgsqlPreparedStatementHandle otherHandle && Equals(otherHandle);

    public override int GetHashCode()
        => HashCode.Combine(DataSource, StatementName);

    public override string ToString() => $"{StatementName} -> {(Sql.Length < 30 ? Sql : Sql.Substring(0, 30))}";
}
