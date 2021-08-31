using System;
using NodaTime;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.NodaTime.Internal
{
    public class NodaTimeTypeHandlerResolver : ITypeHandlerResolver
    {
        readonly NpgsqlDatabaseInfo _databaseInfo;

        readonly TimestampHandler _timestampHandler;
        readonly TimestampTzHandler _timestampTzHandler;
        readonly DateHandler _dateHandler;
        readonly TimeHandler _timeHandler;
        readonly TimeTzHandler _timeTzHandler;
        readonly IntervalHandler _intervalHandler;

        internal NodaTimeTypeHandlerResolver(NpgsqlConnector connector)
        {
            _databaseInfo = connector.DatabaseInfo;

            _timestampHandler = new TimestampHandler(PgType("timestamp without time zone"), connector.Settings.ConvertInfinityDateTime);
            _timestampTzHandler = new TimestampTzHandler(PgType("timestamp with time zone"), connector.Settings.ConvertInfinityDateTime);
            _dateHandler = new DateHandler(PgType("date"), connector.Settings.ConvertInfinityDateTime);
            _timeHandler = new TimeHandler(PgType("time without time zone"));
            _timeTzHandler = new TimeTzHandler(PgType("time with time zone"));
            _intervalHandler = new IntervalHandler(PgType("interval"));
        }

        public NpgsqlTypeHandler? ResolveOID(uint oid)
            => oid switch
            {
                PostgresTypeOIDs.Timestamp => _timestampHandler,
                PostgresTypeOIDs.TimestampTz => _timestampTzHandler,
                PostgresTypeOIDs.Date => _dateHandler,
                PostgresTypeOIDs.Time => _timeHandler,
                PostgresTypeOIDs.TimeTz => _timeTzHandler,
                PostgresTypeOIDs.Interval => _intervalHandler,

                _ => null
            };

        public NpgsqlTypeHandler? ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType)
            => npgsqlDbType switch
            {
                NpgsqlDbType.Timestamp => _timestampHandler,
                NpgsqlDbType.TimestampTz => _timestampTzHandler,
                NpgsqlDbType.Date => _dateHandler,
                NpgsqlDbType.Time => _timeHandler,
                NpgsqlDbType.TimeTz => _timeTzHandler,
                NpgsqlDbType.Interval => _intervalHandler,

                _ => null
            };

        public NpgsqlTypeHandler? ResolveDataTypeName(string typeName)
            => typeName switch
            {
                "timestamp" or "timestamp without time zone" => _timestampHandler,
                "timestamptz" or "timestamp with time zone" => _timestampTzHandler,
                "date" => _dateHandler,
                "time" => _timeHandler,
                "timetz" => _timeTzHandler,
                "interval" => _intervalHandler,

                _ => null
            };

        public NpgsqlTypeHandler? ResolveClrType(Type type)
        {
            if (type == typeof(Instant) || type == typeof(LocalDateTime))
                return _timestampHandler;
            if (type == typeof(ZonedDateTime) || type == typeof(OffsetDateTime))
                return _timestampTzHandler;
            if (type == typeof(LocalDate))
                return _dateHandler;
            if (type == typeof(LocalTime))
                return _timeHandler;
            if (type == typeof(OffsetTime))
                return _timeTzHandler;
            if (type == typeof(Period) || type == typeof(Duration))
                return _intervalHandler;

            return null;
        }

        PostgresType PgType(string pgTypeName) => _databaseInfo.GetPostgresTypeByName(pgTypeName);
    }
}
