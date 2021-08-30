using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.NetworkInformation;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandlers;
using Npgsql.Internal.TypeHandlers.DateTimeHandlers;
using Npgsql.Internal.TypeHandlers.FullTextSearchHandlers;
using Npgsql.Internal.TypeHandlers.NetworkHandlers;
using Npgsql.Internal.TypeHandlers.NumericHandlers;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.TypeMapping
{
    class BuiltInTypeHandlerResolver : ITypeHandlerResolver
    {
        readonly NpgsqlConnector _connector;
        readonly NpgsqlDatabaseInfo _databaseInfo;

        readonly Type _readonlyIpType = IPAddress.Loopback.GetType();

        #region Cached handlers

        // Numeric types
        Int16Handler? _int16Handler;
        Int32Handler? _int32Handler;
        Int64Handler? _int64Handler;
        SingleHandler? _singleHandler;
        DoubleHandler? _doubleHandler;
        NumericHandler? _numericHandler;
        MoneyHandler? _moneyHandler;

        // Text types
        TextHandler? _textHandler;

        // Date/time types
        TimestampHandler? _timestampHandler;
        TimestampTzHandler? _timestampTzHandler;
        DateHandler? _dateHandler;
        TimeHandler? _timeHandler;
        TimeTzHandler? _timeTzHandler;
        IntervalHandler? _intervalHandler;

        // Network types
        CidrHandler? _cidrHandler;
        InetHandler? _inetHandler;
        MacaddrHandler? _macaddrHandler;
        MacaddrHandler? _macaddr8Handler;

        // Full-text search types
        TsQueryHandler? _tsQueryHandler;
        TsVectorHandler? _tsVectorHandler;

        // Misc types
        BoolHandler? _boolHandler;
        ByteaHandler? _byteaHandler;
        UuidHandler? _uuidHandler;
        BitStringHandler? _bitVaryingHandler;
        BitStringHandler? _bitHandler;
        RecordHandler? _recordHandler;
        VoidHandler? _voidHandler;
        HstoreHandler? _hstoreHandler;

        // Special types
        UnknownTypeHandler? _unknownHandler;

        /// <summary>
        /// A dictionary for type OIDs of PG types which aren't known in advance (i.e. extension types)
        /// </summary>
        readonly ConcurrentDictionary<uint, NpgsqlTypeHandler> _extensionHandlers = new();

        #endregion Cached handlers

        internal BuiltInTypeHandlerResolver(NpgsqlConnector connector)
        {
            _connector = connector;
            _databaseInfo = connector.DatabaseInfo;

            // TODO: Eagerly instantiate some handlers for very common types so we don't need to check later
            // _int16Handler = new Int16Handler(GetPgType("smallint"));
            // _int32Handler = new Int32Handler(GetPgType("integer"));
            // _int64Handler = new Int64Handler(GetPgType("bigint"));
            // _doubleHandler = new DoubleHandler(GetPgType("double precision"));
            // _numericHandler = new NumericHandler(GetPgType("numeric"));
            // _textHandler ??= new TextHandler(GetPgType("text"), _connector);
        }

        public bool ResolveOID(uint oid, [NotNullWhen(true)] out NpgsqlTypeHandler? handler)
        {
            // First handle the built-in types whose OIDs are fixed
            switch (oid)
            {
            // Numeric types

            case PostgresTypeOIDs.Int2:
                handler = Int16();
                return true;

            case PostgresTypeOIDs.Int4:
                handler = Int32();
                return true;

            case PostgresTypeOIDs.Int8:
                handler = Int64();
                return true;

            case PostgresTypeOIDs.Float4:
                handler = Single();
                return true;

            case PostgresTypeOIDs.Float8:
                handler = Double();
                return true;

            case PostgresTypeOIDs.Numeric:
                handler = Numeric();
                return true;

            case PostgresTypeOIDs.Money:
                handler = Money();
                return true;

            // Text types

            case PostgresTypeOIDs.Text:
                handler = Text();
                return true;

            // Date/time types

            case PostgresTypeOIDs.Timestamp:
                handler = Timestamp();
                return true;

            case PostgresTypeOIDs.TimestampTz:
                handler = TimestampTz();
                return true;

            case PostgresTypeOIDs.Date:
                handler = Date();
                return true;

            case PostgresTypeOIDs.Time:
                handler = Time();
                return true;

            case PostgresTypeOIDs.TimeTz:
                handler = TimeTz();
                return true;

            case PostgresTypeOIDs.Interval:
                handler = Interval();
                return true;

            // Network types

            case PostgresTypeOIDs.Cidr:
                handler = Cidr();
                return true;

            case PostgresTypeOIDs.Inet:
                handler = Inet();
                return true;

            case PostgresTypeOIDs.Macaddr:
                handler = Macaddr();
                return true;

            case PostgresTypeOIDs.Macaddr8:
                handler = Macaddr8();
                return true;

            // Full-text search types

            case PostgresTypeOIDs.TsQuery:
                handler = TsQuery();
                return true;

            case PostgresTypeOIDs.TsVector:
                handler = TsVector();
                return true;

            // Misc types

            case PostgresTypeOIDs.Bool:
                handler = Bool();
                return true;

            case PostgresTypeOIDs.Bytea:
                handler = Bytea();
                return true;

            case PostgresTypeOIDs.Uuid:
                handler = Uuid();
                return true;

            case PostgresTypeOIDs.Varbit:
                handler = Varbit();
                return true;

            case PostgresTypeOIDs.Bit:
                handler = Bit();
                return true;

            case PostgresTypeOIDs.Record:
                handler = Record();
                return true;

            case PostgresTypeOIDs.Void:
                handler = Void();
                return true;
            }

            // This isn't a well-known type with a fixed type OID.
            if (_extensionHandlers.TryGetValue(oid, out handler))
                return true;

            if (_databaseInfo.ByOID.TryGetValue(oid, out var pgType)
                && pgType.Name != "pg_catalog"
                && (handler = ResolveDataTypeName(pgType.Name)) is not null)
            {
                _extensionHandlers[oid] = handler;
                return true;
            }

            return false;
        }

        public NpgsqlTypeHandler? ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType)
        {
            switch (npgsqlDbType)
            {
            // Numeric types

            case NpgsqlDbType.Smallint:
                // TODO: this throws if type not found. Maybe that's OK.
                return Int16();

            case NpgsqlDbType.Integer:
                return Int32();

            case NpgsqlDbType.Bigint:
                return Int64();

            case NpgsqlDbType.Real:
                return Single();

            case NpgsqlDbType.Double:
                return Double();

            case NpgsqlDbType.Numeric:
                return Numeric();

            case NpgsqlDbType.Money:
                return Money();

            // Text types

            case NpgsqlDbType.Text:
                return Text();

            // Date/time types

            case NpgsqlDbType.Timestamp:
                return Timestamp();

            case NpgsqlDbType.TimestampTz:
                return TimestampTz();

            case NpgsqlDbType.Date:
                return Date();

            case NpgsqlDbType.Time:
                return Time();

            case NpgsqlDbType.TimeTz:
                return TimeTz();

            case NpgsqlDbType.Interval:
                return Interval();

            // Network types

            case NpgsqlDbType.Cidr:
                return Cidr();

            case NpgsqlDbType.Inet:
                return Inet();

            case NpgsqlDbType.MacAddr:
                return Macaddr();

            case NpgsqlDbType.MacAddr8:
                return Macaddr8();

            // Full-text search types

            case NpgsqlDbType.TsQuery:
                return TsQuery();

            case NpgsqlDbType.TsVector:
                return TsVector();

            // Misc types

            case NpgsqlDbType.Boolean:
                return Bool();

            case NpgsqlDbType.Bytea:
                return Bytea();

            case NpgsqlDbType.Uuid:
                return Uuid();

            case NpgsqlDbType.Varbit:
                return Varbit();

            case NpgsqlDbType.Bit:
                return Bit();

            case NpgsqlDbType.Hstore:
                return Hstore();

            // Special types
            case NpgsqlDbType.Unknown:
                return Unknown();

            default:
                return null;
            }
        }

        public NpgsqlTypeHandler? ResolveDataTypeName(string typeName)
        {
            switch (typeName)
            {
            // Numeric types

            case "smallint":
                return Int16();

            case "integer" or "int":
                return Int32();

            case "bigint":
                return Int64();

            case "real":
                return Single();

            case "double precision":
                return Double();

            case "numeric" or "decimal":
                return Numeric();

            case "money":
                return Money();

            // Text types

            case "text":
                return Text();

            // Date/time types

            case "timestamp" or "timestamp without time zone":
                return Timestamp();

            case "timestamptz" or "timestamp with time zone":
                return TimestampTz();

            case "date":
                return Date();

            case "time":
                return Time();

            case "timetz":
                return TimeTz();

            case "interval":
                return Interval();

            // Network types

            case "cidr":
                return Cidr();

            case "inet":
                return Inet();

            case "macaddr":
                return Macaddr();

            case "macaddr8":
                return Macaddr8();

            // Full-text search types

            case "tsquery":
                return TsQuery();

            case "tsvector":
                return TsVector();

            // Misc types

            case "bool" or "boolean":
                return Bool();

            case "bytea":
                return Bytea();

            case "uuid":
                return Uuid();

            case "bit varying" or "varbit":
                return Varbit();

            case "bit":
                return Bit();

            case "hstore":
                return Hstore();

            default:
                return null;
            }
        }

        // TODO: Make sure we actually need a version for Type, rather than for object. Would simplify for the date/time types, and we
        // can also do a simple switch.
        public NpgsqlTypeHandler? ResolveClrType(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
            case TypeCode.Byte or TypeCode.Int16 when !type.IsEnum:
                return Int16();
            case TypeCode.Int32 when !type.IsEnum:
                return Int32();
            case TypeCode.Int64 when !type.IsEnum:
                return Int64();
            case TypeCode.Single:
                return Single();
            case TypeCode.Double:
                return Double();
            case TypeCode.Decimal:
                return Numeric();
            case TypeCode.String:
                return Text();
            case TypeCode.Boolean:
                return Bool();
            case TypeCode.DateTime:
                return Timestamp();
            case TypeCode.UInt32 when !type.IsEnum:
            case TypeCode.UInt64 when !type.IsEnum:
            case TypeCode.SByte when !type.IsEnum:
            case TypeCode.Char:
                // TODO
                throw new NotImplementedException();
            }

            // Date/time types

            if (type == typeof(DateTime) || type == typeof(NpgsqlDateTime))
                return Timestamp();

            if (type == typeof(DateTimeOffset))
                return TimestampTz();

            if (type == typeof(NpgsqlDate)
#if NET6_0_OR_GREATER
                || type == typeof(DateOnly)
#endif
            )
            {
                return Date();
            }

#if NET6_0_OR_GREATER
            if (type == typeof(TimeOnly))
#endif
            {
                return Time();
            }

            if (type == typeof(TimeSpan) || type == typeof(NpgsqlTimeSpan))
                return Interval();

            // Network types

#pragma warning disable 618
            if (type == typeof(IPAddress) || type == _readonlyIpType || type == typeof((IPAddress Address, int Subnet)) ||
                type == typeof(NpgsqlInet))
                return Inet();
#pragma warning restore 618

            if (type == typeof(PhysicalAddress))
                return Macaddr();

            // Full-text search types
            if (type == typeof(NpgsqlTsQuery) || type == typeof(NpgsqlTsQueryAnd) || type == typeof(NpgsqlTsQueryEmpty) ||
                type == typeof(NpgsqlTsQueryFollowedBy) || type == typeof(NpgsqlTsQueryLexeme)
                || type == typeof(NpgsqlTsQueryNot) || type == typeof(NpgsqlTsQueryOr) || type == typeof(NpgsqlTsQueryBinOp))
            {
                return TsQuery();
            }

            if (type == typeof(NpgsqlTsVector))
                return TsVector();

            // Misc types

            if (type == typeof(byte[])
                || type == typeof(ArraySegment<byte>)
#if !NETSTANDARD2_0
                || type == typeof(ReadOnlyMemory<byte>)
                || type == typeof(Memory<byte>)
#endif
            )
            {
                return Bytea();
            }

            if (type == typeof(Guid))
                return Uuid();

            if (type == typeof(BitArray) || type == typeof(BitVector32))
                return Varbit();

            if (type == typeof(Dictionary<string, string>)
                || type == typeof(IDictionary<string, string?>)
#if !NETSTANDARD2_0 && !NETSTANDARD2_1
                || type == typeof(System.Collections.Immutable.ImmutableDictionary<string, string?>)
#endif
            )
            {
                return Hstore();
            }

            return null;
        }

        #region Handler creation

        // Numeric types
        NpgsqlTypeHandler Int16()   => _int16Handler   ??= new Int16Handler(PgType("smallint"));
        NpgsqlTypeHandler Int32()   => _int32Handler   ??= new Int32Handler(PgType("integer"));
        NpgsqlTypeHandler Int64()   => _int64Handler   ??= new Int64Handler(PgType("bigint"));
        NpgsqlTypeHandler Single()  => _singleHandler  ??= new SingleHandler(PgType("real"));
        NpgsqlTypeHandler Double()  => _doubleHandler  ??= new DoubleHandler(PgType("double precision"));
        NpgsqlTypeHandler Numeric() => _numericHandler ??= new NumericHandler(PgType("numeric"));
        NpgsqlTypeHandler Money()   => _moneyHandler   ??= new MoneyHandler(PgType("money"));

        // Text types

        NpgsqlTypeHandler Text()    => _textHandler ??= new TextHandler(PgType("text"), _connector);

        // Date/time types

        NpgsqlTypeHandler Timestamp() => _timestampHandler
            ??= new TimestampHandler(PgType("timestamp without time zone"), _connector.Settings.ConvertInfinityDateTime);
        NpgsqlTypeHandler TimestampTz() => _timestampTzHandler
            ??= new TimestampTzHandler(PgType("timestamp with time zone"), _connector.Settings.ConvertInfinityDateTime);
        NpgsqlTypeHandler Date() => _dateHandler ??= new DateHandler(PgType("date"), _connector.Settings.ConvertInfinityDateTime);
        NpgsqlTypeHandler Time() => _timeHandler ??= new TimeHandler(PgType("time without time zone"));
        NpgsqlTypeHandler TimeTz() => _timeTzHandler ??= new TimeTzHandler(PgType("time with time zone"));
        NpgsqlTypeHandler Interval() => _intervalHandler ??= new IntervalHandler(PgType("interval"));

        // Network types

        NpgsqlTypeHandler Cidr() => _cidrHandler ??= new CidrHandler(PgType("cidr"));
        NpgsqlTypeHandler Inet() => _inetHandler ??= new InetHandler(PgType("inet"));
        NpgsqlTypeHandler Macaddr() => _macaddrHandler ??= new MacaddrHandler(PgType("macaddr"));
        NpgsqlTypeHandler Macaddr8() => _macaddr8Handler ??= new MacaddrHandler(PgType("macaddr8"));

        // Misc types

        NpgsqlTypeHandler Bool() => _boolHandler ??= new BoolHandler(PgType("boolean"));
        NpgsqlTypeHandler Bytea() => _byteaHandler ??= new ByteaHandler(PgType("bytea"));
        NpgsqlTypeHandler Uuid() => _uuidHandler ??= new UuidHandler(PgType("uuid"));
        NpgsqlTypeHandler Varbit() => _bitVaryingHandler ??= new BitStringHandler(PgType("varbit"));
        NpgsqlTypeHandler Bit() => _bitHandler ??= new BitStringHandler(PgType("bit"));
        NpgsqlTypeHandler Record() => _recordHandler ??= new RecordHandler(PgType("record"), _connector.TypeMapper);
        NpgsqlTypeHandler Void() => _voidHandler ??= new VoidHandler(PgType("void"));
        NpgsqlTypeHandler Hstore() => _hstoreHandler
            ??= new HstoreHandler(PgType("hstore"), _textHandler ??= new TextHandler(PgType("text"), _connector));
        NpgsqlTypeHandler Unknown() => _unknownHandler ??= new UnknownTypeHandler(_connector);

        // Full-text search types

        NpgsqlTypeHandler TsQuery() => _tsQueryHandler ??= new TsQueryHandler(PgType("tsquery"));
        NpgsqlTypeHandler TsVector() => _tsVectorHandler ??= new TsVectorHandler(PgType("tsvector"));

        #endregion Handler creation

        PostgresType PgType(string pgTypeName)
            => _databaseInfo.GetPostgresTypeByName(pgTypeName);
    }
}
