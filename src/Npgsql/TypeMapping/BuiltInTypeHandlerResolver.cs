using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Collections.Specialized;
using System.Net;
using System.Net.NetworkInformation;
using System.Numerics;
using System.Text.Json;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandlers;
using Npgsql.Internal.TypeHandlers.DateTimeHandlers;
using Npgsql.Internal.TypeHandlers.FullTextSearchHandlers;
using Npgsql.Internal.TypeHandlers.GeometricHandlers;
using Npgsql.Internal.TypeHandlers.InternalTypeHandlers;
using Npgsql.Internal.TypeHandlers.LTreeHandlers;
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
        readonly Int16Handler _int16Handler;
        readonly Int32Handler _int32Handler;
        readonly Int64Handler _int64Handler;
        SingleHandler? _singleHandler;
        readonly DoubleHandler _doubleHandler;
        readonly NumericHandler _numericHandler;
        MoneyHandler? _moneyHandler;

        // Text types
        readonly TextHandler _textHandler;
        TextHandler? _xmlHandler;
        TextHandler? _varcharHandler;
        TextHandler? _charHandler;
        TextHandler? _nameHandler;
        TextHandler? _refcursorHandler;
        TextHandler? _citextHandler;
        readonly JsonHandler _jsonbHandler;
        JsonHandler? _jsonHandler;
        JsonPathHandler? _jsonPathHandler;

        // Date/time types
        readonly TimestampHandler _timestampHandler;
        readonly TimestampTzHandler _timestampTzHandler;
        readonly DateHandler _dateHandler;
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

        // Geometry types
        BoxHandler? _boxHandler;
        CircleHandler? _circleHandler;
        LineHandler? _lineHandler;
        LineSegmentHandler? _lineSegmentHandler;
        PathHandler? _pathHandler;
        PointHandler? _pointHandler;
        PolygonHandler? _polygonHandler;

        // LTree types
        LQueryHandler? _lQueryHandler;
        LTreeHandler? _lTreeHandler;
        LTxtQueryHandler? _lTxtQueryHandler;

        // UInt types
        UInt32Handler? _oidHandler;
        UInt32Handler? _xidHandler;
        UInt64Handler? _xid8Handler;
        UInt32Handler? _cidHandler;
        UInt32Handler? _regtypeHandler;
        UInt32Handler? _regconfigHandler;

        // Misc types
        readonly BoolHandler _boolHandler;
        ByteaHandler? _byteaHandler;
        readonly UuidHandler _uuidHandler;
        BitStringHandler? _bitVaryingHandler;
        BitStringHandler? _bitHandler;
        RecordHandler? _recordHandler;
        VoidHandler? _voidHandler;
        HstoreHandler? _hstoreHandler;

        // Internal types
        Int2VectorHandler? _int2VectorHandler;
        OIDVectorHandler? _oidVectorHandler;
        PgLsnHandler? _pgLsnHandler;
        TidHandler? _tidHandler;
        InternalCharHandler? _internalCharHandler;

        // Special types
        UnknownTypeHandler? _unknownHandler;

        /// <summary>
        /// A dictionary for type OIDs of PG types which aren't known in advance (i.e. extension types)
        /// </summary>
        readonly ConcurrentDictionary<uint, NpgsqlTypeHandler> _extensionHandlers = new();

        readonly ConcurrentDictionary<Type, NpgsqlTypeHandler> _cachedHandlersByClrType = new();

        #endregion Cached handlers

        internal BuiltInTypeHandlerResolver(NpgsqlConnector connector)
        {
            _connector = connector;
            _databaseInfo = connector.DatabaseInfo;

            // Eagerly instantiate some handlers for very common types so we don't need to check later
            _int16Handler = new Int16Handler(PgType("smallint"));
            _int32Handler = new Int32Handler(PgType("integer"));
            _int64Handler = new Int64Handler(PgType("bigint"));
            _doubleHandler = new DoubleHandler(PgType("double precision"));
            _numericHandler = new NumericHandler(PgType("numeric"));
            _textHandler ??= new TextHandler(PgType("text"), _connector);
            _timestampHandler ??= new TimestampHandler(PgType("timestamp without time zone"), _connector.Settings.ConvertInfinityDateTime);
            _timestampTzHandler ??= new TimestampTzHandler(PgType("timestamp with time zone"), _connector.Settings.ConvertInfinityDateTime);
            _dateHandler ??= new DateHandler(PgType("date"), _connector.Settings.ConvertInfinityDateTime);
            _boolHandler ??= new BoolHandler(PgType("boolean"));
            _uuidHandler ??= new UuidHandler(PgType("uuid"));
            _jsonbHandler ??= new JsonHandler(PgType("jsonb"), _connector, isJsonb: true);
        }

        public NpgsqlTypeHandler? ResolveOID(uint oid)
            => oid switch
            {
                // Numeric types
                PostgresTypeOIDs.Int2 => Int16(),
                PostgresTypeOIDs.Int4 => Int32(),
                PostgresTypeOIDs.Int8 => Int64(),
                PostgresTypeOIDs.Float4 => Single(),
                PostgresTypeOIDs.Float8 => Double(),
                PostgresTypeOIDs.Numeric => Numeric(),
                PostgresTypeOIDs.Money => Money(),

                // Text types
                PostgresTypeOIDs.Text => Text(),
                PostgresTypeOIDs.Xml => Xml(),
                PostgresTypeOIDs.Varchar => Varchar(),
                PostgresTypeOIDs.BPChar => Char(),
                PostgresTypeOIDs.Name => Name(),
                PostgresTypeOIDs.Refcursor => Refcursor(),
                PostgresTypeOIDs.Jsonb => Jsonb(),
                PostgresTypeOIDs.Json => Json(),
                PostgresTypeOIDs.JsonPath => JsonPath(),

                // Date/time types
                PostgresTypeOIDs.Timestamp => Timestamp(),
                PostgresTypeOIDs.TimestampTz => TimestampTz(),
                PostgresTypeOIDs.Date => Date(),
                PostgresTypeOIDs.Time => Time(),
                PostgresTypeOIDs.TimeTz => TimeTz(),
                PostgresTypeOIDs.Interval => Interval(),

                // Network types
                PostgresTypeOIDs.Cidr => Cidr(),
                PostgresTypeOIDs.Inet => Inet(),
                PostgresTypeOIDs.Macaddr => Macaddr(),
                PostgresTypeOIDs.Macaddr8 => Macaddr8(),

                // Full-text search types
                PostgresTypeOIDs.TsQuery => TsQuery(),
                PostgresTypeOIDs.TsVector => TsVector(),

                // Geometry types
                PostgresTypeOIDs.Box => Box(),
                PostgresTypeOIDs.Circle => Circle(),
                PostgresTypeOIDs.Line => Line(),
                PostgresTypeOIDs.LSeg => LineSegment(),
                PostgresTypeOIDs.Path => Path(),
                PostgresTypeOIDs.Point => Point(),
                PostgresTypeOIDs.Polygon => Polygon(),

                // UInt types
                PostgresTypeOIDs.Oid => OID(),
                PostgresTypeOIDs.Xid => Xid(),
                PostgresTypeOIDs.Xid8 => Xid8(),
                PostgresTypeOIDs.Cid => Cid(),
                PostgresTypeOIDs.Regtype => Regtype(),
                PostgresTypeOIDs.Regconfig => Regconfig(),

                // Misc types
                PostgresTypeOIDs.Bool => Bool(),
                PostgresTypeOIDs.Bytea => Bytea(),
                PostgresTypeOIDs.Uuid => Uuid(),
                PostgresTypeOIDs.Varbit => Varbit(),
                PostgresTypeOIDs.Bit => Bit(),
                PostgresTypeOIDs.Record => Record(),
                PostgresTypeOIDs.Void => Void(),

                // Internal types
                PostgresTypeOIDs.Int2vector => Int2Vector(),
                PostgresTypeOIDs.Oidvector => OIDVector(),
                PostgresTypeOIDs.PgLsn => PgLsn(),
                PostgresTypeOIDs.Tid => Tid(),
                PostgresTypeOIDs.Char => InternalChar(),

                // This isn't a well-known type with a fixed type OID.
                _ => _extensionHandlers.TryGetValue(oid, out var handler)
                    ? handler
                    : _databaseInfo.ByOID.TryGetValue(oid, out var pgType)
                      && pgType.Name != "pg_catalog"
                      && (handler = ResolveDataTypeName(pgType.Name)) is not null
                        ? _extensionHandlers[oid] = handler
                        : null
            };

        public NpgsqlTypeHandler? ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType)
            => npgsqlDbType switch
            {
                // Numeric types
                // TODO: this throws if type not found. Maybe that's OK.
                NpgsqlDbType.Smallint => Int16(),
                NpgsqlDbType.Integer => Int32(),
                NpgsqlDbType.Bigint => Int64(),
                NpgsqlDbType.Real => Single(),
                NpgsqlDbType.Double => Double(),
                NpgsqlDbType.Numeric => Numeric(),
                NpgsqlDbType.Money => Money(),

                // Text types
                NpgsqlDbType.Text => Text(),
                NpgsqlDbType.Xml => Xml(),
                NpgsqlDbType.Varchar => Varchar(),
                NpgsqlDbType.Char => Char(),
                NpgsqlDbType.Name => Name(),
                NpgsqlDbType.Refcursor => Refcursor(),
                NpgsqlDbType.Citext => Citext(),
                NpgsqlDbType.Jsonb => Jsonb(),
                NpgsqlDbType.Json => Json(),
                NpgsqlDbType.JsonPath => JsonPath(),

                // Date/time types
                NpgsqlDbType.Timestamp => Timestamp(),
                NpgsqlDbType.TimestampTz => TimestampTz(),
                NpgsqlDbType.Date => Date(),
                NpgsqlDbType.Time => Time(),
                NpgsqlDbType.TimeTz => TimeTz(),
                NpgsqlDbType.Interval => Interval(),

                // Network types
                NpgsqlDbType.Cidr => Cidr(),
                NpgsqlDbType.Inet => Inet(),
                NpgsqlDbType.MacAddr => Macaddr(),
                NpgsqlDbType.MacAddr8 => Macaddr8(),

                // Full-text search types
                NpgsqlDbType.TsQuery => TsQuery(),
                NpgsqlDbType.TsVector => TsVector(),

                // Geometry types
                NpgsqlDbType.Box => Box(),
                NpgsqlDbType.Circle => Circle(),
                NpgsqlDbType.Line => Line(),
                NpgsqlDbType.LSeg => LineSegment(),
                NpgsqlDbType.Path => Path(),
                NpgsqlDbType.Point => Point(),
                NpgsqlDbType.Polygon => Polygon(),

                // LTree types
                NpgsqlDbType.LQuery => LQuery(),
                NpgsqlDbType.LTree => LTree(),
                NpgsqlDbType.LTxtQuery => LTxtQuery(),

                // UInt types
                NpgsqlDbType.Oid => OID(),
                NpgsqlDbType.Xid => Xid(),
                NpgsqlDbType.Xid8 => Xid8(),
                NpgsqlDbType.Cid => Cid(),
                NpgsqlDbType.Regtype => Regtype(),
                NpgsqlDbType.Regconfig => Regconfig(),

                // Misc types
                NpgsqlDbType.Boolean => Bool(),
                NpgsqlDbType.Bytea => Bytea(),
                NpgsqlDbType.Uuid => Uuid(),
                NpgsqlDbType.Varbit => Varbit(),
                NpgsqlDbType.Bit => Bit(),
                NpgsqlDbType.Hstore => Hstore(),

                // Internal types
                NpgsqlDbType.Int2Vector => Int2Vector(),
                NpgsqlDbType.Oidvector => OIDVector(),
                NpgsqlDbType.PgLsn => PgLsn(),
                NpgsqlDbType.Tid => Tid(),
                NpgsqlDbType.InternalChar => InternalChar(),

                // Special types
                NpgsqlDbType.Unknown => Unknown(),

                _ => null
            };

        public NpgsqlTypeHandler? ResolveDataTypeName(string typeName)
            => typeName switch
            {
                // Numeric types
                "smallint" => Int16(),
                "integer" or "int" => Int32(),
                "bigint" => Int64(),
                "real" => Single(),
                "double precision" => Double(),
                "numeric" or "decimal" => Numeric(),
                "money" => Money(),

                // Text types
                "text" => Text(),
                "xml" => Xml(),
                "varchar" or "character varying" => Varchar(),
                "character" => Char(), // Note that "char" is mapped to the internal type
                "name" => Name(),
                "refcursor" => Refcursor(),
                "citext" => Citext(),
                "jsonb" => Jsonb(),
                "json" => Json(),
                "jsonpath" => JsonPath(),

                // Date/time types
                "timestamp" or "timestamp without time zone" => Timestamp(),
                "timestamptz" or "timestamp with time zone" => TimestampTz(),
                "date" => Date(),
                "time" => Time(),
                "timetz" => TimeTz(),
                "interval" => Interval(),

                // Network types
                "cidr" => Cidr(),
                "inet" => Inet(),
                "macaddr" => Macaddr(),
                "macaddr8" => Macaddr8(),

                // Full-text search types
                "tsquery" => TsQuery(),
                "tsvector" => TsVector(),

                // Geometry types
                "box" => Box(),
                "circle" => Circle(),
                "line" => Line(),
                "lseg" => LineSegment(),
                "path" => Path(),
                "point" => Point(),
                "polygon" => Polygon(),

                // LTree types
                "lquery" => LQuery(),
                "ltree" => LTree(),
                "ltxtquery" => LTxtQuery(),

                // UInt types
                "oid" => OID(),
                "xid" => Xid(),
                "xid8" => Xid8(),
                "cid" => Cid(),
                "regtype" => Regtype(),
                "regconfig" => Regconfig(),

                // Misc types
                "bool" or "boolean" => Bool(),
                "bytea" => Bytea(),
                "uuid" => Uuid(),
                "bit varying" or "varbit" => Varbit(),
                "bit" => Bit(),
                "hstore" => Hstore(),

                // Internal types
                "int2vector" => Int2Vector(),
                "oidvector" => OIDVector(),
                "pg_lsn" => PgLsn(),
                "tid" => Tid(),
                "char" => InternalChar(),

                _ => null
            };

        // TODO: Make sure we actually need a version for Type, rather than for object. Would simplify for the date/time types, and we
        // can also do a simple switch.
        public NpgsqlTypeHandler? ResolveClrType(Type type)
        {
            if (_cachedHandlersByClrType.TryGetValue(type, out var handler))
                return handler;

            // Numeric types
            if (!type.IsEnum)
            {
                if (type == typeof(byte))
                    return _cachedHandlersByClrType[typeof(byte)] = Int16();
                if (type == typeof(short))
                    return _cachedHandlersByClrType[typeof(short)] = Int16();
                if (type == typeof(int))
                    return _cachedHandlersByClrType[typeof(int)] = Int32();
                if (type == typeof(long))
                    return _cachedHandlersByClrType[typeof(long)] = Int64();
            }

            if (type == typeof(float))
                return _cachedHandlersByClrType[typeof(float)] = Single();
            if (type == typeof(double))
                return _cachedHandlersByClrType[typeof(double)] = Double();
            if (type == typeof(decimal))
                return _cachedHandlersByClrType[typeof(decimal)] = Numeric();
            if (type == typeof(BigInteger))
                return _cachedHandlersByClrType[typeof(BigInteger)] = Numeric();

            // Text types
            if (type == typeof(string))
                return _cachedHandlersByClrType[typeof(string)] = Text();
            if (type == typeof(char[]))
                return _cachedHandlersByClrType[typeof(char[])] = Text();
            if (type == typeof(char) && !type.IsEnum)
                return _cachedHandlersByClrType[typeof(char)] = Text();
            if (type == typeof(ArraySegment<char>))
                return _cachedHandlersByClrType[typeof(char)] = Text();
            if (type == typeof(JsonDocument))
                return _cachedHandlersByClrType[typeof(JsonDocument)] = Jsonb();

            // Date/time types
            if (type == typeof(DateTime))
                return _cachedHandlersByClrType[typeof(DateTime)] = Timestamp();
            if (type == typeof(NpgsqlDateTime))
                return _cachedHandlersByClrType[typeof(NpgsqlDateTime)] = Timestamp();
            if (type == typeof(DateTimeOffset))
                return _cachedHandlersByClrType[typeof(DateTimeOffset)] = TimestampTz();
            if (type == typeof(NpgsqlDate))
                return _cachedHandlersByClrType[typeof(NpgsqlDate)] = Date();
#if NET6_0_OR_GREATER
            if (type == typeof(DateOnly))
                return _cachedHandlersByClrType[typeof(DateOnly)] = Date();
            if (type == typeof(TimeOnly))
                return _cachedHandlersByClrType[typeof(TimeOnly)] = Time();
#endif
            if (type == typeof(TimeSpan))
                return _cachedHandlersByClrType[typeof(TimeSpan)] = Interval();
            if (type == typeof(NpgsqlTimeSpan))
                return _cachedHandlersByClrType[typeof(NpgsqlTimeSpan)] = Interval();

            // Network types
            if (type == typeof(IPAddress))
                return _cachedHandlersByClrType[typeof(IPAddress)] = Inet();
            if (type == _readonlyIpType)
                return _cachedHandlersByClrType[_readonlyIpType] = Inet();
            if (type == typeof((IPAddress Address, int Subnet)))
                return _cachedHandlersByClrType[typeof((IPAddress Address, int Subnet))] = Inet();
#pragma warning disable 618
            if (type == typeof(NpgsqlInet))
                return _cachedHandlersByClrType[typeof(NpgsqlInet)] = Inet();
#pragma warning restore 618
            if (type == typeof(PhysicalAddress))
                return _cachedHandlersByClrType[typeof(PhysicalAddress)] = Macaddr();

            // Full-text types
            if (type == typeof(NpgsqlTsQuery))
                return _cachedHandlersByClrType[typeof(NpgsqlTsQuery)] = TsQuery();
            if (type == typeof(NpgsqlTsVector))
                return _cachedHandlersByClrType[typeof(NpgsqlTsVector)] = TsVector();

            // Geometry types
            if (type == typeof(NpgsqlBox))
                return _cachedHandlersByClrType[typeof(NpgsqlBox)] = Box();
            if (type == typeof(NpgsqlCircle))
                return _cachedHandlersByClrType[typeof(NpgsqlCircle)] = Circle();
            if (type == typeof(NpgsqlLine))
                return _cachedHandlersByClrType[typeof(NpgsqlLine)] = Line();
            if (type == typeof(NpgsqlLSeg))
                return _cachedHandlersByClrType[typeof(NpgsqlLSeg)] = LineSegment();
            if (type == typeof(NpgsqlPath))
                return _cachedHandlersByClrType[typeof(NpgsqlPath)] = Path();
            if (type == typeof(NpgsqlPoint))
                return _cachedHandlersByClrType[typeof(NpgsqlPoint)] = Point();
            if (type == typeof(NpgsqlPolygon))
                return _cachedHandlersByClrType[typeof(NpgsqlPolygon)] = Polygon();

            // Misc types
            if (type == typeof(bool))
                return _cachedHandlersByClrType[typeof(bool)] = Bool();
            if (type == typeof(byte[]))
                return _cachedHandlersByClrType[typeof(byte[])] = Bytea();
            if (type == typeof(ArraySegment<byte>))
                return _cachedHandlersByClrType[typeof(ArraySegment<byte>)] = Bytea();
#if !NETSTANDARD2_0
            if (type == typeof(ReadOnlyMemory<byte>))
                return _cachedHandlersByClrType[typeof(ReadOnlyMemory<byte>)] = Bytea();
            if (type == typeof(Memory<byte>))
                return _cachedHandlersByClrType[typeof(Memory<byte>)] = Bytea();
#endif
            if (type == typeof(Guid))
                return _cachedHandlersByClrType[typeof(Guid)] = Uuid();
            if (type == typeof(BitArray))
                return _cachedHandlersByClrType[typeof(BitArray)] = Varbit();
            if (type == typeof(BitVector32))
                return _cachedHandlersByClrType[typeof(BitVector32)] = Varbit();
            if (type == typeof(Dictionary<string, string>))
                return _cachedHandlersByClrType[typeof(Dictionary<string, string>)] = Hstore();
            if (type == typeof(IDictionary<string, string>))
                return _cachedHandlersByClrType[typeof(IDictionary<string, string>)] = Hstore();
#if !NETSTANDARD2_0 && !NETSTANDARD2_1
            if (type == typeof(ImmutableDictionary<string, string>))
                return _cachedHandlersByClrType[typeof(ImmutableDictionary<string, string>)] = Hstore();
#endif

            // Internal types
            if (type == typeof(NpgsqlLogSequenceNumber))
                return _cachedHandlersByClrType[typeof(NpgsqlLogSequenceNumber)] = PgLsn();
            if (type == typeof(NpgsqlTid))
                return _cachedHandlersByClrType[typeof(NpgsqlTid)] = Tid();
            if (type == typeof(DBNull))
                return _cachedHandlersByClrType[typeof(DBNull)] = Unknown();

            return null;
        }

        #region Handler creation

        // Numeric types
        NpgsqlTypeHandler Int16()   => _int16Handler;
        NpgsqlTypeHandler Int32()   => _int32Handler;
        NpgsqlTypeHandler Int64()   => _int64Handler;
        NpgsqlTypeHandler Single()  => _singleHandler  ??= new SingleHandler(PgType("real"));
        NpgsqlTypeHandler Double()  => _doubleHandler;
        NpgsqlTypeHandler Numeric() => _numericHandler;
        NpgsqlTypeHandler Money()   => _moneyHandler   ??= new MoneyHandler(PgType("money"));

        // Text types
        NpgsqlTypeHandler Text()      => _textHandler;
        NpgsqlTypeHandler Xml()       => _xmlHandler ??= new TextHandler(PgType("xml"), _connector);
        NpgsqlTypeHandler Varchar()   => _varcharHandler ??= new TextHandler(PgType("character varying"), _connector);
        NpgsqlTypeHandler Char()      => _charHandler ??= new TextHandler(PgType("character"), _connector);
        NpgsqlTypeHandler Name()      => _nameHandler ??= new TextHandler(PgType("name"), _connector);
        NpgsqlTypeHandler Refcursor() => _refcursorHandler ??= new TextHandler(PgType("refcursor"), _connector);
        NpgsqlTypeHandler Citext()    => _citextHandler ??= new TextHandler(PgType("citext"), _connector);
        NpgsqlTypeHandler Jsonb()     => _jsonbHandler;
        NpgsqlTypeHandler Json()      => _jsonHandler ??= new JsonHandler(PgType("json"), _connector, isJsonb: false);
        NpgsqlTypeHandler JsonPath()  => _jsonPathHandler ??= new JsonPathHandler(PgType("jsonpath"), _connector);

        // Date/time types
        NpgsqlTypeHandler Timestamp()   => _timestampHandler;
        NpgsqlTypeHandler TimestampTz() => _timestampTzHandler;
        NpgsqlTypeHandler Date()        => _dateHandler;
        NpgsqlTypeHandler Time()        => _timeHandler ??= new TimeHandler(PgType("time without time zone"));
        NpgsqlTypeHandler TimeTz()      => _timeTzHandler ??= new TimeTzHandler(PgType("time with time zone"));
        NpgsqlTypeHandler Interval()    => _intervalHandler ??= new IntervalHandler(PgType("interval"));

        // Network types
        NpgsqlTypeHandler Cidr()     => _cidrHandler ??= new CidrHandler(PgType("cidr"));
        NpgsqlTypeHandler Inet()     => _inetHandler ??= new InetHandler(PgType("inet"));
        NpgsqlTypeHandler Macaddr()  => _macaddrHandler ??= new MacaddrHandler(PgType("macaddr"));
        NpgsqlTypeHandler Macaddr8() => _macaddr8Handler ??= new MacaddrHandler(PgType("macaddr8"));

        // Full-text search types
        NpgsqlTypeHandler TsQuery()  => _tsQueryHandler ??= new TsQueryHandler(PgType("tsquery"));
        NpgsqlTypeHandler TsVector() => _tsVectorHandler ??= new TsVectorHandler(PgType("tsvector"));

        // Geometry types
        NpgsqlTypeHandler Box()         => _boxHandler ??= new BoxHandler(PgType("box"));
        NpgsqlTypeHandler Circle()      => _circleHandler ??= new CircleHandler(PgType("circle"));
        NpgsqlTypeHandler Line()        => _lineHandler ??= new LineHandler(PgType("line"));
        NpgsqlTypeHandler LineSegment() => _lineSegmentHandler ??= new LineSegmentHandler(PgType("lseg"));
        NpgsqlTypeHandler Path()        => _pathHandler ??= new PathHandler(PgType("path"));
        NpgsqlTypeHandler Point()       => _pointHandler ??= new PointHandler(PgType("point"));
        NpgsqlTypeHandler Polygon()     => _polygonHandler ??= new PolygonHandler(PgType("polygon"));

        // LTree types
        NpgsqlTypeHandler LQuery()    => _lQueryHandler ??= new LQueryHandler(PgType("lquery"), _connector);
        NpgsqlTypeHandler LTree()     => _lTreeHandler ??= new LTreeHandler(PgType("ltree"), _connector);
        NpgsqlTypeHandler LTxtQuery() => _lTxtQueryHandler ??= new LTxtQueryHandler(PgType("ltxtquery"), _connector);

        // UInt types
        NpgsqlTypeHandler OID()       => _oidHandler ??= new UInt32Handler(PgType("oid"));
        NpgsqlTypeHandler Xid()       => _xidHandler ??= new UInt32Handler(PgType("xid"));
        NpgsqlTypeHandler Xid8()      => _xid8Handler ??= new UInt64Handler(PgType("xid8"));
        NpgsqlTypeHandler Cid()       => _cidHandler ??= new UInt32Handler(PgType("cid"));
        NpgsqlTypeHandler Regtype()   => _regtypeHandler ??= new UInt32Handler(PgType("regtype"));
        NpgsqlTypeHandler Regconfig() => _regconfigHandler ??= new UInt32Handler(PgType("regconfig"));
        
        // Misc types
        NpgsqlTypeHandler Bool()    => _boolHandler;
        NpgsqlTypeHandler Bytea()   => _byteaHandler ??= new ByteaHandler(PgType("bytea"));
        NpgsqlTypeHandler Uuid()    => _uuidHandler;
        NpgsqlTypeHandler Varbit()  => _bitVaryingHandler ??= new BitStringHandler(PgType("bit varying"));
        NpgsqlTypeHandler Bit()     => _bitHandler ??= new BitStringHandler(PgType("bit"));
        NpgsqlTypeHandler Record()  => _recordHandler ??= new RecordHandler(PgType("record"), _connector.TypeMapper);
        NpgsqlTypeHandler Void()    => _voidHandler ??= new VoidHandler(PgType("void"));
        NpgsqlTypeHandler Hstore()  => _hstoreHandler ??= new HstoreHandler(PgType("hstore"), _textHandler);
        NpgsqlTypeHandler Unknown() => _unknownHandler ??= new UnknownTypeHandler(_connector);

        // Internal types
        NpgsqlTypeHandler Int2Vector()   => _int2VectorHandler ??= new Int2VectorHandler(PgType("int2vector"), PgType("smallint"));
        NpgsqlTypeHandler OIDVector()    => _oidVectorHandler ??= new OIDVectorHandler(PgType("oidvector"), PgType("oid"));
        NpgsqlTypeHandler PgLsn()        => _pgLsnHandler ??= new PgLsnHandler(PgType("pg_lsn"));
        NpgsqlTypeHandler Tid()          => _tidHandler ??= new TidHandler(PgType("tid"));
        NpgsqlTypeHandler InternalChar() => _internalCharHandler ??= new InternalCharHandler(PgType("char"));

        #endregion Handler creation

        PostgresType PgType(string pgTypeName) => _databaseInfo.GetPostgresTypeByName(pgTypeName);
    }
}
