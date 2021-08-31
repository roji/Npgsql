using System;
using NetTopologySuite;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.NetTopologySuite.Internal
{
    public class NetTopologySuiteTypeHandlerResolver : ITypeHandlerResolver
    {
        readonly NpgsqlDatabaseInfo _databaseInfo;
        readonly bool _geographyAsDefault;

        readonly NetTopologySuiteHandler _geometryHandler, _geographyHAndler;
        readonly uint _geometryOid, _geographyOid;

        internal NetTopologySuiteTypeHandlerResolver(
            NpgsqlConnector connector,
            CoordinateSequenceFactory? coordinateSequenceFactory = null,
            PrecisionModel? precisionModel = null,
            Ordinates handleOrdinates = Ordinates.None,
            bool geographyAsDefault = false)
        {
            _databaseInfo = connector.DatabaseInfo;
            _geographyAsDefault = geographyAsDefault;

            // TODO: Should bomb correctly if the types aren't there
            var (pgGeometryType, pgGeographyType) = (PgType("geometry"), PgType("geography"));
            (_geometryOid, _geographyOid) = (pgGeometryType.OID, pgGeographyType.OID);

            coordinateSequenceFactory ??= NtsGeometryServices.Instance.DefaultCoordinateSequenceFactory;
            precisionModel ??= NtsGeometryServices.Instance.DefaultPrecisionModel;
            if (handleOrdinates == Ordinates.None)
                handleOrdinates = coordinateSequenceFactory.Ordinates;


            // TODO: In multiplexing, these are used concurrently... not sure they're thread-safe :(
            var reader = new PostGisReader(coordinateSequenceFactory, precisionModel, handleOrdinates);
            var writer = new PostGisWriter();

            _geometryHandler = new NetTopologySuiteHandler(pgGeometryType, reader, writer);
            _geographyHAndler = new NetTopologySuiteHandler(pgGeographyType, reader, writer);
        }

        public NpgsqlTypeHandler? ResolveOID(uint oid)
            => oid == _geometryOid
                ? _geometryHandler
                : oid == _geographyOid
                    ? _geographyHAndler
                    : null;

        public NpgsqlTypeHandler? ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType)
            => npgsqlDbType switch
            {
                NpgsqlDbType.Geometry => _geometryHandler,
                NpgsqlDbType.Geography => _geographyHAndler,
                _ => null
            };

        public NpgsqlTypeHandler? ResolveDataTypeName(string typeName)
            => typeName switch
            {
                "geometry" => _geometryHandler,
                "geography" => _geographyHAndler,
                _ => null
            };

        public NpgsqlTypeHandler? ResolveClrType(Type type)
            => type != typeof(Geometry) && type.BaseType != typeof(Geometry) && type.BaseType != typeof(GeometryCollection)
                ? null
                : _geographyAsDefault
                    ? _geographyHAndler
                    : _geometryHandler;

        PostgresType PgType(string pgTypeName) => _databaseInfo.GetPostgresTypeByName(pgTypeName);
    }
}
