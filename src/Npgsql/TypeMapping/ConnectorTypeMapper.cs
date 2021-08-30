using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandlers;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

namespace Npgsql.TypeMapping
{
    sealed class ConnectorTypeMapper : TypeMapperBase
    {
        readonly NpgsqlConnector _connector;
        readonly object _writeLock = new();

        NpgsqlDatabaseInfo? _databaseInfo;

        internal NpgsqlDatabaseInfo DatabaseInfo
        {
            get => _databaseInfo ?? throw new InvalidOperationException("Internal error: this type mapper hasn't yet been bound to a database info object");
            set => _databaseInfo = value;
        }

        internal ITypeHandlerResolver[] _resolvers;
        internal NpgsqlTypeHandler UnrecognizedTypeHandler { get; }

        internal IDictionary<string, NpgsqlTypeMapping> MappingsByName { get; private set; }
        internal IDictionary<NpgsqlDbType, NpgsqlTypeMapping> MappingsByNpgsqlDbType { get; private set; }
        internal IDictionary<Type, NpgsqlTypeMapping> MappingsByClrType { get; private set; }

        readonly ConcurrentDictionary<uint, NpgsqlTypeHandler> _handlersByOID = new();
        readonly ConcurrentDictionary<NpgsqlDbType, NpgsqlTypeHandler> _handlersByNpgsqlDbType = new();
        readonly ConcurrentDictionary<string, NpgsqlTypeHandler> _handlersByTypeName = new();
        readonly ConcurrentDictionary<Type, NpgsqlTypeHandler> _handlersByClrType = new();
        readonly ConcurrentDictionary<Type, NpgsqlTypeHandler> _arrayHandlerByClrType = new();

        /// <summary>
        /// Copy of <see cref="GlobalTypeMapper.ChangeCounter"/> at the time when this
        /// mapper was created, to detect mapping changes. If changes are made to this connection's
        /// mapper, the change counter is set to -1.
        /// </summary>
        internal int ChangeCounter { get; private set; }

        #region Construction

        internal ConnectorTypeMapper(NpgsqlConnector connector) : base(GlobalTypeMapper.Instance.DefaultNameTranslator)
        {
            _connector = connector;
            UnrecognizedTypeHandler = new UnknownTypeHandler(_connector);
            Reset();
        }

        #endregion Constructors

        #region Type handler lookup

        readonly ConcurrentDictionary<uint, NpgsqlTypeHandler> _extraHandlersByOID = new();
        readonly ConcurrentDictionary<NpgsqlDbType, NpgsqlTypeHandler> _extraHandlersByNpgsqlDbType = new();
        readonly ConcurrentDictionary<Type, NpgsqlTypeHandler> _extraHandlersByClrType = new();
        readonly ConcurrentDictionary<string, NpgsqlTypeHandler> _extraHandlersByDataTypeName = new();

        /// <summary>
        /// Looks up a type handler by its PostgreSQL type's OID.
        /// </summary>
        /// <param name="oid">A PostgreSQL type OID</param>
        /// <returns>A type handler that can be used to encode and decode values.</returns>
        internal NpgsqlTypeHandler ResolveOID(uint oid)
            => TryResolveOID(oid, out var result) ? result : UnrecognizedTypeHandler;

        internal bool TryResolveOID(uint oid, [NotNullWhen(true)] out NpgsqlTypeHandler? handler)
        {
            foreach (var resolver in _resolvers)
                if (resolver.ResolveOID(oid, out handler))
                    return true;

            if (_extraHandlersByOID.TryGetValue(oid, out handler))
                return true;

            if (!DatabaseInfo.ByOID.TryGetValue(oid, out var pgType))
                return false;

            switch (pgType)
            {
            case PostgresArrayType pgArrayType:
            {
                // TODO: This will throw, but we're in TryResolve. Figure out the whole Try/non-Try strategy here.
                var elementHandler = ResolveOID(pgArrayType.Element.OID);
                handler = _extraHandlersByOID[oid] =
                    elementHandler.CreateArrayHandler(pgArrayType, _connector.Settings.ArrayNullabilityMode);
                return true;
            }

            case PostgresRangeType pgRangeType:
            {
                // TODO: This will throw, but we're in TryResolve. Figure out the whole Try/non-Try strategy here.
                var subtypeHandler = ResolveOID(pgRangeType.Subtype.OID);
                handler = _extraHandlersByOID[oid] = (NpgsqlTypeHandler)subtypeHandler.CreateRangeHandler(pgRangeType);
                return true;
            }

            case PostgresMultirangeType pgMultirangeType:
            {
                // TODO: This will throw, but we're in TryResolve. Figure out the whole Try/non-Try strategy here.
                var subtypeHandler = ResolveOID(pgMultirangeType.Subrange.Subtype.OID);
                handler = _extraHandlersByOID[oid] = (NpgsqlTypeHandler)subtypeHandler.CreateMultirangeHandler(pgMultirangeType);
                return true;
            }

            case PostgresEnumType pgEnumType:
            {
                // A mapped enum would have been registered in _extraHandlersByOID and returned above - this is unmapped.
                handler = _extraHandlersByOID[oid] = new UnmappedEnumHandler(pgEnumType, DefaultNameTranslator, _connector);
                return true;
            }

            case PostgresDomainType pgDomainType:
            {
                // Note that when when sending back domain types, PG sends back the type OID of their base type - so in regular
                // circumstances we never need to resolve domains from a type OID.
                // However, when a domain is part of a composite type, the domain's type OID is sent, so we support this here.
                // TODO: This will throw, but we're in TryResolve. Figure out the whole Try/non-Try strategy here.
                handler = _extraHandlersByOID[oid] = ResolveOID(pgDomainType.BaseType.OID);
                return true;
            }

            default:
                handler = null;
                return false;
            }
        }

        // internal bool TryGetByOID(uint oid, [NotNullWhen(true)] out NpgsqlTypeHandler? handler)
        // {
        //     if (_handlersByOID.TryGetValue(oid, out handler))
        //         return true;
        //
        //     if (!DatabaseInfo.ByOID.TryGetValue(oid, out var pgType))
        //         return false;
        //
        //     if (MappingsByName.TryGetValue(pgType.Name, out var mapping))
        //     {
        //         handler = GetOrBindBaseHandler(mapping, pgType);
        //         return true;
        //     }
        //
        //     switch (pgType)
        //     {
        //     case PostgresArrayType pgArrayType when GetMapping(pgArrayType.Element) is { } elementMapping:
        //         handler = GetOrBindArrayHandler(elementMapping);
        //         return true;
        //
        //     case PostgresRangeType pgRangeType when GetMapping(pgRangeType.Subtype) is { } subtypeMapping:
        //         handler = GetOrBindRangeHandler(subtypeMapping);
        //         return true;
        //
        //     case PostgresMultirangeType pgMultirangeType when GetMapping(pgMultirangeType.Subrange.Subtype) is { } subtypeMapping:
        //         handler = GetOrBindMultirangeHandler(subtypeMapping);
        //         return true;
        //
        //     case PostgresEnumType pgEnumType:
        //         // A mapped enum would have been registered in InternalMappings and bound above - this is unmapped.
        //         handler = GetOrBindUnmappedEnumHandler(pgEnumType);
        //         return true;
        //
        //     case PostgresArrayType { Element: PostgresEnumType } pgArrayType:
        //         // Array over unmapped enum
        //         handler = GetOrBindUnmappedEnumArrayHandler(pgArrayType);
        //         return true;
        //
        //     case PostgresDomainType pgDomainType:
        //         // Note that when when sending back domain types, PG sends back the type OID of their base type - so in regular
        //         // circumstances we never need to resolve domains from a type OID.
        //         // However, when a domain is part of a composite type, the domain's type OID is sent, so we support this here.
        //         if (TryGetByOID(pgDomainType.BaseType.OID, out handler))
        //         {
        //             _handlersByOID[oid] = handler;
        //             return true;
        //         }
        //         return false;
        //
        //     default:
        //         return false;
        //     }
        // }

        internal NpgsqlTypeHandler ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType)
        {
            if (TryResolve(npgsqlDbType, out var handler) || _extraHandlersByNpgsqlDbType.TryGetValue(npgsqlDbType, out handler))
                return handler;

            if (npgsqlDbType.HasFlag(NpgsqlDbType.Array))
            {
                if (!TryResolve(npgsqlDbType & ~NpgsqlDbType.Array, out var elementHandler))
                    throw new ArgumentException($"Array type over NpgsqlDbType {npgsqlDbType} isn't supported by Npgsql");

                if (elementHandler.PostgresType.Array is not { } pgArrayType)
                    throw new ArgumentException($"No array type could be found in the database for element {elementHandler.PostgresType}");

                return _extraHandlersByNpgsqlDbType[npgsqlDbType] =
                    elementHandler.CreateArrayHandler(pgArrayType, _connector.Settings.ArrayNullabilityMode);
            }

            if (npgsqlDbType.HasFlag(NpgsqlDbType.Range))
            {
                if (!TryResolve(npgsqlDbType & ~NpgsqlDbType.Range, out var subtypeHandler))
                    throw new ArgumentException($"Range type over NpgsqlDbType {npgsqlDbType} isn't supported by Npgsql");

                if (subtypeHandler.PostgresType.Range is not { } pgRangeType)
                    throw new ArgumentException($"No range type could be found in the database for subtype {subtypeHandler.PostgresType}");

                return _extraHandlersByNpgsqlDbType[npgsqlDbType] = (NpgsqlTypeHandler)subtypeHandler.CreateRangeHandler(pgRangeType);
            }

            if (npgsqlDbType.HasFlag(NpgsqlDbType.Multirange))
            {
                if (!TryResolve(npgsqlDbType & ~NpgsqlDbType.Multirange, out var subtypeHandler))
                    throw new ArgumentException($"Multirange type over NpgsqlDbType {npgsqlDbType} isn't supported by Npgsql");

                if (subtypeHandler.PostgresType.Range?.Multirange is not { } pgMultirangeType)
                    throw new ArgumentException($"No multirange type could be found in the database for subtype {subtypeHandler.PostgresType}");

                return _extraHandlersByNpgsqlDbType[npgsqlDbType] = (NpgsqlTypeHandler)subtypeHandler.CreateMultirangeHandler(pgMultirangeType);
            }

            throw new NpgsqlException($"The NpgsqlDbType '{npgsqlDbType}' isn't present in your database. " +
                                      "You may need to install an extension or upgrade to a newer version.");

            bool TryResolve(NpgsqlDbType npgsqlDbType, [NotNullWhen(true)] out NpgsqlTypeHandler? handler)
            {
                foreach (var resolver in _resolvers)
                    if ((handler = resolver.ResolveNpgsqlDbType(npgsqlDbType)) is not null)
                        return true;
                handler = null;
                return false;
            }
        }

        // internal NpgsqlTypeHandler GetByNpgsqlDbType(NpgsqlDbType npgsqlDbType)
        // {
        //     if (_handlersByNpgsqlDbType.TryGetValue(npgsqlDbType, out var handler))
        //         return handler;
        //
        //     // TODO: revisit externalCall - things are changing. No more "binding at global time" which only needs to log - always throw?
        //     if (MappingsByNpgsqlDbType.TryGetValue(npgsqlDbType, out var mapping))
        //         return GetOrBindBaseHandler(mapping);
        //
        //     if (npgsqlDbType.HasFlag(NpgsqlDbType.Array))
        //     {
        //         var elementNpgsqlDbType = npgsqlDbType & ~NpgsqlDbType.Array;
        //
        //         return MappingsByNpgsqlDbType.TryGetValue(elementNpgsqlDbType, out var elementMapping)
        //             ? GetOrBindArrayHandler(elementMapping)
        //             : throw new ArgumentException($"Could not find a mapping for array element NpgsqlDbType {elementNpgsqlDbType}");
        //     }
        //
        //     if (npgsqlDbType.HasFlag(NpgsqlDbType.Range))
        //     {
        //         var subtypeNpgsqlDbType = npgsqlDbType & ~NpgsqlDbType.Range;
        //
        //         return MappingsByNpgsqlDbType.TryGetValue(subtypeNpgsqlDbType, out var subtypeMapping)
        //             ? GetOrBindRangeHandler(subtypeMapping)
        //             : throw new ArgumentException($"Could not find a mapping for range subtype NpgsqlDbType {subtypeNpgsqlDbType}");
        //     }
        //
        //     if (npgsqlDbType.HasFlag(NpgsqlDbType.Multirange))
        //     {
        //         var subtypeNpgsqlDbType = npgsqlDbType & ~NpgsqlDbType.Multirange;
        //
        //         return MappingsByNpgsqlDbType.TryGetValue(subtypeNpgsqlDbType, out var subtypeMapping)
        //             ? GetOrBindMultirangeHandler(subtypeMapping)
        //             : throw new ArgumentException($"Could not find a mapping for range subtype NpgsqlDbType {subtypeNpgsqlDbType}");
        //     }
        //
        //     throw new NpgsqlException($"The NpgsqlDbType '{npgsqlDbType}' isn't present in your database. " +
        //                               "You may need to install an extension or upgrade to a newer version.");
        // }

        internal NpgsqlTypeHandler ResolveDataTypeName(string typeName)
        {
            NpgsqlTypeHandler? handler;

            foreach (var resolver in _resolvers)
                if ((handler = resolver.ResolveDataTypeName(typeName)) is not null)
                    return handler;

            if (_extraHandlersByDataTypeName.TryGetValue(typeName, out handler))
                return handler;

            if (DatabaseInfo.GetPostgresTypeByName(typeName) is not { } pgType)
                throw new NotSupportedException("Could not find PostgreSQL type " + typeName);

            switch (pgType)
            {
            case PostgresArrayType pgArrayType:
            {
                var elementHandler = ResolveOID(pgArrayType.Element.OID);
                return _extraHandlersByDataTypeName[typeName] =
                    elementHandler.CreateArrayHandler(pgArrayType, _connector.Settings.ArrayNullabilityMode);
            }
            case PostgresRangeType pgRangeType:
            {
                var subtypeHandler = ResolveOID(pgRangeType.Subtype.OID);
                return _extraHandlersByDataTypeName[typeName] = (NpgsqlTypeHandler)subtypeHandler.CreateRangeHandler(pgRangeType);
            }
            case PostgresMultirangeType pgMultirangeType:
            {
                var subtypeHandler = ResolveOID(pgMultirangeType.Subrange.Subtype.OID);
                return _extraHandlersByDataTypeName[typeName] = (NpgsqlTypeHandler)subtypeHandler.CreateMultirangeHandler(pgMultirangeType);
            }
            case PostgresEnumType pgEnumType:
            {
                // A mapped enum would have been registered in InternalMappings and bound above - this is unmapped.
                return _extraHandlersByDataTypeName[typeName] = new UnmappedEnumHandler(pgEnumType, DefaultNameTranslator, _connector);
            }

            case PostgresDomainType pgDomainType:
                return _extraHandlersByDataTypeName[typeName] = ResolveOID(pgDomainType.BaseType.OID);

            case PostgresBaseType pgBaseType:
                throw new NotSupportedException($"Could not find PostgreSQL type '{pgBaseType}'");

            default:
                throw new ArgumentOutOfRangeException($"Unhandled PostgreSQL type type: {pgType.GetType()}");
            }

            // bool TryResolve(string typeName, [NotNullWhen(true)] out NpgsqlTypeHandler? handler)
            // {
            //     foreach (var resolver in _resolvers)
            //         if ((handler = resolver.ResolveDataTypeName(typeName)) is not null)
            //             return true;
            //     handler = null;
            //     return false;
            // }
        }

        // internal NpgsqlTypeHandler GetByDataTypeName(string typeName)
        // {
        //     if (_handlersByTypeName.TryGetValue(typeName, out var handler))
        //         return handler;
        //
        //     if (MappingsByName.TryGetValue(typeName, out var mapping))
        //         return GetOrBindBaseHandler(mapping);
        //
        //     if (DatabaseInfo.GetPostgresTypeByName(typeName) is not { } pgType)
        //         throw new NotSupportedException("Could not find PostgreSQL type " + typeName);
        //
        //     return pgType switch
        //     {
        //         PostgresArrayType pgArrayType when GetMapping(pgArrayType.Element) is { } elementMapping
        //             => GetOrBindArrayHandler(elementMapping),
        //         PostgresRangeType pgRangeType when GetMapping(pgRangeType.Subtype) is { } subtypeMapping
        //             => GetOrBindRangeHandler(subtypeMapping),
        //         PostgresMultirangeType pgMultirangeType when GetMapping(pgMultirangeType.Subrange.Subtype) is { } subtypeMapping
        //             => GetOrBindMultirangeHandler(subtypeMapping),
        //         // A mapped enum would have been registered in InternalMappings and bound above - this is unmapped.
        //         PostgresEnumType pgEnumType
        //             => GetOrBindUnmappedEnumHandler(pgEnumType),
        //         // Array over unmapped enum
        //         PostgresArrayType { Element: PostgresEnumType } pgArrayType
        //             => GetOrBindUnmappedEnumArrayHandler(pgArrayType),
        //         PostgresDomainType pgDomainType
        //             => _handlersByTypeName[typeName] = GetByDataTypeName(pgDomainType.BaseType.Name),
        //         // Unmapped base type
        //         PostgresBaseType
        //             => throw new NotSupportedException("Could not find PostgreSQL type " + typeName),
        //
        //         _ => throw new ArgumentOutOfRangeException($"Unhandled PostgreSQL type type: {pgType.GetType()}")
        //     };
        // }

        internal NpgsqlTypeHandler ResolveClrType(Type type)
        {
            NpgsqlTypeHandler? handler;

            foreach (var resolver in _resolvers)
                if ((handler = resolver.ResolveClrType(type)) is not null)
                    return handler;

            if (_extraHandlersByClrType.TryGetValue(type, out handler))
                return handler;

            // Try to see if it is an array type
            var arrayElementType = GetArrayListElementType(type);
            if (arrayElementType is not null)
            {
                // Arrays over range types are multiranges, not regular arrays.
                if (arrayElementType.IsGenericType && arrayElementType.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
                {
                    var subtypeType = arrayElementType.GetGenericArguments()[0];

                    return ResolveClrType(subtypeType) is { PostgresType : { Range : { Multirange: { } pgMultirangeType } } } subtypeHandler
                        ? _extraHandlersByClrType[type] = (NpgsqlTypeHandler)subtypeHandler.CreateMultirangeHandler(pgMultirangeType)
                        : throw new NotSupportedException($"The CLR range type {type} isn't supported by Npgsql or your PostgreSQL.");
                }

                if (ResolveClrType(arrayElementType) is not { } elementHandler)
                    throw new ArgumentException($"Array type over CLR type {arrayElementType.Name} isn't supported by Npgsql");

                if (elementHandler.PostgresType.Array is not { } pgArrayType)
                    throw new ArgumentException($"No array type could be found in the database for element {elementHandler.PostgresType}");

                return _extraHandlersByClrType[type] =
                    elementHandler.CreateArrayHandler(pgArrayType, _connector.Settings.ArrayNullabilityMode);
            }

            if (Nullable.GetUnderlyingType(type) is { } underlyingType && ResolveClrType(underlyingType) is { } underlyingHandler)
                return _extraHandlersByClrType[type] = underlyingHandler;

            if (type.IsEnum)
            {
                return DatabaseInfo.GetPostgresTypeByName(GetPgName(type, DefaultNameTranslator)) is PostgresEnumType pgEnumType
                    ? _extraHandlersByClrType[type] = new UnmappedEnumHandler(pgEnumType, DefaultNameTranslator, _connector)
                    : throw new NotSupportedException(
                        $"Could not find a PostgreSQL enum type corresponding to {type.Name}. " +
                        "Consider mapping the enum before usage, refer to the documentation for more details.");
            }

            // TODO: We can make the following compatible with reflection-free mode by having NpgsqlRange implement some interface, and
            // check for that.
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
            {
                var subtypeType = type.GetGenericArguments()[0];

                return ResolveClrType(subtypeType) is { PostgresType : { Range : { } pgRangeType } } subtypeHandler
                    ? _extraHandlersByClrType[type] = (NpgsqlTypeHandler)subtypeHandler.CreateRangeHandler(pgRangeType)
                    : throw new NotSupportedException($"The CLR range type {type} isn't supported by Npgsql or your PostgreSQL.");
            }

            if (typeof(IEnumerable).IsAssignableFrom(type))
                throw new NotSupportedException("IEnumerable parameters are not supported, pass an array or List instead");

            throw new NotSupportedException($"The CLR type {type} isn't natively supported by Npgsql or your PostgreSQL. " +
                                            $"To use it with a PostgreSQL composite you need to specify {nameof(NpgsqlParameter.DataTypeName)} or to map it, please refer to the documentation.");
        }

        // internal NpgsqlTypeHandler GetByClrType(Type type)
        // {
        //     if (_handlersByClrType.TryGetValue(type, out var handler))
        //         return handler;
        //
        //     if (MappingsByClrType.TryGetValue(type, out var mapping))
        //         return GetOrBindBaseHandler(mapping);
        //
        //     // Try to see if it is an array type
        //     var arrayElementType = GetArrayListElementType(type);
        //     if (arrayElementType is not null)
        //     {
        //         if (_arrayHandlerByClrType.TryGetValue(arrayElementType, out handler))
        //             return handler;
        //
        //         // Arrays over range types are multiranges, not regular arrays.
        //         if (arrayElementType.IsGenericType && arrayElementType.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
        //         {
        //             var subtypeType = arrayElementType.GetGenericArguments()[0];
        //
        //             return MappingsByClrType.TryGetValue(subtypeType, out var subtypeMapping)
        //                 ? GetOrBindMultirangeHandler(subtypeMapping)
        //                 : throw new NotSupportedException($"The CLR multirange type {type} isn't supported by Npgsql or your PostgreSQL.");
        //         }
        //
        //         return MappingsByClrType.TryGetValue(arrayElementType, out var elementMapping)
        //             ? GetOrBindArrayHandler(elementMapping)
        //             : throw new NotSupportedException($"The CLR array type {type} isn't supported by Npgsql or your PostgreSQL. " +
        //                                               "If you wish to map it to a PostgreSQL composite type array you need to register " +
        //                                               "it before usage, please refer to the documentation.");
        //     }
        //
        //     if (Nullable.GetUnderlyingType(type) is { } underlyingType && GetByClrType(underlyingType) is { } underlyingHandler)
        //         return _handlersByClrType[type] = underlyingHandler;
        //
        //     if (type.IsEnum)
        //     {
        //         return DatabaseInfo.GetPostgresTypeByName(GetPgName(type, DefaultNameTranslator)) is PostgresEnumType pgEnumType
        //             ? GetOrBindUnmappedEnumHandler(pgEnumType)
        //             : throw new NotSupportedException(
        //                 $"Could not find a PostgreSQL enum type corresponding to {type.Name}. " +
        //                 "Consider mapping the enum before usage, refer to the documentation for more details.");
        //     }
        //
        //     // TODO: We can make the following compatible with reflection-free mode by having NpgsqlRange implement some interface, and
        //     // check for that.
        //     if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(NpgsqlRange<>))
        //     {
        //         var subtypeType = type.GetGenericArguments()[0];
        //
        //         return MappingsByClrType.TryGetValue(subtypeType, out var subtypeMapping)
        //             ? GetOrBindRangeHandler(subtypeMapping)
        //             : throw new NotSupportedException($"The CLR range type {type} isn't supported by Npgsql or your PostgreSQL.");
        //     }
        //
        //     if (typeof(IEnumerable).IsAssignableFrom(type))
        //         throw new NotSupportedException("Npgsql 3.x removed support for writing a parameter with an IEnumerable value, use .ToList()/.ToArray() instead");
        //
        //     throw new NotSupportedException($"The CLR type {type} isn't natively supported by Npgsql or your PostgreSQL. " +
        //                                     $"To use it with a PostgreSQL composite you need to specify {nameof(NpgsqlParameter.DataTypeName)} or to map it, please refer to the documentation.");
        // }

        static Type? GetArrayListElementType(Type type)
        {
            var typeInfo = type.GetTypeInfo();
            if (typeInfo.IsArray)
                return GetUnderlyingType(type.GetElementType()!); // The use of bang operator is justified here as Type.GetElementType() only returns null for the Array base class which can't be mapped in a useful way.

            var ilist = typeInfo.ImplementedInterfaces.FirstOrDefault(x => x.GetTypeInfo().IsGenericType && x.GetGenericTypeDefinition() == typeof(IList<>));
            if (ilist != null)
                return GetUnderlyingType(ilist.GetGenericArguments()[0]);

            if (typeof(IList).IsAssignableFrom(type))
                throw new NotSupportedException("Non-generic IList is a supported parameter, but the NpgsqlDbType parameter must be set on the parameter");

            return null;

            Type GetUnderlyingType(Type t)
                => Nullable.GetUnderlyingType(t) ?? t;
        }

        #endregion Type handler lookup

        #region Mapping management

        protected override INpgsqlTypeMapper DoMapEnum<TEnum>(string pgName, INpgsqlNameTranslator nameTranslator)
        {
            var userEnumMapping = new UserEnumTypeMapping<TEnum>(pgName, nameTranslator);

            if (DatabaseInfo.GetPostgresTypeByName(userEnumMapping.PgTypeName) is not PostgresEnumType pgEnumType)
                throw new InvalidCastException($"Cannot map enum type {userEnumMapping.ClrType.Name} to PostgreSQL type {userEnumMapping.PgTypeName} which isn't an enum");

            ApplyEnumMapping(pgEnumType, userEnumMapping);
            return this;
        }

        void ApplyEnumMapping(PostgresEnumType pgEnumType, IUserEnumTypeMapping userEnumMapping)
            => _extraHandlersByOID[pgEnumType.OID] =
                _extraHandlersByClrType[userEnumMapping.ClrType] =
                    _extraHandlersByDataTypeName[userEnumMapping.PgTypeName] =
                        userEnumMapping.CreateHandler(pgEnumType);

        protected override bool DoUnmapEnum<TEnum>(string pgName, INpgsqlNameTranslator nameTranslator)
        {
            var userEnumMapping = new UserEnumTypeMapping<TEnum>(pgName, nameTranslator);

            if (DatabaseInfo.GetPostgresTypeByName(userEnumMapping.PgTypeName) is not PostgresEnumType pgEnumType)
                throw new InvalidCastException($"Cannot map enum type {userEnumMapping.ClrType.Name} to PostgreSQL type {userEnumMapping.PgTypeName} which isn't an enum");

            var found = _extraHandlersByOID.Remove(pgEnumType.OID, out _);
            found |= _extraHandlersByClrType.Remove(userEnumMapping.ClrType, out _);
            found |= _extraHandlersByDataTypeName.Remove(userEnumMapping.PgTypeName, out _);
            return found;
        }

        public override INpgsqlTypeMapper AddMapping(NpgsqlTypeMapping mapping)
        {
            CheckReady();

            if (MappingsByName.ContainsKey(mapping.PgTypeName))
                RemoveMapping(mapping.PgTypeName);

            MappingsByName[mapping.PgTypeName] = mapping;
            if (mapping.NpgsqlDbType is not null)
                MappingsByNpgsqlDbType[mapping.NpgsqlDbType.Value] = mapping;
            foreach (var clrType in mapping.ClrTypes)
                MappingsByClrType[clrType] = mapping;

            GetOrBindBaseHandler(mapping);

            ChangeCounter = -1;
            return this;
        }

        public override bool RemoveMapping(string pgTypeName)
        {
            CheckReady();

            if (!MappingsByName.TryGetValue(pgTypeName, out var mapping))
                return false;

            MappingsByName.Remove(pgTypeName);
            if (mapping.NpgsqlDbType is not null)
                MappingsByNpgsqlDbType.Remove(mapping.NpgsqlDbType.Value);
            foreach (var clrType in mapping.ClrTypes)
                MappingsByClrType.Remove(clrType);

            // Clear all bindings. We do this rather than trying to update the existing dictionaries because it's complex to remove arrays,
            // ranges...
            ClearBindings();
            ChangeCounter = -1;
            return true;
        }

        public override IEnumerable<NpgsqlTypeMapping> Mappings => MappingsByName.Values;

        void CheckReady()
        {
            if (_connector.State != ConnectorState.Ready)
                throw new InvalidOperationException("Connection must be open and idle to perform registration");
        }

        [MemberNotNull(nameof(MappingsByName), nameof(MappingsByNpgsqlDbType), nameof(MappingsByClrType))]
        void ResetMappings()
        {
            var globalMapper = GlobalTypeMapper.Instance;
            globalMapper.Lock.EnterReadLock();
            try
            {
                MappingsByName = new Dictionary<string, NpgsqlTypeMapping>(globalMapper.MappingsByName);
                MappingsByNpgsqlDbType = new Dictionary<NpgsqlDbType, NpgsqlTypeMapping>(globalMapper.MappingsByNpgsqlDbType);
                MappingsByClrType = new Dictionary<Type, NpgsqlTypeMapping>(globalMapper.MappingsByClrType);
            }
            finally
            {
                globalMapper.Lock.ExitReadLock();
            }
            ChangeCounter = GlobalTypeMapper.Instance.ChangeCounter;
        }

        void ClearBindings()
        {
            lock (_writeLock)
            {
                _handlersByOID.Clear();
                _handlersByNpgsqlDbType.Clear();
                _handlersByClrType.Clear();
                _arrayHandlerByClrType.Clear();

                _handlersByNpgsqlDbType[NpgsqlDbType.Unknown] = UnrecognizedTypeHandler;
                _handlersByClrType[typeof(DBNull)] = UnrecognizedTypeHandler;
            }
        }

        [MemberNotNull(nameof(_resolvers))]
        [MemberNotNull(nameof(MappingsByName), nameof(MappingsByNpgsqlDbType), nameof(MappingsByClrType))]
        public override void Reset()
        {
            var globalMapper = GlobalTypeMapper.Instance;
            globalMapper.Lock.EnterReadLock();
            try
            {
                _extraHandlersByOID.Clear();
                _extraHandlersByNpgsqlDbType.Clear();
                _extraHandlersByClrType.Clear();
                _extraHandlersByDataTypeName.Clear();

                _resolvers = new ITypeHandlerResolver[globalMapper.ResolverFactories.Count];
                for (var i = 0; i < _resolvers.Length; i++)
                    _resolvers[i] = globalMapper.ResolverFactories[i].Create(_connector);

                // TODO: Skip this entire method, we'll be called again later after injecting the DatabaseInfo
                if (_databaseInfo is not null)
                {
                    foreach (var userEnumMapping in globalMapper.UserEnumTypeMappings.Values)
                    {
                        if (DatabaseInfo.TryGetPostgresTypeByName(userEnumMapping.PgTypeName, out var pgType) &&
                            pgType is PostgresEnumType pgEnumType)
                        {
                            ApplyEnumMapping(pgEnumType, userEnumMapping);
                        }
                    }
                }
            }
            finally
            {
                globalMapper.Lock.ExitReadLock();
            }
            ChangeCounter = GlobalTypeMapper.Instance.ChangeCounter;

            // TODO: Remove
            ClearBindings();
            ResetMappings();
        }

        #endregion Mapping management

        #region Binding

        NpgsqlTypeHandler GetOrBindBaseHandler(NpgsqlTypeMapping mapping, PostgresType? pgType = null)
        {
            lock (_writeLock)
            {
                pgType ??= GetPostgresType(mapping);
                var handler = mapping.TypeHandlerFactory.CreateNonGeneric(pgType, _connector);
                return GetOrBindHandler(handler, pgType, mapping.NpgsqlDbType, mapping.ClrTypes);
            }
        }

        NpgsqlTypeHandler GetOrBindHandler(NpgsqlTypeHandler handler, PostgresType pgType, NpgsqlDbType? npgsqlDbType = null, Type[]? clrTypes = null)
        {
            Debug.Assert(Monitor.IsEntered(_writeLock));

            if (_handlersByOID.TryGetValue(pgType.OID, out var existingHandler))
            {
                if (handler.GetType() != existingHandler.GetType())
                {
                    throw new InvalidOperationException($"Two type handlers registered on same type OID '{pgType.OID}': " +
                                                        $"{existingHandler.GetType().Name} and {handler.GetType().Name}");
                }

                return existingHandler;
            }

            _handlersByOID[pgType.OID] = handler;
            _handlersByTypeName[pgType.FullName] = handler;
            _handlersByTypeName[pgType.Name] = handler;

            if (npgsqlDbType.HasValue)
            {
                var value = npgsqlDbType.Value;
                if (_handlersByNpgsqlDbType.ContainsKey(npgsqlDbType.Value))
                {
                    throw new InvalidOperationException($"Two type handlers registered on same NpgsqlDbType '{npgsqlDbType.Value}': " +
                                                        $"{_handlersByNpgsqlDbType[value].GetType().Name} and {handler.GetType().Name}");
                }

                _handlersByNpgsqlDbType[npgsqlDbType.Value] = handler;
            }

            if (clrTypes != null)
            {
                foreach (var type in clrTypes)
                {
                    if (_handlersByClrType.ContainsKey(type))
                    {
                        throw new InvalidOperationException($"Two type handlers registered on same .NET type '{type}': " +
                                                            $"{_handlersByClrType[type].GetType().Name} and {handler.GetType().Name}");
                    }

                    _handlersByClrType[type] = handler;
                }
            }

            return handler;
        }

        NpgsqlTypeHandler GetOrBindArrayHandler(NpgsqlTypeMapping elementMapping)
        {
            if (GetPostgresType(elementMapping).Array is not { } pgArrayType)
                throw new ArgumentException($"No array type could be found in the database for element {elementMapping.PgTypeName}");

            lock (_writeLock)
            {
                var elementHandler = GetOrBindBaseHandler(elementMapping);
                var arrayNpgsqlDbType = NpgsqlDbType.Array | elementMapping.NpgsqlDbType;

                return GetOrBindArrayHandler(elementHandler, pgArrayType, arrayNpgsqlDbType, elementMapping.ClrTypes);
            }
        }

        NpgsqlTypeHandler GetOrBindArrayHandler(
            NpgsqlTypeHandler elementHandler,
            PostgresArrayType arrayPgType,
            NpgsqlDbType? arrayNpgsqlDbType = null,
            Type[]? elementClrTypes = null)
        {
            Debug.Assert(Monitor.IsEntered(_writeLock));

            NpgsqlTypeHandler arrayHandler = elementHandler.CreateArrayHandler(arrayPgType, _connector.Settings.ArrayNullabilityMode);

            arrayHandler = GetOrBindHandler(arrayHandler, arrayPgType, arrayNpgsqlDbType);

            // Note that array handlers aren't registered in ByClrType like base types, because they handle all
            // dimension types and not just one CLR type (e.g. int[], int[,], int[,,]).
            // So the by-type lookup is special and goes via _arrayHandlerByClrType, see this[Type type]
            // TODO: register single-dimensional in _byType as a specific optimization? But avoid MakeArrayType for reflection-free mode?
            if (elementClrTypes is not null)
            {
                foreach (var elementType in elementClrTypes)
                {
                    if (_arrayHandlerByClrType.TryGetValue(elementType, out var existingArrayHandler))
                    {
                        if (arrayHandler.GetType() != existingArrayHandler.GetType())
                        {
                            throw new Exception(
                                $"Two array type handlers registered on same .NET type {elementType}: " +
                                $"{existingArrayHandler.GetType().Name} and {arrayHandler.GetType().Name}");
                        }
                    }
                    else
                        _arrayHandlerByClrType[elementType] = arrayHandler;
                }
            }

            return arrayHandler;
        }

        NpgsqlTypeHandler GetOrBindRangeHandler(NpgsqlTypeMapping subtypeMapping)
        {
            if (GetPostgresType(subtypeMapping).Range is not { } pgRangeType)
                throw new ArgumentException($"No range type could be found in the database for subtype {subtypeMapping.PgTypeName}");

            lock (_writeLock)
            {
                var subtypeHandler = GetOrBindBaseHandler(subtypeMapping);
                var rangeHandler = subtypeHandler.CreateRangeHandler(pgRangeType);

                var rangeNpgsqlDbType = NpgsqlDbType.Range | subtypeMapping.NpgsqlDbType;

                // We only want to bind supported range CLR types whose element CLR types are being bound as well.
                var clrTypes = rangeHandler.SupportedRangeClrTypes
                    .Where(r => subtypeMapping.ClrTypes.Contains(r.GenericTypeArguments[0]))
                    .ToArray();

                return GetOrBindHandler((NpgsqlTypeHandler)rangeHandler, pgRangeType, rangeNpgsqlDbType, clrTypes: clrTypes);
            }
        }

        NpgsqlTypeHandler GetOrBindMultirangeHandler(NpgsqlTypeMapping subtypeMapping)
        {
            if (GetPostgresType(subtypeMapping).Range?.Multirange is not { } pgMultirangeType)
                throw new ArgumentException($"No range type could be found in the database for subtype {subtypeMapping.PgTypeName}");

            lock (_writeLock)
            {
                var subtypeHandler = GetOrBindBaseHandler(subtypeMapping);
                var multirangeHandler = subtypeHandler.CreateMultirangeHandler(pgMultirangeType);

                var rangeNpgsqlDbType = NpgsqlDbType.Multirange | subtypeMapping.NpgsqlDbType;

                // We only want to bind supported range CLR types whose element CLR types are being bound as well.
                var clrTypes = multirangeHandler.SupportedMultirangeClrTypes
                    .Where(r => subtypeMapping.ClrTypes.Contains(GetArrayListElementType(r)))
                    .ToArray();

                return GetOrBindHandler((NpgsqlTypeHandler)multirangeHandler, pgMultirangeType, rangeNpgsqlDbType, clrTypes: clrTypes);
            }
        }

        NpgsqlTypeHandler GetOrBindUnmappedEnumHandler(PostgresEnumType pgEnumType)
        {
            throw new NotImplementedException();
            // lock (_writeLock)
            // {
            //     var unmappedEnumFactory = new UnmappedEnumTypeHandlerFactory(DefaultNameTranslator);
            //     var handler = unmappedEnumFactory.Create(pgEnumType, _connector);
            //     // TODO: Can map the enum's CLR type to prevent future lookups
            //     return GetOrBindHandler(handler, pgEnumType);
            // }
        }

        NpgsqlTypeHandler GetOrBindUnmappedEnumArrayHandler(PostgresArrayType pgArrayType)
        {
            lock (_writeLock)
            {
                var elementHandler = GetOrBindUnmappedEnumHandler((PostgresEnumType)pgArrayType.Element);
                return GetOrBindArrayHandler(elementHandler, pgArrayType);
            }
        }

        PostgresType GetPostgresType(NpgsqlTypeMapping mapping)
        {
            var pgType = DatabaseInfo.GetPostgresTypeByName(mapping.PgTypeName);

            // TODO: Revisit this
            if (pgType is PostgresDomainType)
                throw new NotSupportedException("Cannot add a mapping to a PostgreSQL domain type");

            return pgType;
        }

        NpgsqlTypeMapping? GetMapping(PostgresType pgType)
            => MappingsByName.TryGetValue(
                pgType is PostgresDomainType pgDomainType ? pgDomainType.BaseType.Name : pgType.Name,
                out var mapping)
                ? mapping
                : null;

        #endregion Binding

        internal (NpgsqlDbType? npgsqlDbType, PostgresType postgresType) GetTypeInfoByOid(uint oid)
        {
            if (!DatabaseInfo.ByOID.TryGetValue(oid, out var postgresType))
                throw new InvalidOperationException($"Couldn't find PostgreSQL type with OID {oid}");

            // Try to find the postgresType in the mappings
            if (TryGetMapping(postgresType, out var npgsqlTypeMapping))
                return (npgsqlTypeMapping.NpgsqlDbType, postgresType);

            // Try to find the elements' postgresType in the mappings
            if (postgresType is PostgresArrayType arrayType &&
                TryGetMapping(arrayType.Element, out var elementNpgsqlTypeMapping))
                return (elementNpgsqlTypeMapping.NpgsqlDbType | NpgsqlDbType.Array, postgresType);

            // Try to find the elements' postgresType of the base type in the mappings
            // this happens with domains over arrays
            if (postgresType is PostgresDomainType domainType && domainType.BaseType is PostgresArrayType baseType &&
                TryGetMapping(baseType.Element, out var baseTypeElementNpgsqlTypeMapping))
                return (baseTypeElementNpgsqlTypeMapping.NpgsqlDbType | NpgsqlDbType.Array, postgresType);

            // It might be an unmapped enum/composite type, or some other unmapped type
            return (null, postgresType);
        }

        bool TryGetMapping(PostgresType pgType, [NotNullWhen(true)] out NpgsqlTypeMapping? mapping)
            => MappingsByName.TryGetValue(pgType.Name, out mapping) ||
               MappingsByName.TryGetValue(pgType.FullName, out mapping) ||
               pgType is PostgresDomainType domain && (
                   MappingsByName.TryGetValue(domain.BaseType.Name, out mapping) ||
                   MappingsByName.TryGetValue(domain.BaseType.FullName, out mapping));
    }
}
