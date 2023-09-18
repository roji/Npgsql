using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql.Internal;
using Npgsql.Internal.Postgres;

namespace Npgsql.Json.NET.Internal;

class JsonNetTypeInfoResolver : IPgTypeInfoResolver
{
    protected TypeInfoMappingCollection Mappings { get; } = new();

    public JsonNetTypeInfoResolver(JsonSerializerSettings? settings = null)
        => AddTypeInfos(Mappings, settings);

    static void AddTypeInfos(TypeInfoMappingCollection mappings, JsonSerializerSettings? settings = null)
    {
        // Capture default settings during construction.
        settings ??= JsonConvert.DefaultSettings?.Invoke() ?? new JsonSerializerSettings();

        // Jsonb is the first default for JToken etc.
        foreach (var dataTypeName in new[] { "jsonb", "json" })
        {
            var jsonb = dataTypeName == "jsonb";
            mappings.AddType<JObject>(dataTypeName, (options, mapping, _) =>
                mapping.CreateInfo(options, new JsonNetJsonConverter<JObject>(jsonb, options.TextEncoding, settings)),
                isDefault: true);
            mappings.AddType<JToken>(dataTypeName, (options, mapping, _) =>
                mapping.CreateInfo(options, new JsonNetJsonConverter<JToken>(jsonb, options.TextEncoding, settings)));
            mappings.AddType<JArray>(dataTypeName, (options, mapping, _) =>
                mapping.CreateInfo(options, new JsonNetJsonConverter<JArray>(jsonb, options.TextEncoding, settings)));
            mappings.AddType<JValue>(dataTypeName, (options, mapping, _) =>
                mapping.CreateInfo(options, new JsonNetJsonConverter<JValue>(jsonb, options.TextEncoding, settings)));
        }
    }

    protected static void AddArrayInfos(TypeInfoMappingCollection mappings)
    {
        foreach (var dataTypeName in new[] { "jsonb", "json" })
        {
            mappings.AddArrayType<JObject>(dataTypeName);
            mappings.AddArrayType<JToken>(dataTypeName);
            mappings.AddArrayType<JArray>(dataTypeName);
            mappings.AddArrayType<JValue>(dataTypeName);
        }
    }

    public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
        => Mappings.Find(type, dataTypeName, options);
}

sealed class JsonNetArrayTypeInfoResolver : JsonNetTypeInfoResolver, IPgTypeInfoResolver
{
    new TypeInfoMappingCollection Mappings { get; }

    public JsonNetArrayTypeInfoResolver(JsonSerializerSettings? settings = null) : base(settings)
    {
        Mappings = new TypeInfoMappingCollection(base.Mappings);
        AddArrayInfos(Mappings);
    }

    public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
        => Mappings.Find(type, dataTypeName, options);
}

[RequiresUnreferencedCode("Json serializer may perform reflection on trimmed types.")]
[RequiresDynamicCode("Serializing arbitary types to json can require creating new generic types or methods, which requires creating code at runtime. This may not work when AOT compiling.")]
class JsonNetPocoTypeInfoResolver : IPgTypeInfoResolver
{
    protected TypeInfoMappingCollection Mappings { get; } = new();
    protected readonly JsonSerializerSettings _serializerSettings;

    public JsonNetPocoTypeInfoResolver(Type[]? jsonbClrTypes = null, Type[]? jsonClrTypes = null, JsonSerializerSettings? serializerSettings = null)
    {
        // Capture default settings during construction.
        _serializerSettings = serializerSettings ??= JsonConvert.DefaultSettings?.Invoke() ?? new JsonSerializerSettings();

        AddMappings(Mappings, jsonbClrTypes ?? Array.Empty<Type>(), jsonClrTypes ?? Array.Empty<Type>(), serializerSettings);
    }

    static void AddMappings(TypeInfoMappingCollection mappings, Type[] jsonbClrTypes, Type[] jsonClrTypes, JsonSerializerSettings settings)
    {
        AddUserMappings(jsonb: true, jsonbClrTypes);
        AddUserMappings(jsonb: false, jsonClrTypes);

        void AddUserMappings(bool jsonb, Type[] clrTypes)
        {
            foreach (var jsonType in clrTypes)
            {
                if (Nullable.GetUnderlyingType(jsonType) is not null)
                    throw new NotSupportedException("Manually mapping nullable types is not supported");

                AddType(mappings, jsonType,
                    jsonb ? "jsonb" : "json",
                    factory: (options, mapping, _) => mapping.CreateInfo(options,
                        CreateJsonNetConverter(mapping.Type, jsonb, options.TextEncoding, settings, jsonType)));
            }
        }
    }

    protected static void AddArrayInfos(TypeInfoMappingCollection mappings, Type[] jsonbClrTypes, Type[] jsonClrTypes)
    {
        foreach (var jsonbClrType in jsonbClrTypes)
            AddArrayType(mappings, jsonbClrType, "jsonb");

        foreach (var jsonClrType in jsonClrTypes)
            AddArrayType(mappings, jsonClrType, "json");
    }

    public PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
    {
        var info = Mappings.Find(type, dataTypeName, options);
        if (info is not null)
            return info;

        // Match all types except null, object and text types as long as DataTypeName (json/jsonb) is present.
        if (type is null || type == typeof(object) || Array.IndexOf(PgSerializerOptions.WellKnownTextTypes, type) != -1)
            return null;

        if (dataTypeName is null || (dataTypeName.GetValueOrDefault() != "pg_catalog.jsonb" && dataTypeName.GetValueOrDefault() != "pg_catalog.json"))
            return null;

        // Synthesize mapping
        var mappings = new TypeInfoMappingCollection();
        CreatePocoMapping(mappings, type, dataTypeName.GetValueOrDefault(), _serializerSettings);
        return mappings.Find(type, dataTypeName, options);
    }

    protected void CreatePocoMapping(TypeInfoMappingCollection mappings, Type type, string dataTypeName, JsonSerializerSettings serializerSettings)
        => (type.IsValueType ? AddStructTypeMethodInfo : AddTypeMethodInfo).MakeGenericMethod(type).Invoke(mappings, new object?[] {
            (string)dataTypeName,
            new TypeInfoFactory((options, mapping, _) =>
            {
                var jsonb = dataTypeName == "pg_catalog.jsonb";

                // For jsonb we can't properly support polymorphic serialization unless we do quite some additional work
                // so we default to mapping.Type instead (exact types will never serialize their "$type" fields, essentially disabling the feature).
                var baseType = jsonb ? mapping.Type : typeof(object);

                return mapping.CreateInfo(options, CreateJsonNetConverter(mapping.Type, jsonb, options.TextEncoding, serializerSettings, baseType));
            }),
            null});

    static void AddType(TypeInfoMappingCollection mappings, Type type, string dataTypeName, TypeInfoFactory factory)
        => (type.IsValueType ? AddStructTypeMethodInfo : AddTypeMethodInfo).MakeGenericMethod(type)
            .Invoke(mappings, new object?[]
        {
            dataTypeName,
            factory,
            null
        });

    static void AddArrayType(TypeInfoMappingCollection mappings, Type type, string dataTypeName)
        => (type.IsValueType ? AddStructArrayTypeMethodInfo : AddArrayTypeMethodInfo).MakeGenericMethod(type)
            .Invoke(mappings, new object?[]
            {
                dataTypeName,
            });

    static readonly MethodInfo AddTypeMethodInfo = typeof(TypeInfoMappingCollection).GetMethod(nameof(TypeInfoMappingCollection.AddType),
        new[] { typeof(string), typeof(TypeInfoFactory), typeof(Func<TypeInfoMapping, TypeInfoMapping>) }) ?? throw new NullReferenceException();

    protected static readonly MethodInfo AddArrayTypeMethodInfo = typeof(TypeInfoMappingCollection)
        .GetMethod(nameof(TypeInfoMappingCollection.AddArrayType), new[] { typeof(string) }) ?? throw new NullReferenceException();

    static readonly MethodInfo AddStructTypeMethodInfo = typeof(TypeInfoMappingCollection).GetMethod(nameof(TypeInfoMappingCollection.AddStructType),
        new[] { typeof(string), typeof(TypeInfoFactory), typeof(Func<TypeInfoMapping, TypeInfoMapping>) }) ?? throw new NullReferenceException();

    protected static readonly MethodInfo AddStructArrayTypeMethodInfo = typeof(TypeInfoMappingCollection)
        .GetMethod(nameof(TypeInfoMappingCollection.AddStructArrayType), new[] { typeof(string) }) ?? throw new NullReferenceException();

    static PgConverter CreateJsonNetConverter(Type valueType, bool jsonb, Encoding textEncoding, JsonSerializerSettings settings, Type baseType)
        => (PgConverter)Activator.CreateInstance(
                typeof(JsonNetJsonConverter<>).MakeGenericType(valueType),
                jsonb,
                textEncoding,
                settings
            )!;
}

sealed class JsonNetPocoArrayTypeInfoResolver : JsonNetPocoTypeInfoResolver, IPgTypeInfoResolver
{
    new TypeInfoMappingCollection Mappings { get; }

    public JsonNetPocoArrayTypeInfoResolver(Type[]? jsonbClrTypes = null, Type[]? jsonClrTypes = null, JsonSerializerSettings? serializerSettings = null)
        : base(jsonbClrTypes, jsonClrTypes, serializerSettings)
    {
        Mappings = new TypeInfoMappingCollection(base.Mappings);
        AddArrayInfos(Mappings, jsonbClrTypes ?? Array.Empty<Type>(), jsonClrTypes ?? Array.Empty<Type>());
    }

    public new PgTypeInfo? GetTypeInfo(Type? type, DataTypeName? dataTypeName, PgSerializerOptions options)
    {
        var info = Mappings.Find(type, dataTypeName, options);
        if (info is not null)
            return info;

        // Match all types except null, object and text types as long as DataTypeName (json/jsonb) is present.
        if (type is null || type == typeof(object) || !TypeInfoMappingCollection.IsArrayType(type, out var elementType)
            || Array.IndexOf(PgSerializerOptions.WellKnownTextTypes, elementType) != -1)
            return null;

        if (dataTypeName != "pg_catalog._jsonb" && dataTypeName != "pg_catalog._json")
            return null;

        // Synthesize mapping

        var mappings = new TypeInfoMappingCollection();
        var elementDataTypeName = dataTypeName == "pg_catalog._jsonb" ? "pg_catalog.jsonb" : "pg_catalog.json";
        CreatePocoMapping(mappings, elementType, elementDataTypeName, _serializerSettings);
        (elementType.IsValueType ? AddStructArrayTypeMethodInfo : AddArrayTypeMethodInfo)
            .MakeGenericMethod(elementType).Invoke(mappings, new []{ elementDataTypeName });

        return mappings.Find(type, dataTypeName, options);
    }
}
