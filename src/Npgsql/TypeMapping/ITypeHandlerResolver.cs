using System;
using System.Diagnostics.CodeAnalysis;
using Npgsql.Internal.TypeHandling;
using NpgsqlTypes;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.TypeMapping
{
    public interface ITypeHandlerResolver
    {
        bool ResolveOID(uint oid, [NotNullWhen(true)] out NpgsqlTypeHandler? handler);

        NpgsqlTypeHandler? ResolveNpgsqlDbType(NpgsqlDbType npgsqlDbType);

        NpgsqlTypeHandler? ResolveDataTypeName(string typeName);

        // TODO: Add generic GetByClrType with a default implementation that delegates to the non-generic version.
        // This way the built-in resolver can specialize for some types.
        NpgsqlTypeHandler? ResolveClrType(Type type);
    }
}
