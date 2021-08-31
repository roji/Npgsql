using Npgsql.Internal;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.TypeMapping
{
    public interface ITypeHandlerResolverFactory
    {
        ITypeHandlerResolver Create(NpgsqlConnector connector);
    }
}
