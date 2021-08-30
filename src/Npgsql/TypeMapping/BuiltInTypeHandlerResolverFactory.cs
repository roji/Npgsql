using Npgsql.Internal;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.TypeMapping
{
    public class BuiltInTypeHandlerResolverFactory : ITypeHandlerResolverFactory
    {
        public ITypeHandlerResolver Create(NpgsqlConnector npgsqlConnector)
            => new BuiltInTypeHandlerResolver(npgsqlConnector);
    }
}
