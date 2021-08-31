using Npgsql.Internal;
using Npgsql.TypeMapping;

namespace Npgsql.NodaTime.Internal
{
    public class NodaTimeTypeHandlerResolverFactory : ITypeHandlerResolverFactory
    {
        public ITypeHandlerResolver Create(NpgsqlConnector connector)
            => new NodaTimeTypeHandlerResolver(connector);
    }
}
