using Npgsql.Internal;
using Npgsql.TypeMapping;

namespace Npgsql.NetTopologySuite.Internal
{
    public class NetTopologySuiteTypeHandlerResolverFactory : ITypeHandlerResolverFactory
    {
        public ITypeHandlerResolver Create(NpgsqlConnector connector)
            => new NetTopologySuiteTypeHandlerResolver(connector);
    }
}
