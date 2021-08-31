using BenchmarkDotNet.Attributes;
using Npgsql.Internal.TypeHandling;
using Npgsql.TypeMapping;
using NpgsqlTypes;

namespace Npgsql.Benchmarks
{
    [MemoryDiagnoser]
    public class ResolveHandler
    {
        NpgsqlConnection _conn = null!;
        ConnectorTypeMapper _typeMapper = null!;

        [GlobalSetup]
        public void Setup()
        {
            _conn = BenchmarkEnvironment.OpenConnection();
            _typeMapper = (ConnectorTypeMapper)_conn.TypeMapper;
        }

        [GlobalCleanup]
        public void Cleanup() => _conn.Dispose();

        [Benchmark]
        public NpgsqlTypeHandler ResolveOID()
            => _typeMapper.ResolveOID(23); // int4

        [Benchmark]
        public NpgsqlTypeHandler ResolveNpgsqlDbType()
            => _typeMapper.ResolveNpgsqlDbType(NpgsqlDbType.Integer);

        [Benchmark]
        public NpgsqlTypeHandler ResolveDataTypeName()
            => _typeMapper.ResolveDataTypeName("integer");

        [Benchmark]
        public NpgsqlTypeHandler ResolveClrTypeInt()
            => _typeMapper.ResolveClrType(typeof(int));

        [Benchmark]
        public NpgsqlTypeHandler ResolveClrTypeTid()
            => _typeMapper.ResolveClrType(typeof(NpgsqlTid));

    }
}
