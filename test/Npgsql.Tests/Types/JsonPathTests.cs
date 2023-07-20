using System.Data;
using System.Threading.Tasks;
using NpgsqlTypes;
using NUnit.Framework;
using static Npgsql.Tests.TestUtil;

namespace Npgsql.Tests.Types;

public class JsonPathTests : MultiplexingTestBase
{
    public JsonPathTests(MultiplexingMode multiplexingMode)
        : base(multiplexingMode) { }

    [Test]
    [TestCase("$")]
    [TestCase("$\"varname\"")]
    public Task JsonPath(string jsonPath)
        => AssertType(
            jsonPath, jsonPath, "jsonpath", NpgsqlDbType.JsonPath, isDefaultForWriting: false, isNpgsqlDbTypeInferredFromClrType: false,
            inferredDbType: DbType.String);

    [Test]
    public async Task Read_with_GetTextReader()
    {
        await using var command = DataSource.CreateCommand("SELECT '$.foo[3].bar'::jsonpath");
        await using var reader = await command.ExecuteReaderAsync();

        await reader.ReadAsync();
        Assert.That(reader.GetTextReader(0).ReadToEnd(), Is.EqualTo("$.foo[3].bar"));
    }

    [OneTimeSetUp]
    public async Task SetUp()
    {
        await using var conn = await OpenConnectionAsync();
        MinimumPgVersion(conn, "12.0", "The jsonpath type was introduced in PostgreSQL 12");
    }
}
