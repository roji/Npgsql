using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using NpgsqlTypes;
using NUnit.Framework;

namespace Npgsql.Tests.Types
{
    public class DateTimeTests : MultiplexingTestBase
    {
        #region Date

        [Test]
        public async Task Date()
        {
            using var conn = await OpenConnectionAsync();
            var dateTime = new DateTime(2002, 3, 4, 0, 0, 0, 0, DateTimeKind.Unspecified);
            var npgsqlDate = new NpgsqlDate(dateTime);

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            var p1 = new NpgsqlParameter("p1", NpgsqlDbType.Date) {Value = npgsqlDate};
            var p2 = new NpgsqlParameter {ParameterName = "p2", Value = npgsqlDate};
            Assert.That(p2.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Date));
            Assert.That(p2.DbType, Is.EqualTo(DbType.Date));
            cmd.Parameters.Add(p1);
            cmd.Parameters.Add(p2);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                // Regular type (DateTime)
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(DateTime)));
                Assert.That(reader.GetDateTime(i), Is.EqualTo(dateTime));
                Assert.That(reader.GetFieldValue<DateTime>(i), Is.EqualTo(dateTime));
                Assert.That(reader[i], Is.EqualTo(dateTime));
                Assert.That(reader.GetValue(i), Is.EqualTo(dateTime));

                // Provider-specific type (NpgsqlDate)
                Assert.That(reader.GetDate(i), Is.EqualTo(npgsqlDate));
                Assert.That(reader.GetProviderSpecificFieldType(i), Is.EqualTo(typeof(NpgsqlDate)));
                Assert.That(reader.GetProviderSpecificValue(i), Is.EqualTo(npgsqlDate));
                Assert.That(reader.GetFieldValue<NpgsqlDate>(i), Is.EqualTo(npgsqlDate));
            }
        }

        static readonly TestCaseData[] DateSpecialCases = {
            new TestCaseData(NpgsqlDate.Infinity).SetName(nameof(DateSpecial) + "Infinity"),
            new TestCaseData(NpgsqlDate.NegativeInfinity).SetName(nameof(DateSpecial) + "NegativeInfinity"),
            new TestCaseData(new NpgsqlDate(-5, 3, 3)).SetName(nameof(DateSpecial) +"BC"),
        };

        [Test, TestCaseSource(nameof(DateSpecialCases))]
        public async Task DateSpecial(NpgsqlDate value)
        {
            using var conn = await OpenConnectionAsync();
            using var cmd = new NpgsqlCommand("SELECT @p", conn);
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Value = value });
            using (var reader = await cmd.ExecuteReaderAsync()) {
                reader.Read();
                Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(value));
                Assert.That(() => reader.GetDateTime(0), Throws.Exception.TypeOf<InvalidCastException>());
            }
            Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }

        [Test, Description("Makes sure that when ConvertInfinityDateTime is true, infinity values are properly converted")]
        public async Task DateConvertInfinity_DateTime()
        {
            using var conn = new NpgsqlConnection(ConnectionString + ";ConvertInfinityDateTime=true");
            conn.Open();

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Date, DateTime.MaxValue);
            cmd.Parameters.AddWithValue("p2", NpgsqlDbType.Date, DateTime.MinValue);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            Assert.That(reader.GetFieldValue<NpgsqlDate>(0), Is.EqualTo(NpgsqlDate.Infinity));
            Assert.That(reader.GetFieldValue<NpgsqlDate>(1), Is.EqualTo(NpgsqlDate.NegativeInfinity));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(DateTime.MaxValue));
            Assert.That(reader.GetDateTime(1), Is.EqualTo(DateTime.MinValue));
        }

#if NET6_0_OR_GREATER
        [Test]
        public async Task Date_DateOnly()
        {
            using var conn = await OpenConnectionAsync();
            var dateOnly = new DateOnly(2002, 3, 4);
            var dateTime = dateOnly.ToDateTime(default);

            using var cmd = new NpgsqlCommand("SELECT @p1", conn);
            var p1 = new NpgsqlParameter { ParameterName = "p1", Value = dateOnly };
            Assert.That(p1.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Date));
            Assert.That(p1.DbType, Is.EqualTo(DbType.Date));
            cmd.Parameters.Add(p1);

            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            Assert.That(reader.GetFieldValue<DateOnly>(0), Is.EqualTo(dateOnly));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(dateTime));
            Assert.That(reader[0], Is.EqualTo(dateTime));
            Assert.That(reader.GetValue(0), Is.EqualTo(dateTime));
        }

        [Test, Description("Makes sure that when ConvertInfinityDateTime is true, infinity values are properly converted")]
        public async Task DateConvertInfinity_DateOnly()
        {
            using var conn = new NpgsqlConnection(ConnectionString + ";ConvertInfinityDateTime=true");
            conn.Open();

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Date, DateOnly.MaxValue);
            cmd.Parameters.AddWithValue("p2", NpgsqlDbType.Date, DateOnly.MinValue);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            Assert.That(reader.GetFieldValue<NpgsqlDate>(0), Is.EqualTo(NpgsqlDate.Infinity));
            Assert.That(reader.GetFieldValue<NpgsqlDate>(1), Is.EqualTo(NpgsqlDate.NegativeInfinity));
            Assert.That(reader.GetFieldValue<DateOnly>(0), Is.EqualTo(DateOnly.MaxValue));
            Assert.That(reader.GetFieldValue<DateOnly>(1), Is.EqualTo(DateOnly.MinValue));
        }
#endif

        #endregion

        #region Time

        [Test]
        public async Task Time()
        {
            using var conn = await OpenConnectionAsync();
            var expected = new TimeSpan(0, 10, 45, 34, 500);

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Time) {Value = expected});
            cmd.Parameters.Add(new NpgsqlParameter("p2", DbType.Time) {Value = expected});
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(TimeSpan)));
                Assert.That(reader.GetTimeSpan(i), Is.EqualTo(expected));
                Assert.That(reader.GetFieldValue<TimeSpan>(i), Is.EqualTo(expected));
                Assert.That(reader[i], Is.EqualTo(expected));
                Assert.That(reader.GetValue(i), Is.EqualTo(expected));
            }
        }

#if NET6_0_OR_GREATER
        [Test]
        public async Task Time_TimeOnly()
        {
            using var conn = await OpenConnectionAsync();
            var timeOnly = new TimeOnly(10, 45, 34, 500);
            var timeSpan = timeOnly.ToTimeSpan();

            using var cmd = new NpgsqlCommand("SELECT @p1", conn);
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p1", Value = timeOnly });

            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            Assert.That(reader.GetFieldValue<TimeOnly>(0), Is.EqualTo(timeOnly));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(TimeSpan)));
            Assert.That(reader.GetTimeSpan(0), Is.EqualTo(timeSpan));
            Assert.That(reader.GetFieldValue<TimeSpan>(0), Is.EqualTo(timeSpan));
            Assert.That(reader[0], Is.EqualTo(timeSpan));
            Assert.That(reader.GetValue(0), Is.EqualTo(timeSpan));
        }
#endif

        #endregion

        #region Time with timezone

        [Test]
        [MonoIgnore]
        public async Task TimeTz()
        {
            using var conn = await OpenConnectionAsync();
            var tzOffset = TimeZoneInfo.Local.BaseUtcOffset;
            if (tzOffset == TimeSpan.Zero)
                Assert.Ignore("Test cannot run when machine timezone is UTC");

            // Note that the date component of the below is ignored
            var dto = new DateTimeOffset(5, 5, 5, 13, 3, 45, 510, tzOffset);
            var dtUtc = new DateTime(dto.Year, dto.Month, dto.Day, dto.Hour, dto.Minute, dto.Second, dto.Millisecond, DateTimeKind.Utc) - tzOffset;
            var dtLocal = new DateTime(dto.Year, dto.Month, dto.Day, dto.Hour, dto.Minute, dto.Second, dto.Millisecond, DateTimeKind.Local);
            var dtUnspecified = new DateTime(dto.Year, dto.Month, dto.Day, dto.Hour, dto.Minute, dto.Second, dto.Millisecond, DateTimeKind.Unspecified);
            var ts = dto.TimeOfDay;

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2, @p3, @p4, @p5", conn);
            cmd.Parameters.AddWithValue("p1", NpgsqlDbType.TimeTz, dto);
            cmd.Parameters.AddWithValue("p2", NpgsqlDbType.TimeTz, dtUtc);
            cmd.Parameters.AddWithValue("p3", NpgsqlDbType.TimeTz, dtLocal);
            cmd.Parameters.AddWithValue("p4", NpgsqlDbType.TimeTz, dtUnspecified);
            cmd.Parameters.AddWithValue("p5", NpgsqlDbType.TimeTz, ts);
            Assert.That(cmd.Parameters.All(p => p.DbType == DbType.Object));

            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(DateTimeOffset)));

                Assert.That(reader.GetFieldValue<DateTimeOffset>(i), Is.EqualTo(new DateTimeOffset(1, 1, 2, dto.Hour, dto.Minute, dto.Second, dto.Millisecond, dto.Offset)));
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(DateTimeOffset)));
                Assert.That(reader.GetFieldValue<DateTime>(i).Kind, Is.EqualTo(DateTimeKind.Local));
                Assert.That(reader.GetFieldValue<DateTime>(i), Is.EqualTo(reader.GetFieldValue<DateTimeOffset>(i).LocalDateTime));
                Assert.That(reader.GetFieldValue<TimeSpan>(i), Is.EqualTo(reader.GetFieldValue<DateTimeOffset>(i).LocalDateTime.TimeOfDay));
            }
        }

        [Test]
        public async Task TimeWithTimeZoneBeforeUtcZero()
        {
            using var conn = await OpenConnectionAsync();
            using var cmd = new NpgsqlCommand("SELECT TIME WITH TIME ZONE '01:00:00+02'", conn);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            Assert.That(reader.GetFieldValue<DateTimeOffset>(0), Is.EqualTo(new DateTimeOffset(1, 1, 2, 1, 0, 0, new TimeSpan(0, 2, 0, 0))));
        }

        #endregion

        #region Timestamp

        static readonly TestCaseData[] TimestampValues =
        {
            new TestCaseData(new DateTime(1998, 4, 12, 13, 26, 38, DateTimeKind.Utc), "1998-04-12 13:26:38")
                .SetName("TimestampPre2000"),
            new TestCaseData(new DateTime(2015, 1, 27, 8, 45, 12, 345, DateTimeKind.Utc), "2015-01-27 08:45:12.345")
                .SetName("TimestampPost2000"),
            new TestCaseData(new DateTime(2013, 7, 25, 0, 0, 0, DateTimeKind.Utc), "2013-07-25 00:00:00")
                .SetName("TimestampDateOnly"),
        };

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamp_read(DateTime dateTime, string s)
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand($"SELECT '{s}'::timestamp without time zone", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("timestamp without time zone"));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));

            Assert.That(reader[0], Is.EqualTo(dateTime));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(dateTime));
            Assert.That(reader.GetDateTime(0).Kind, Is.EqualTo(DateTimeKind.Unspecified));
            Assert.That(reader.GetFieldValue<DateTime>(0), Is.EqualTo(dateTime));

            // Provider-specific type (NpgsqlTimeStamp)
            var npgsqlDateTime = new NpgsqlDateTime(dateTime.Ticks);
            Assert.That(reader.GetProviderSpecificFieldType(0), Is.EqualTo(typeof(NpgsqlDateTime)));
            Assert.That(reader.GetTimeStamp(0), Is.EqualTo(npgsqlDateTime));
            Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(npgsqlDateTime));
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(0), Is.EqualTo(npgsqlDateTime));

            // DateTimeOffset
            Assert.That(() => reader.GetFieldValue<DateTimeOffset>(0), Throws.Exception.TypeOf<InvalidCastException>());
        }

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamp_write_values(DateTime dateTime, string expected)
        {
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Utc));

            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters =
                {
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp }
                }
            };

            Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(expected));
        }

        static NpgsqlParameter[] TimestampParameters
        {
            get
            {
                var dateTime = new DateTime(1998, 4, 12, 13, 26, 38);

                return new NpgsqlParameter[]
                {
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified) },
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Local) },
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Local), NpgsqlDbType = NpgsqlDbType.Timestamp },
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Local), DbType = DbType.DateTime },
                    new() { Value = DateTime.SpecifyKind(dateTime, DateTimeKind.Local), DbType = DbType.DateTime2 },
                    new() { Value = new NpgsqlDateTime(dateTime.Ticks, DateTimeKind.Unspecified) },
                    new() { Value = new NpgsqlDateTime(dateTime.Ticks, DateTimeKind.Local) },
                };
            }
        }

        [Test, TestCaseSource(nameof(TimestampParameters))]
        public async Task Timestamp_resolution(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            conn.TypeMapper.Reset();

            await using var cmd = new NpgsqlCommand("SELECT pg_typeof($1)::text, $1::text", conn)
            {
                Parameters = { parameter }
            };

            Assert.That(parameter.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Timestamp));
            Assert.That(parameter.DbType, Is.EqualTo(DbType.DateTime).Or.EqualTo(DbType.DateTime2));

            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            Assert.That(reader[0], Is.EqualTo("timestamp without time zone"));
            Assert.That(reader[1], Is.EqualTo("1998-04-12 13:26:38"));
        }

        static NpgsqlParameter[] TimestampInvalidParameters
            => new NpgsqlParameter[]
            {
                new() { Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.Timestamp },
                new() { Value = new NpgsqlDateTime(0, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.Timestamp },
                new() { Value = new DateTimeOffset(DateTime.UtcNow, TimeSpan.Zero), NpgsqlDbType = NpgsqlDbType.Timestamp }
            };

        [Test, TestCaseSource(nameof(TimestampInvalidParameters))]
        public async Task Timestamp_resolution_failure(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters = { parameter }
            };

            Assert.That(() => cmd.ExecuteReaderAsync(), Throws.Exception.TypeOf<InvalidCastException>());
        }

        static readonly TestCaseData[] TimestampSpecialCases = {
            new TestCaseData(NpgsqlDateTime.Infinity).SetName(nameof(TimeStampSpecial) + "Infinity"),
            new TestCaseData(NpgsqlDateTime.NegativeInfinity).SetName(nameof(TimeStampSpecial) + "NegativeInfinity"),
            new TestCaseData(new NpgsqlDateTime(-5, 3, 3, 1, 0, 0)).SetName(nameof(TimeStampSpecial) + "BC"),
        };

        [Test, TestCaseSource(nameof(TimestampSpecialCases))]
        public async Task TimeStampSpecial(NpgsqlDateTime value)
        {
            using var conn = await OpenConnectionAsync();
            using var cmd = new NpgsqlCommand("SELECT @p", conn);
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Value = value });
            using (var reader = await cmd.ExecuteReaderAsync()) {
                reader.Read();
                Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(value));
                Assert.That(() => reader.GetDateTime(0), Throws.Exception.TypeOf<InvalidCastException>());
            }
            Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }

        [Test, Description("Makes sure that when ConvertInfinityDateTime is true, infinity values are properly converted")]
        public async Task TimeStampConvertInfinity()
        {
            using var conn = new NpgsqlConnection(ConnectionString + ";ConvertInfinityDateTime=true");
            conn.Open();

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Timestamp, DateTime.MaxValue);
            cmd.Parameters.AddWithValue("p2", NpgsqlDbType.Timestamp, DateTime.MinValue);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(0), Is.EqualTo(NpgsqlDateTime.Infinity));
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(1), Is.EqualTo(NpgsqlDateTime.NegativeInfinity));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(DateTime.MaxValue));
            Assert.That(reader.GetDateTime(1), Is.EqualTo(DateTime.MinValue));
        }

        #endregion

        #region Timestamp with timezone

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamptz_read(DateTime dateTime, string s)
        {
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Utc));

            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand($"SELECT '{s}+00'::timestamp with time zone", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("timestamp with time zone"));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(DateTime)));

            Assert.That(reader[0], Is.EqualTo(dateTime));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(dateTime));
            Assert.That(reader.GetFieldValue<DateTime>(0), Is.EqualTo(dateTime));
            Assert.That(reader.GetDateTime(0).Kind, Is.EqualTo(DateTimeKind.Utc));

            // DateTimeOffset
            Assert.That(reader.GetFieldValue<DateTimeOffset>(0), Is.EqualTo(new DateTimeOffset(dateTime)));
            Assert.That(reader.GetFieldValue<DateTimeOffset>(0).Offset, Is.EqualTo(TimeSpan.Zero));

            // Provider-specific type (NpgsqlTimeStamp)
            var npgsqlDateTime = new NpgsqlDateTime(dateTime.Ticks, DateTimeKind.Utc);
            Assert.That(reader.GetProviderSpecificFieldType(0), Is.EqualTo(typeof(NpgsqlDateTime)));
            Assert.That(reader.GetTimeStamp(0), Is.EqualTo(npgsqlDateTime));
            Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(npgsqlDateTime));
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(0), Is.EqualTo(npgsqlDateTime));
            Assert.That(reader.GetTimeStamp(0).Kind, Is.EqualTo(DateTimeKind.Utc));
        }

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamptz_write_values(DateTime dateTime, string expected)
        {
            Assert.That(dateTime.Kind, Is.EqualTo(DateTimeKind.Utc));

            await using var conn = await OpenConnectionAsync();
            await conn.ExecuteNonQueryAsync("SET TimeZone='UTC'");
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters = { new() { Value = dateTime, NpgsqlDbType = NpgsqlDbType.TimestampTz} }
            };

            Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(expected + "+00"));
        }

        static NpgsqlParameter[] TimestamptzParameters
        {
            get
            {
                var dateTime = new DateTime(1998, 4, 12, 13, 26, 38, DateTimeKind.Utc);

                return new NpgsqlParameter[]
                {
                    new() { Value = dateTime },
                    new() { Value = dateTime, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                    new() { Value = new NpgsqlDateTime(dateTime.Ticks, DateTimeKind.Utc), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                    new() { Value = new DateTimeOffset(dateTime) }
                };
            }
        }

        [Test, TestCaseSource(nameof(TimestamptzParameters))]
        public async Task Timestamptz_resolution(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            await conn.ExecuteNonQueryAsync("SET TimeZone='UTC'");
            conn.TypeMapper.Reset();

            await using var cmd = new NpgsqlCommand("SELECT pg_typeof($1)::text, $1::text", conn)
            {
                Parameters = { parameter }
            };

            Assert.That(parameter.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.TimestampTz));
            Assert.That(parameter.DbType, Is.EqualTo(DbType.DateTimeOffset));

            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            Assert.That(reader[0], Is.EqualTo("timestamp with time zone"));
            Assert.That(reader[1], Is.EqualTo("1998-04-12 13:26:38+00"));
        }

        static NpgsqlParameter[] TimestamptzInvalidParameters
            => new NpgsqlParameter[]
            {
                new() { Value = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = DateTime.Now, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = new NpgsqlDateTime(0, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = new NpgsqlDateTime(0, DateTimeKind.Local), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = new DateTimeOffset(DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Unspecified), TimeSpan.FromHours(2)) }
            };

        [Test, TestCaseSource(nameof(TimestamptzInvalidParameters))]
        public async Task Timestamptz_resolution_failure(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters = { parameter }
            };

            Assert.That(() => cmd.ExecuteReaderAsync(), Throws.Exception.TypeOf<InvalidCastException>());
        }

        static readonly TestCaseData[] TimeStampTzSpecialCases = {
            new TestCaseData(NpgsqlDateTime.Infinity).SetName(nameof(TimeStampTzSpecialCases) + "Infinity"),
            new TestCaseData(NpgsqlDateTime.NegativeInfinity).SetName(nameof(TimeStampTzSpecialCases) + "NegativeInfinity"),
            new TestCaseData(new NpgsqlDateTime(-5, 3, 3, 1, 0, 0, DateTimeKind.Local)).SetName(nameof(TimeStampTzSpecialCases) + "BC"),
        };

        [Test, TestCaseSource(nameof(TimeStampTzSpecialCases))]
        public async Task TimeStampTzSpecial(NpgsqlDateTime value)
        {
            using var conn = await OpenConnectionAsync();
            using var cmd = new NpgsqlCommand("SELECT @p", conn);
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p", Value = value, NpgsqlDbType = NpgsqlDbType.TimestampTz });
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                reader.Read();
                Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(value));
                Assert.That(() => reader.GetDateTime(0), Throws.Exception.TypeOf<InvalidCastException>());
            }
            Assert.That(await conn.ExecuteScalarAsync("SELECT 1"), Is.EqualTo(1));
        }

        [Test, Description("Makes sure that when ConvertInfinityDateTime is true, infinity values are properly converted")]
        public async Task TimeStampTzConvertInfinity()
        {
            using var conn = new NpgsqlConnection(ConnectionString + ";ConvertInfinityDateTime=true");
            conn.Open();

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.AddWithValue("p1", NpgsqlDbType.TimestampTz, DateTime.MaxValue);
            cmd.Parameters.AddWithValue("p2", NpgsqlDbType.TimestampTz, DateTime.MinValue);
            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(0), Is.EqualTo(NpgsqlDateTime.Infinity));
            Assert.That(reader.GetFieldValue<NpgsqlDateTime>(1), Is.EqualTo(NpgsqlDateTime.NegativeInfinity));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(DateTime.MaxValue));
            Assert.That(reader.GetDateTime(1), Is.EqualTo(DateTime.MinValue));
        }

        #endregion

        #region Interval

        [Test]
        public async Task Interval()
        {
            using var conn = await OpenConnectionAsync();
            var expectedNpgsqlInterval = new NpgsqlTimeSpan(1, 2, 3, 4, 5);
            var expectedTimeSpan = new TimeSpan(1, 2, 3, 4, 5);

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2, @p3", conn);
            var p1 = new NpgsqlParameter("p1", NpgsqlDbType.Interval);
            var p2 = new NpgsqlParameter("p2", expectedTimeSpan);
            var p3 = new NpgsqlParameter("p3", expectedNpgsqlInterval);
            Assert.That(p2.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Interval));
            Assert.That(p2.DbType, Is.EqualTo(DbType.Object));
            Assert.That(p3.NpgsqlDbType, Is.EqualTo(NpgsqlDbType.Interval));
            Assert.That(p3.DbType, Is.EqualTo(DbType.Object));
            cmd.Parameters.Add(p1);
            cmd.Parameters.Add(p2);
            cmd.Parameters.Add(p3);
            p1.Value = expectedNpgsqlInterval;

            using var reader = await cmd.ExecuteReaderAsync();
            reader.Read();

            // Regular type (TimeSpan)
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(TimeSpan)));
            Assert.That(reader.GetTimeSpan(0), Is.EqualTo(expectedTimeSpan));
            Assert.That(reader.GetFieldValue<TimeSpan>(0), Is.EqualTo(expectedTimeSpan));
            Assert.That(reader[0], Is.EqualTo(expectedTimeSpan));
            Assert.That(reader.GetValue(0), Is.EqualTo(expectedTimeSpan));

            // Provider-specific type (NpgsqlInterval)
            Assert.That(reader.GetInterval(0), Is.EqualTo(expectedNpgsqlInterval));
            Assert.That(reader.GetProviderSpecificFieldType(0), Is.EqualTo(typeof(NpgsqlTimeSpan)));
            Assert.That(reader.GetProviderSpecificValue(0), Is.EqualTo(expectedNpgsqlInterval));
            Assert.That(reader.GetFieldValue<NpgsqlTimeSpan>(0), Is.EqualTo(expectedNpgsqlInterval));
        }

        #endregion

        public DateTimeTests(MultiplexingMode multiplexingMode) : base(multiplexingMode) {}
    }
}
