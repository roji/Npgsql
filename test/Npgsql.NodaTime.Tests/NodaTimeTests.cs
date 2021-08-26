using System;
using System.Data;
using System.Threading.Tasks;
using NodaTime;
using Npgsql.Tests;
using NpgsqlTypes;
using NUnit.Framework;

// ReSharper disable AccessToModifiedClosure
// ReSharper disable AccessToDisposedClosure

namespace Npgsql.NodaTime.Tests
{
    public class NodaTimeTests : TestBase
    {
        #region Timestamp

        static readonly TestCaseData[] TimestampValues =
        {
            new TestCaseData(new LocalDateTime(1998, 4, 12, 13, 26, 38, 789), "1998-04-12 13:26:38.789")
                .SetName("TimestampPre2000"),
            new TestCaseData(new LocalDateTime(2015, 1, 27, 8, 45, 12, 345), "2015-01-27 08:45:12.345")
                .SetName("TimestampPost2000"),
            new TestCaseData(new LocalDateTime(1999, 12, 31, 23, 59, 59, 999).PlusNanoseconds(456000), "1999-12-31 23:59:59.999456")
                .SetName("TimestampMicroseconds"),
        };

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamp_read(LocalDateTime localDateTime, string s)
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand($"SELECT '{s}'::timestamp without time zone", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("timestamp without time zone"));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(LocalDateTime)));

            Assert.That(reader[0], Is.EqualTo(localDateTime));
            Assert.That(reader.GetFieldValue<LocalDateTime>(0), Is.EqualTo(localDateTime));
            Assert.That(reader.GetDateTime(0), Is.EqualTo(localDateTime.ToDateTimeUnspecified()));
            Assert.That(reader.GetFieldValue<DateTime>(0), Is.EqualTo(localDateTime.ToDateTimeUnspecified()));

            Assert.That(() => reader.GetFieldValue<Instant>(0), Throws.TypeOf<InvalidCastException>());
            Assert.That(() => reader.GetFieldValue<ZonedDateTime>(0), Throws.TypeOf<InvalidCastException>());
            Assert.That(() => reader.GetDate(0), Throws.TypeOf<InvalidCastException>());
        }

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamp_write_values(LocalDateTime localDateTime, string expected)
        {
            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters =
                {
                    new() { Value = localDateTime, NpgsqlDbType = NpgsqlDbType.Timestamp }
                }
            };

            Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(expected));
        }

        static NpgsqlParameter[] TimestampParameters
        {
            get
            {
                var localDateTime = new LocalDateTime(1998, 4, 12, 13, 26, 38);

                return new NpgsqlParameter[]
                {
                    new() { Value = localDateTime },
                    new() { Value = localDateTime, NpgsqlDbType = NpgsqlDbType.Timestamp },
                    new() { Value = localDateTime, DbType = DbType.DateTime },
                    new() { Value = localDateTime, DbType = DbType.DateTime2 },
                    new() { Value = localDateTime.ToDateTimeUnspecified() },
                    new() { Value = DateTime.SpecifyKind(localDateTime.ToDateTimeUnspecified(), DateTimeKind.Local) }
                };
            }
        }

        [Test, TestCaseSource(nameof(TimestampParameters))]
        public async Task Timestamp_resolution(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            conn.TypeMapper.Reset();
            conn.TypeMapper.UseNodaTime();

            await using var cmd = new NpgsqlCommand("SELECT pg_typeof($1)::text, $1::text", conn)
            {
                Parameters = { parameter }
            };

            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            Assert.That(reader[0], Is.EqualTo("timestamp without time zone"));
            Assert.That(reader[1], Is.EqualTo("1998-04-12 13:26:38"));
        }

        static NpgsqlParameter[] TimestampInvalidParameters
            => new NpgsqlParameter[]
            {
                new() { Value = new LocalDateTime().InUtc().ToInstant(), NpgsqlDbType = NpgsqlDbType.Timestamp },
                new() { Value = new DateTimeOffset(), NpgsqlDbType = NpgsqlDbType.Timestamp },
                new() { Value = DateTime.UtcNow, NpgsqlDbType = NpgsqlDbType.Timestamp }
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

        #endregion Timestamp

        #region Timestamp with time zone

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamptz_read(LocalDateTime expectedLocalDateTime, string s)
        {
            var expectedInstance = expectedLocalDateTime.InUtc().ToInstant();

            await using var conn = await OpenConnectionAsync();
            await using var cmd = new NpgsqlCommand($"SELECT '{s}+00'::timestamp with time zone", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader.GetDataTypeName(0), Is.EqualTo("timestamp with time zone"));
            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(Instant)));

            Assert.That(reader[0], Is.EqualTo(expectedInstance));
            Assert.That(reader.GetFieldValue<Instant>(0), Is.EqualTo(expectedInstance));
            Assert.That(reader.GetFieldValue<ZonedDateTime>(0), Is.EqualTo(expectedInstance.InUtc()));
            Assert.That(reader.GetFieldValue<OffsetDateTime>(0), Is.EqualTo(expectedInstance.WithOffset(Offset.Zero)));
            Assert.That(reader.GetFieldValue<DateTime>(0), Is.EqualTo(expectedInstance.ToDateTimeUtc()));
            Assert.That(reader.GetFieldValue<DateTimeOffset>(0), Is.EqualTo(expectedInstance.ToDateTimeOffset()));

            Assert.That(() => reader.GetFieldValue<LocalDateTime>(0), Throws.TypeOf<InvalidCastException>());
            Assert.That(() => reader.GetDate(0), Throws.TypeOf<InvalidCastException>());
        }

        [Test, TestCaseSource(nameof(TimestampValues))]
        public async Task Timestamptz_write_values(LocalDateTime localDateTime, string expected)
        {
            await using var conn = await OpenConnectionAsync();
            await conn.ExecuteNonQueryAsync("SET TimeZone='UTC'");
            await using var cmd = new NpgsqlCommand("SELECT $1::text", conn)
            {
                Parameters = { new() { Value = localDateTime.InUtc().ToInstant(), NpgsqlDbType = NpgsqlDbType.TimestampTz} }
            };

            Assert.That(await cmd.ExecuteScalarAsync(), Is.EqualTo(expected + "+00"));
        }

        static NpgsqlParameter[] TimestamptzParameters
        {
            get
            {
                var localDateTime = new LocalDateTime(1998, 4, 12, 13, 26, 38);
                var instance = localDateTime.InUtc().ToInstant();

                return new NpgsqlParameter[]
                {
                    new() { Value = instance },
                    new() { Value = instance, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                    new() { Value = instance, DbType = DbType.DateTimeOffset },
                    new() { Value = instance.InUtc() },
                    new() { Value = instance.WithOffset(Offset.Zero) },
                    new() { Value = instance.InUtc().ToDateTimeUtc() },
                    new() { Value = instance.ToDateTimeOffset() }
                };
            }
        }

        [Test, TestCaseSource(nameof(TimestamptzParameters))]
        public async Task Timestamptz_resolution(NpgsqlParameter parameter)
        {
            await using var conn = await OpenConnectionAsync();
            await conn.ExecuteNonQueryAsync("SET TimeZone='UTC'");
            conn.TypeMapper.Reset();
            conn.TypeMapper.UseNodaTime();

            await using var cmd = new NpgsqlCommand("SELECT pg_typeof($1)::text, $1::text", conn)
            {
                Parameters = { parameter }
            };

            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();
            Assert.That(reader[0], Is.EqualTo("timestamp with time zone"));
            Assert.That(reader[1], Is.EqualTo("1998-04-12 13:26:38+00"));
        }

        static NpgsqlParameter[] TimestamptzInvalidParameters
            => new NpgsqlParameter[]
            {
                new() { Value = new LocalDateTime(), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = DateTime.Now, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = new DateTimeOffset(DateTime.SpecifyKind(DateTime.Now, DateTimeKind.Unspecified), TimeSpan.FromHours(2)), NpgsqlDbType = NpgsqlDbType.TimestampTz },

                // We only support ZonedDateTime and OffsetDateTime in UTC
                new() { Value = new LocalDateTime().InUtc().ToInstant().InZone(DateTimeZoneProviders.Tzdb["America/New_York"]), NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new() { Value = new LocalDateTime().WithOffset(Offset.FromHours(1)), NpgsqlDbType = NpgsqlDbType.TimestampTz }
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

        [Test]
        public async Task Timestamptz_read_infinity()
        {
            var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString) { ConvertInfinityDateTime = true }.ConnectionString;
            await using var conn = await OpenConnectionAsync(connectionString);
            await using var cmd =
                new NpgsqlCommand("SELECT 'infinity'::timestamp with time zone, '-infinity'::timestamp with time zone", conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader.GetFieldValue<Instant>(0), Is.EqualTo(Instant.MaxValue));
            Assert.That(reader.GetFieldValue<DateTime>(0), Is.EqualTo(DateTime.MaxValue));
            Assert.That(reader.GetFieldValue<Instant>(1), Is.EqualTo(Instant.MinValue));
            Assert.That(reader.GetFieldValue<DateTime>(1), Is.EqualTo(DateTime.MinValue));
        }

        [Test]
        public async Task Timestamptz_write_infinity()
        {
            var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString) { ConvertInfinityDateTime = true }.ConnectionString;
            await using var conn = await OpenConnectionAsync(connectionString);
            await using var cmd = new NpgsqlCommand("SELECT $1::text, $2::text, $3::text, $4::text", conn)
            {
                Parameters =
                {
                    new() { Value = Instant.MaxValue },
                    new() { Value = DateTime.MaxValue },
                    new() { Value = Instant.MinValue },
                    new() { Value = DateTime.MinValue }
                }
            };
            await using var reader = await cmd.ExecuteReaderAsync();
            await reader.ReadAsync();

            Assert.That(reader[0], Is.EqualTo("infinity"));
            Assert.That(reader[1], Is.EqualTo("infinity"));
            Assert.That(reader[2], Is.EqualTo("-infinity"));
            Assert.That(reader[3], Is.EqualTo("-infinity"));
        }

        #endregion Timestamp with time zone

        #region Date

        [Test]
        public void Date()
        {
            using var conn = OpenConnection();
            var localDate = new LocalDate(2002, 3, 4);
            var dateTime = new DateTime(localDate.Year, localDate.Month, localDate.Day);

            using (var cmd = new NpgsqlCommand("CREATE TEMP TABLE data (d1 DATE, d2 DATE, d3 DATE, d4 DATE, d5 DATE)", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new NpgsqlCommand("INSERT INTO data VALUES (@p1, @p2, @p3, @p4, @p5)", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Date) { Value = localDate });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p2", Value = localDate });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p3", Value = new LocalDate(-5, 3, 3) });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p4", Value = dateTime });
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p5", Value = dateTime, NpgsqlDbType = NpgsqlDbType.Date });
                cmd.ExecuteNonQuery();
            }

            using (var cmd = new NpgsqlCommand("SELECT d1::TEXT, d2::TEXT, d3::TEXT, d4::TEXT, d5::TEXT FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();
                Assert.That(reader.GetValue(0), Is.EqualTo("2002-03-04"));
                Assert.That(reader.GetValue(1), Is.EqualTo("2002-03-04"));
                Assert.That(reader.GetValue(2), Is.EqualTo("0006-03-03 BC"));
                Assert.That(reader.GetValue(3), Is.EqualTo("2002-03-04"));
                Assert.That(reader.GetValue(4), Is.EqualTo("2002-03-04"));
            }

            using (var cmd = new NpgsqlCommand("SELECT * FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();

                Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(LocalDate)));
                Assert.That(reader.GetFieldValue<LocalDate>(0), Is.EqualTo(localDate));
                Assert.That(reader.GetValue(0), Is.EqualTo(localDate));
                Assert.That(() => reader.GetDateTime(0), Is.EqualTo(dateTime));
                Assert.That(() => reader.GetDate(0), Is.EqualTo(new NpgsqlDate(localDate.Year, localDate.Month, localDate.Day)));
                Assert.That(reader.GetFieldValue<LocalDate>(2), Is.EqualTo(new LocalDate(-5, 3, 3)));
                Assert.That(reader.GetFieldValue<DateTime>(3), Is.EqualTo(dateTime));
                Assert.That(reader.GetDateTime(4), Is.EqualTo(dateTime));
            }
        }

#if NET6_0_OR_GREATER
        [Test]
        public void Date_DateOnly()
        {
            using var conn = OpenConnection();
            var localDate = new LocalDate(2002, 3, 4);
            var dateOnly = new DateOnly(2002, 3, 4);

            using (var cmd = new NpgsqlCommand("CREATE TEMP TABLE data (d1 DATE)", conn))
                cmd.ExecuteNonQuery();

            using (var cmd = new NpgsqlCommand("INSERT INTO data VALUES (@p1)", conn))
            {
                cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p1", Value = dateOnly });
                cmd.ExecuteNonQuery();
            }

            using (var cmd = new NpgsqlCommand("SELECT d1::TEXT FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();
                Assert.That(reader.GetValue(0), Is.EqualTo("2002-03-04"));
            }

            using (var cmd = new NpgsqlCommand("SELECT * FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();

                Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(LocalDate)));
                Assert.That(reader.GetValue(0), Is.EqualTo(localDate));
                Assert.That(reader.GetFieldValue<DateOnly>(0), Is.EqualTo(dateOnly));
            }
        }
#endif

        [Test, Description("Makes sure that when ConvertInfinityDateTime is true, infinity values are properly converted")]
        public void DateConvertInfinity()
        {
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString) { ConvertInfinityDateTime = true };
            using var conn = OpenConnection(csb);
            conn.ExecuteNonQuery("CREATE TEMP TABLE data (d1 DATE, d2 DATE, d3 DATE, d4 DATE)");

            using (var cmd = new NpgsqlCommand("INSERT INTO data VALUES (@p1, @p2, @p3, @p4)", conn))
            {
                cmd.Parameters.AddWithValue("p1", NpgsqlDbType.Date, LocalDate.MaxIsoValue);
                cmd.Parameters.AddWithValue("p2", NpgsqlDbType.Date, LocalDate.MinIsoValue);
                cmd.Parameters.AddWithValue("p3", NpgsqlDbType.Date, DateTime.MaxValue);
                cmd.Parameters.AddWithValue("p4", NpgsqlDbType.Date, DateTime.MinValue);
                cmd.ExecuteNonQuery();
            }

            using (var cmd = new NpgsqlCommand("SELECT d1::TEXT, d2::TEXT, d3::TEXT, d4::TEXT FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();
                Assert.That(reader.GetValue(0), Is.EqualTo("infinity"));
                Assert.That(reader.GetValue(1), Is.EqualTo("-infinity"));
                Assert.That(reader.GetValue(2), Is.EqualTo("infinity"));
                Assert.That(reader.GetValue(3), Is.EqualTo("-infinity"));
            }

            using (var cmd = new NpgsqlCommand("SELECT * FROM data", conn))
            using (var reader = cmd.ExecuteReader())
            {
                reader.Read();
                Assert.That(reader.GetFieldValue<LocalDate>(0), Is.EqualTo(LocalDate.MaxIsoValue));
                Assert.That(reader.GetFieldValue<LocalDate>(1), Is.EqualTo(LocalDate.MinIsoValue));
                Assert.That(reader.GetFieldValue<DateTime>(2), Is.EqualTo(DateTime.MaxValue));
                Assert.That(reader.GetFieldValue<DateTime>(3), Is.EqualTo(DateTime.MinValue));
            }
        }

        #endregion Date

        #region Time

        [Test]
        public void Time()
        {
            using var conn = OpenConnection();
            var expected = new LocalTime(1, 2, 3, 4).PlusNanoseconds(5000);
            var timeSpan = new TimeSpan(0, 1, 2, 3, 4).Add(TimeSpan.FromTicks(50));

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2, @p3", conn);
            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Time) { Value = expected });
            cmd.Parameters.Add(new NpgsqlParameter("p2", DbType.Time) { Value = expected });
            cmd.Parameters.Add(new NpgsqlParameter("p3", DbType.Time) { Value = timeSpan });
            using var reader = cmd.ExecuteReader();
            reader.Read();

            for (var i = 0; i < cmd.Parameters.Count; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(LocalTime)));
                Assert.That(reader.GetFieldValue<LocalTime>(i), Is.EqualTo(expected));
                Assert.That(reader.GetValue(i), Is.EqualTo(expected));
                Assert.That(() => reader.GetTimeSpan(i), Is.EqualTo(timeSpan));
            }
        }

#if NET6_0_OR_GREATER
        [Test]
        public void Time_TimeOnly()
        {
            using var conn = OpenConnection();
            var timeOnly = new TimeOnly(1, 2, 3, 500);
            var localTime = new LocalTime(1, 2, 3, 500);

            using var cmd = new NpgsqlCommand("SELECT @p1", conn);
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p1", Value = timeOnly });

            using var reader = cmd.ExecuteReader();
            reader.Read();

            Assert.That(reader.GetFieldType(0), Is.EqualTo(typeof(LocalTime)));
            Assert.That(reader.GetFieldValue<TimeOnly>(0), Is.EqualTo(timeOnly));
            Assert.That(reader.GetValue(0), Is.EqualTo(localTime));
        }
#endif

        #endregion Time

        #region Time with time zone

        [Test]
        public void TimeTz()
        {
            using var conn = OpenConnection();
            var time = new LocalTime(1, 2, 3, 4).PlusNanoseconds(5000);
            var offset = Offset.FromHoursAndMinutes(3, 30) + Offset.FromSeconds(5);
            var expected = new OffsetTime(time, offset);
            var dateTimeOffset = new DateTimeOffset(0001, 01, 02, 03, 43, 20, TimeSpan.FromHours(3));
            var dateTime = dateTimeOffset.DateTime;

            using var cmd = new NpgsqlCommand("SELECT @p1, @p2, @p3, @p4, @p5, @p6", conn);
            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.TimeTz) { Value = expected });
            cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "p2", Value = expected });
            cmd.Parameters.Add(new NpgsqlParameter("p3", NpgsqlDbType.TimeTz) { Value = dateTimeOffset });
            cmd.Parameters.Add(new NpgsqlParameter("p4", dateTimeOffset));
            cmd.Parameters.Add(new NpgsqlParameter("p5", NpgsqlDbType.TimeTz) { Value = dateTime });
            cmd.Parameters.Add(new NpgsqlParameter("p6", dateTime));

            using var reader = cmd.ExecuteReader();
            reader.Read();

            for (var i = 0; i < 2; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(OffsetTime)));
                Assert.That(reader.GetFieldValue<OffsetTime>(i), Is.EqualTo(expected));
                Assert.That(reader.GetValue(i), Is.EqualTo(expected));
            }
            for (var i = 2; i < 4; i++)
            {
                Assert.That(reader.GetFieldValue<DateTimeOffset>(i), Is.EqualTo(dateTimeOffset));
            }
            for (var i = 4; i < 6; i++)
            {
                Assert.That(reader.GetFieldValue<DateTime>(i), Is.EqualTo(dateTime));
            }
        }

        #endregion Time with time zone

        #region Interval

        [Test]
        public void IntervalAsPeriod()
        {
            // PG has microsecond precision, so sub-microsecond values are stripped
            var expectedPeriod = new PeriodBuilder
            {
                Years = 1,
                Months = 2,
                Weeks = 3,
                Days = 4,
                Hours = 5,
                Minutes = 6,
                Seconds = 7,
                Milliseconds = 8,
                Nanoseconds = 9000
            }.Build().Normalize();

            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);
            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Interval) { Value = expectedPeriod });
            cmd.Parameters.AddWithValue("p2", expectedPeriod);
            using var reader = cmd.ExecuteReader();
            reader.Read();

            for (var i = 0; i < 2; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(Period)));
                Assert.That(reader.GetFieldValue<Period>(i), Is.EqualTo(expectedPeriod));
                Assert.That(reader.GetValue(i), Is.EqualTo(expectedPeriod));
            }
        }

        [Test]
        public void IntervalAsDuration()
        {
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);

            // PG has microsecond precision, so sub-microsecond values are stripped
            var expected = Duration.FromDays(5) + Duration.FromMinutes(4) + Duration.FromSeconds(3) + Duration.FromMilliseconds(2) +
                           Duration.FromNanoseconds(1500);

            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Interval) { Value = expected });
            cmd.Parameters.AddWithValue("p2", expected);
            using var reader = cmd.ExecuteReader();
            reader.Read();
            for (var i = 0; i < 2; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(Period)));
                Assert.That(reader.GetFieldValue<Duration>(i), Is.EqualTo(expected - Duration.FromNanoseconds(500)));
            }
        }

        [Test, IssueLink("https://github.com/npgsql/npgsql/issues/3438")]
        public void Bug3438()
        {
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);

            var expected = Duration.FromSeconds(2148);

            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Interval) { Value = expected });
            cmd.Parameters.AddWithValue("p2", expected);
            using var reader = cmd.ExecuteReader();
            reader.Read();
            for (var i = 0; i < 2; i++)
            {
                Assert.That(reader.GetFieldType(i), Is.EqualTo(typeof(Period)));
            }
        }

        [Test]
        public void IntervalAsTimeSpan()
        {
            var expected = new TimeSpan(1, 2, 3, 4, 5);
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT @p1, @p2", conn);

            cmd.Parameters.Add(new NpgsqlParameter("p1", NpgsqlDbType.Interval) { Value = expected });
            cmd.Parameters.AddWithValue("p2", expected);
            using var reader = cmd.ExecuteReader();
            reader.Read();

            for (var i = 0; i < 2; i++)
            {
                Assert.That(() => reader.GetTimeSpan(i), Is.EqualTo(expected));
                Assert.That(reader.GetFieldValue<TimeSpan>(i), Is.EqualTo(expected));
            }
        }

        [Test]
        public void IntervalAsDurationWithMonthsFails()
        {
            using var conn = OpenConnection();
            using var cmd = new NpgsqlCommand("SELECT make_interval(months => 2)", conn);
            using var reader = cmd.ExecuteReader();
            reader.Read();

            Assert.That(() => reader.GetFieldValue<Duration>(0), Throws.Exception.TypeOf<NpgsqlException>().With.Message.EqualTo(
                "Cannot read PostgreSQL interval with non-zero months to NodaTime Duration. Try reading as a NodaTime Period instead."));
        }

        #endregion Interval

        #region Support

        protected override async ValueTask<NpgsqlConnection> OpenConnectionAsync(string? connectionString = null)
        {
            var conn = new NpgsqlConnection(connectionString ?? ConnectionString);
            await conn.OpenAsync();
            conn.TypeMapper.UseNodaTime();
            return conn;
        }

        #endregion Support
    }
}
