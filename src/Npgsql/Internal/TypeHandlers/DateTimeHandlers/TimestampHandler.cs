using System;
using Npgsql.BackendMessages;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using NpgsqlTypes;
using static Npgsql.Util.Statics;
using static Npgsql.Internal.TypeHandlers.DateTimeHandlers.DateTimeUtils;

namespace Npgsql.Internal.TypeHandlers.DateTimeHandlers
{
    /// <summary>
    /// A factory for type handlers for the PostgreSQL timestamp data type.
    /// </summary>
    /// <remarks>
    /// See https://www.postgresql.org/docs/current/static/datatype-datetime.html.
    ///
    /// The type handler API allows customizing Npgsql's behavior in powerful ways. However, although it is public, it
    /// should be considered somewhat unstable, and may change in breaking ways, including in non-major releases.
    /// Use it at your own risk.
    /// </remarks>
    public class TimestampHandlerFactory : NpgsqlTypeHandlerFactory<DateTime>
    {
        /// <inheritdoc />
        public override NpgsqlTypeHandler<DateTime> Create(PostgresType postgresType, NpgsqlConnector conn)
            => conn.DatabaseInfo.HasIntegerDateTimes  // Check for the legacy floating point timestamps feature
                ? new TimestampHandler(postgresType, conn.ConvertInfinityDateTime)
                : throw new NotSupportedException($"The deprecated floating-point date/time format is not supported by {nameof(Npgsql)}.");
    }

    /// <summary>
    /// A type handler for the PostgreSQL timestamp data type.
    /// </summary>
    /// <remarks>
    /// See https://www.postgresql.org/docs/current/static/datatype-datetime.html.
    ///
    /// The type handler API allows customizing Npgsql's behavior in powerful ways. However, although it is public, it
    /// should be considered somewhat unstable, and may change in breaking ways, including in non-major releases.
    /// Use it at your own risk.
    /// </remarks>
    public partial class TimestampHandler : NpgsqlSimpleTypeHandlerWithPsv<DateTime, NpgsqlDateTime>
    {
        /// <summary>
        /// Whether to convert positive and negative infinity values to DateTime.{Max,Min}Value when
        /// a DateTime is requested
        /// </summary>
        protected readonly bool ConvertInfinityDateTime;

        /// <summary>
        /// Constructs a <see cref="TimestampHandler"/>.
        /// </summary>
        public TimestampHandler(PostgresType postgresType, bool convertInfinityDateTime)
        {
            PostgresType = postgresType;
            ConvertInfinityDateTime = convertInfinityDateTime;
        }

        #region Read

        /// <inheritdoc />
        public override DateTime Read(NpgsqlReadBuffer buf, int len, FieldDescription? fieldDescription = null)
            => ReadDateTime(buf, ConvertInfinityDateTime, DateTimeKind.Unspecified);

        /// <inheritdoc />
        protected override NpgsqlDateTime ReadPsv(NpgsqlReadBuffer buf, int len, FieldDescription? fieldDescription = null)
            => ReadNpgsqlDateTime(buf, len, fieldDescription);

        #endregion Read

        #region Write

        /// <inheritdoc />
        public override int ValidateAndGetLength(DateTime value, NpgsqlParameter? parameter)
        {
            if (!LegacyTimestampBehavior && value.Kind == DateTimeKind.Utc)
            {
                throw new InvalidCastException(
                    "Cannot write DateTime with Kind=UTC to PostgreSQL type 'timestamp without time zone', considering using 'timestamp with time zone'. " +
                    "See the Npgsql.EnableLegacyTimestampBehavior AppContext switch to enable legacy behavior.");
            }

            return 8;
        }

        /// <inheritdoc />
        public override int ValidateAndGetLength(NpgsqlDateTime value, NpgsqlParameter? parameter)
        {
            if (!LegacyTimestampBehavior && value.Kind == DateTimeKind.Utc)
            {
                throw new InvalidCastException(
                    "Cannot write NpgsqlDateTime with Kind=UTC to PostgreSQL type 'timestamp without time zone', considering using 'timestamp with time zone'. " +
                    "See the Npgsql.EnableLegacyTimestampBehavior AppContext switch to enable legacy behavior.");
            }

            return 8;
        }

        /// <inheritdoc />
        public override void Write(DateTime value, NpgsqlWriteBuffer buf, NpgsqlParameter? parameter)
            => WriteTimestamp(value, buf, ConvertInfinityDateTime);

        /// <inheritdoc />
        public override void Write(NpgsqlDateTime value, NpgsqlWriteBuffer buf, NpgsqlParameter? parameter)
            => WriteTimestamp(value, buf, ConvertInfinityDateTime);

        #endregion Write
    }
}
