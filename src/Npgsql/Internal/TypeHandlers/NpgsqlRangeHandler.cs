using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal.TypeHandling;
using Npgsql.PostgresTypes;
using NpgsqlTypes;

namespace Npgsql.Internal.TypeHandlers
{
    /// <summary>
    /// A type handler for PostgreSQL range types.
    /// </summary>
    /// <remarks>
    /// See https://www.postgresql.org/docs/current/static/rangetypes.html.
    ///
    /// The type handler API allows customizing Npgsql's behavior in powerful ways. However, although it is public, it
    /// should be considered somewhat unstable, and may change in breaking ways, including in non-major releases.
    /// Use it at your own risk.
    /// </remarks>
    /// <typeparam name="TSubtype">The range subtype.</typeparam>
    public partial class NpgsqlRangeHandler<TSubtype> : NpgsqlTypeHandler<NpgsqlRange<TSubtype>>
    {
        /// <summary>
        /// The type handler for the subtype that this range type holds
        /// </summary>
        protected NpgsqlTypeHandler SubtypeHandler { get; }

        /// <inheritdoc />
        public NpgsqlRangeHandler(PostgresType rangePostgresType, NpgsqlTypeHandler subtypeHandler)
            : base(rangePostgresType)
            => SubtypeHandler = subtypeHandler;

        /// <inheritdoc />
        public override NpgsqlTypeHandler CreateArrayHandler(PostgresArrayType pgArrayType, ArrayNullabilityMode arrayNullabilityMode)
            => new ArrayHandler<NpgsqlRange<TSubtype>>(pgArrayType, this, arrayNullabilityMode);

        public override Type GetFieldType(FieldDescription? fieldDescription = null) => typeof(NpgsqlRange<TSubtype>);
        public override Type GetProviderSpecificFieldType(FieldDescription? fieldDescription = null) => typeof(NpgsqlRange<TSubtype>);

        /// <inheritdoc />
        public override NpgsqlTypeHandler CreateRangeHandler(PostgresType pgRangeType)
            => throw new NotSupportedException();

        #region Read

        /// <inheritdoc />
        public override ValueTask<NpgsqlRange<TSubtype>> Read(NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription = null)
            => ReadRange<TSubtype>(buf, len, async, fieldDescription);

        protected async ValueTask<NpgsqlRange<TAnySubtype>> ReadRange<TAnySubtype>(NpgsqlReadBuffer buf, int len, bool async, FieldDescription? fieldDescription)
        {
            await buf.Ensure(1, async);

            var flags = (RangeFlags)buf.ReadByte();
            if ((flags & RangeFlags.Empty) != 0)
                return NpgsqlRange<TAnySubtype>.Empty;

            var lowerBound = default(TAnySubtype);
            var upperBound = default(TAnySubtype);

            if ((flags & RangeFlags.LowerBoundInfinite) == 0)
                lowerBound = await SubtypeHandler.ReadWithLength<TAnySubtype>(buf, async);

            if ((flags & RangeFlags.UpperBoundInfinite) == 0)
                upperBound = await SubtypeHandler.ReadWithLength<TAnySubtype>(buf, async);

            return new NpgsqlRange<TAnySubtype>(lowerBound, upperBound, flags);
        }

        #endregion

        #region Write

        /// <inheritdoc />
        public override int ValidateAndGetLength(NpgsqlRange<TSubtype> value, [NotNullIfNotNull("lengthCache")] ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
            => ValidateAndGetLengthRange(value, ref lengthCache, parameter);

        protected int ValidateAndGetLengthRange<TAnySubtype>(NpgsqlRange<TAnySubtype> value, ref NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter)
        {
            var totalLen = 1;
            var lengthCachePos = lengthCache?.Position ?? 0;
            if (!value.IsEmpty)
            {
                if (!value.LowerBoundInfinite)
                {
                    totalLen += 4;
                    if (value.LowerBound is not null)
                        totalLen += SubtypeHandler.ValidateAndGetLength(value.LowerBound, ref lengthCache, null);
                }

                if (!value.UpperBoundInfinite)
                {
                    totalLen += 4;
                    if (value.UpperBound is not null)
                        totalLen += SubtypeHandler.ValidateAndGetLength(value.UpperBound, ref lengthCache, null);
                }
            }

            // If we're traversing an already-populated length cache, rewind to first element slot so that
            // the elements' handlers can access their length cache values
            if (lengthCache != null && lengthCache.IsPopulated)
                lengthCache.Position = lengthCachePos;

            return totalLen;
        }

        /// <inheritdoc />
        public override Task Write(NpgsqlRange<TSubtype> value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter, bool async, CancellationToken cancellationToken = default)
            => WriteRange(value, buf, lengthCache, parameter, async, cancellationToken);

        protected async Task WriteRange<TAnySubtype>(NpgsqlRange<TAnySubtype> value, NpgsqlWriteBuffer buf, NpgsqlLengthCache? lengthCache, NpgsqlParameter? parameter, bool async, CancellationToken cancellationToken = default)
        {
            if (buf.WriteSpaceLeft < 1)
                await buf.Flush(async, cancellationToken);

            buf.WriteByte((byte)value.Flags);

            if (value.IsEmpty)
                return;

            if (!value.LowerBoundInfinite)
                await SubtypeHandler.WriteWithLength(value.LowerBound, buf, lengthCache, null, async, cancellationToken);

            if (!value.UpperBoundInfinite)
                await SubtypeHandler.WriteWithLength(value.UpperBound, buf, lengthCache, null, async, cancellationToken);
        }

        #endregion
    }
}
