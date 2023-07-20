using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal.Postgres;
using NpgsqlTypes;

namespace Npgsql.Internal.Converters;

public class RangeConverter<TSubtype> : PgStreamingConverter<NpgsqlRange<TSubtype>>
{
    readonly PgConverter<TSubtype> _subtypeConverter;
    readonly BufferRequirements _subtypeRequirements;

    public RangeConverter(PgConverter<TSubtype> subtypeConverter)
    {
        if (!subtypeConverter.CanConvert(DataFormat.Binary, out var bufferRequirements))
            throw new NotSupportedException("Range subtype converter has to support the binary format to be compatible.");
        _subtypeRequirements = bufferRequirements;
        _subtypeConverter = subtypeConverter;
    }

    public override NpgsqlRange<TSubtype> Read(PgReader reader)
        => Read(async: false, reader, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask<NpgsqlRange<TSubtype>> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
        => Read(async: true, reader, cancellationToken);

    async ValueTask<NpgsqlRange<TSubtype>> Read(bool async, PgReader reader, CancellationToken cancellationToken)
    {
        if (reader.ShouldBuffer(sizeof(byte)))
            await reader.BufferData(async, sizeof(byte), cancellationToken).ConfigureAwait(false);

        var flags = (RangeFlags)reader.ReadByte();
        if ((flags & RangeFlags.Empty) != 0)
            return NpgsqlRange<TSubtype>.Empty;

        var lowerBound = default(TSubtype);
        var upperBound = default(TSubtype);

        var readRequirement = _subtypeRequirements.Read;
        var converter = _subtypeConverter;
        if ((flags & RangeFlags.LowerBoundInfinite) == 0)
        {
            if (reader.ShouldBuffer(sizeof(int)))
                await reader.BufferData(async, sizeof(int), cancellationToken).ConfigureAwait(false);
            var length = reader.ReadInt32();

            // Note that we leave the CLR default for nulls
            if (length != -1)
            {
                await using var _ = await reader
                    .BeginNestedRead(async, length, readRequirement, cancellationToken).ConfigureAwait(false);
                lowerBound = async
                    ? await converter.ReadAsync(reader, cancellationToken).ConfigureAwait(false)
                    // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                    : converter.Read(reader);
            }
        }

        if ((flags & RangeFlags.UpperBoundInfinite) == 0)
        {
            if (reader.ShouldBuffer(sizeof(int)))
                await reader.BufferData(async, sizeof(int), cancellationToken).ConfigureAwait(false);
            var length = reader.ReadInt32();

            // Note that we leave the CLR default for nulls
            if (length != -1)
            {
                // Set size before calling ShouldBuffer (it needs to be able to resolve an upper bound requirement)
                await using var _ = await reader
                    .BeginNestedRead(async, length, readRequirement, cancellationToken).ConfigureAwait(false);

                upperBound = async
                    ? await converter.ReadAsync(reader, cancellationToken).ConfigureAwait(false)
                    // ReSharper disable once MethodHasAsyncOverloadWithCancellation
                    : converter.Read(reader);
            }
        }

        return new NpgsqlRange<TSubtype>(lowerBound, upperBound, flags);
    }

    public override Size GetSize(SizeContext context, NpgsqlRange<TSubtype> value, ref object? writeState)
    {
        var totalSize = Size.Create(1);
        if (value.IsEmpty)
            return totalSize; // Just flags.

        WriteState? state = null;
        if (!value.LowerBoundInfinite)
        {
            totalSize = totalSize.Combine(sizeof(int));
            var subTypeState = (object?)null;
            if (_subtypeConverter.GetSizeOrDbNull(_subtypeRequirements.Write, context, value.LowerBound, ref subTypeState) is { } size)
            {
                totalSize = totalSize.Combine(size);
                (state ??= new WriteState()).LowerBoundSize = size;
                state.LowerBoundWriteState = subTypeState;
            }
            else if (state is not null)
                state.LowerBoundSize = -1;
        }

        if (!value.UpperBoundInfinite)
        {
            totalSize = totalSize.Combine(sizeof(int));
            var subTypeState = (object?)null;
            if (_subtypeConverter.GetSizeOrDbNull(_subtypeRequirements.Write, context, value.UpperBound, ref subTypeState) is { } size)
            {
                totalSize = totalSize.Combine(size);
                (state ??= new WriteState()).UpperBoundSize = size;
                state.UpperBoundWriteState = subTypeState;
            }
            else if (state is not null)
                state.UpperBoundSize = -1;
        }

        writeState = state;
        return totalSize;
    }

    public override void Write(PgWriter writer, NpgsqlRange<TSubtype> value)
        => Write(async: false, writer, value, CancellationToken.None).GetAwaiter().GetResult();

    public override ValueTask WriteAsync(PgWriter writer, NpgsqlRange<TSubtype> value, CancellationToken cancellationToken = default)
        => Write(async: true, writer, value, cancellationToken);

    async ValueTask Write(bool async, PgWriter writer, NpgsqlRange<TSubtype> value, CancellationToken cancellationToken)
    {
        var writeState = writer.Current.WriteState as WriteState;
        var lowerBoundSize = writeState?.LowerBoundSize ?? -1;
        var upperBoundSize = writeState?.UpperBoundSize ?? -1;

        var flags = value.Flags;
        if (!value.IsEmpty)
        {
            // Normalize nulls to infinite, as pg does.
            if (lowerBoundSize == -1 && !value.LowerBoundInfinite)
                flags = (flags & ~RangeFlags.LowerBoundInclusive) | RangeFlags.LowerBoundInfinite;

            if (upperBoundSize == -1 && !value.UpperBoundInfinite)
                flags = (flags & ~RangeFlags.UpperBoundInclusive) | RangeFlags.UpperBoundInfinite;
        }

        if (writer.ShouldFlush(sizeof(byte)))
            await writer.Flush(async, cancellationToken).ConfigureAwait(false);
        writer.WriteByte((byte)flags);
        if (value.IsEmpty)
            return;

        if (!flags.HasFlag(RangeFlags.LowerBoundInfinite))
        {
            Debug.Assert(lowerBoundSize.Kind is SizeKind.Exact && lowerBoundSize.Value != -1);
            var length = lowerBoundSize.Value;
            if (writer.ShouldFlush(sizeof(int))) // Length
                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            writer.WriteInt32(length);
            if (async)
                await writer.NestedWriteAsync(_subtypeConverter, value.LowerBound!, lowerBoundSize,
                    writeState!.LowerBoundWriteState, cancellationToken);
            else
                writer.NestedWrite(_subtypeConverter, value.LowerBound!, lowerBoundSize, writeState!.LowerBoundWriteState);
        }

        if (!flags.HasFlag(RangeFlags.UpperBoundInfinite))
        {
            Debug.Assert(upperBoundSize.Kind is SizeKind.Exact && upperBoundSize.Value != -1);
            var length = upperBoundSize.Value;
            if (writer.ShouldFlush(sizeof(int))) // Length
                await writer.Flush(async, cancellationToken).ConfigureAwait(false);
            writer.WriteInt32(length);
            if (async)
                await writer.NestedWriteAsync(_subtypeConverter, value.UpperBound!, upperBoundSize,
                    writeState!.UpperBoundWriteState, cancellationToken);
            else
                writer.NestedWrite(_subtypeConverter, value.UpperBound!, upperBoundSize, writeState!.UpperBoundWriteState);
        }
    }

    sealed class WriteState
    {
        // ReSharper disable InconsistentNaming
        internal Size LowerBoundSize;
        internal object? LowerBoundWriteState;
        internal Size UpperBoundSize;
        internal object? UpperBoundWriteState;
        // ReSharper restore InconsistentNaming
    }
}

sealed class RangeConverterResolver<TSubtype> : PgComposingConverterResolver<NpgsqlRange<TSubtype>>
{
    public RangeConverterResolver(PgResolverTypeInfo subtypeTypeInfo)
        : base(subtypeTypeInfo.PgTypeId is {} id ? subtypeTypeInfo.Options.GetRangeTypeId(id) : null, subtypeTypeInfo) { }

    PgSerializerOptions Options => EffectiveTypeInfo.Options;

    protected override PgTypeId GetEffectivePgTypeId(PgTypeId pgTypeId) => Options.GetRangeSubtypeTypeId(pgTypeId);
    protected override PgTypeId GetPgTypeId(PgTypeId effectivePgTypeId) => Options.GetRangeTypeId(effectivePgTypeId);

    protected override PgConverter<NpgsqlRange<TSubtype>> CreateConverter(PgConverterResolution effectiveResolution)
        => new RangeConverter<TSubtype>(effectiveResolution.GetConverter<TSubtype>());

    protected override PgConverterResolution? GetEffectiveResolution(NpgsqlRange<TSubtype> value, PgTypeId? expectedEffectiveTypeId)
    {
        // Resolve both sides to make sure we end up with consistent PgTypeIds.
        PgConverterResolution? resolution = null;
        if (!value.LowerBoundInfinite)
            resolution = EffectiveTypeInfo.GetResolution(value.LowerBound, expectedEffectiveTypeId);
        if (!value.UpperBoundInfinite)
            resolution = EffectiveTypeInfo.GetResolution(value.UpperBound, resolution?.PgTypeId);

        return resolution;
    }
}
