using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal.Postgres;
using static Npgsql.Internal.Converters.AsyncHelpers;

namespace Npgsql.Internal.Converters;

// NULL writing is always responsibility of the caller writing the length, so there is not much we do here.
/// Special value converter to be able to use struct converters as System.Nullable converters, it delegates all behavior to the effective converter.
sealed class NullableConverter<T> : PgConverter<T?> where T : struct
{
    readonly PgConverter<T> _effectiveConverter;
    public NullableConverter(PgConverter<T> effectiveConverter)
        : base(effectiveConverter.DbNullPredicateKind is DbNullPredicate.Custom)
        => _effectiveConverter = effectiveConverter;

    protected override bool IsDbNullValue(T? value)
        => value is null || _effectiveConverter.IsDbNull(value.GetValueOrDefault());

    public override bool CanConvert(DataFormat format, out BufferRequirements bufferRequirements)
    {
        var result = _effectiveConverter.CanConvert(format, out var reqs);
        // We have to map exact sizes to upper bounds as values may be null (and thus have a 0 size).
        bufferRequirements = BufferRequirements.Create(
            reqs.Read.Kind is SizeKind.Unknown ? Size.Unknown : Size.CreateUpperBound(reqs.Read.Value),
            reqs.Write.Kind is SizeKind.Unknown ? Size.Unknown : Size.CreateUpperBound(reqs.Write.Value)
        );
        return result;
    }

    public override T? Read(PgReader reader)
        => _effectiveConverter.Read(reader);

    public override unsafe ValueTask<T?> ReadAsync(PgReader reader, CancellationToken cancellationToken = default)
    {
        // Easy if we have all the data.
        var task = _effectiveConverter.ReadAsync(reader, cancellationToken);
        if (task.IsCompletedSuccessfully)
            return new(task.Result);

        // Otherwise we do one additional allocation, this allow us to share state machine codegen for all Ts.
        var source = new CompletionSource<T?>();
        AwaitTask(task.AsTask(), source, new(this, &UnboxAndComplete));
        return source.Task;

        static void UnboxAndComplete(Task task, CompletionSource completionSource)
        {
            Debug.Assert(task is Task<T>);
            Debug.Assert(completionSource is CompletionSource<T?>);
            Unsafe.As<CompletionSource<T?>>(completionSource).SetResult(new ValueTask<T>(Unsafe.As<Task<T>>(task)).Result);
        }
    }

    public override Size GetSize(SizeContext context, T? value, ref object? writeState)
    {
        if (context.BufferRequirement.Kind is not SizeKind.Unknown)
            return context.BufferRequirement.Value;

        return _effectiveConverter.GetSize(context, value.GetValueOrDefault(), ref writeState);
    }

    public override void Write(PgWriter writer, T? value)
        => _effectiveConverter.Write(writer, value.GetValueOrDefault());

    public override ValueTask WriteAsync(PgWriter writer, T? value, CancellationToken cancellationToken = default)
        => _effectiveConverter.WriteAsync(writer, value.GetValueOrDefault(), cancellationToken);

    internal override ValueTask<object> ReadAsObject(bool async, PgReader reader, CancellationToken cancellationToken)
        => _effectiveConverter.ReadAsObject(async, reader, cancellationToken);

    internal override ValueTask WriteAsObject(bool async, PgWriter writer, object value, CancellationToken cancellationToken)
        => _effectiveConverter.WriteAsObject(async, writer, value, cancellationToken);
}

sealed class NullableConverterResolver<T> : PgComposingConverterResolver<T?> where T : struct
{
    public NullableConverterResolver(PgResolverTypeInfo effectiveTypeInfo)
        : base(effectiveTypeInfo.PgTypeId, effectiveTypeInfo) { }

    protected override PgTypeId GetEffectivePgTypeId(PgTypeId pgTypeId) => pgTypeId;
    protected override PgTypeId GetPgTypeId(PgTypeId effectivePgTypeId) => effectivePgTypeId;

    protected override PgConverter<T?> CreateConverter(PgConverterResolution effectiveResolution)
        => new NullableConverter<T>(effectiveResolution.GetConverter<T>());

    protected override PgConverterResolution? GetEffectiveResolution(T? value, PgTypeId? expectedEffectivePgTypeId)
        => value is null
            ? EffectiveTypeInfo.GetDefaultResolution(expectedEffectivePgTypeId)
            : EffectiveTypeInfo.GetResolution(value.GetValueOrDefault(), expectedEffectivePgTypeId);
}
