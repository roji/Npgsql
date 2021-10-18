using System;
using System.Data;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.Replication.PgOutput.Messages
{
    public class ReplicationValue
    {
        readonly NpgsqlReadBuffer _readBuffer;

        public int Length { get; private set; }
        int _bytePos;
        FieldDescription _fieldDescription = null!;

        internal ReplicationValue(NpgsqlReadBuffer readBuffer)
            => _readBuffer = readBuffer;

        internal void Reset(int length, FieldDescription fieldDescription)
        {
            Length = length;
            _bytePos = 0;
            _fieldDescription = fieldDescription;
        }

        // ReSharper disable once InconsistentNaming
        public bool IsDBNull()
            => Length == -1;

        public bool IsUnchangedToastedValue()
            => Length == -2;

        public ValueTask<T> GetAsync<T>(CancellationToken cancellationToken = default)
        {
            using (NoSynchronizationContextScope.Enter())
                return GetAsyncCore(cancellationToken);

            async ValueTask<T> GetAsyncCore(CancellationToken cancellationToken)
            {
                if (_bytePos > 0)
                    throw new NotSupportedException("Column has already been consumed");

                // TODO: Consume column on exception
                // TODO: Non-generic Get
                // TODO: Column streaming

                // TODO: Do this only if we need to do I/O, for perf
                using var tokenRegistration = _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);

                switch (Length)
                {
                case -1: // Null
                    // When T is a Nullable<T> (and only in that case), we support returning null
                    if (NullableHandler<T>.Exists)
                        return default!;

                    if (typeof(T) == typeof(object))
                        return (T)(object)DBNull.Value;

                    ThrowHelper.ThrowInvalidCastException_NoValue(_fieldDescription);
                    break;
                case -2: // Unchanged TOAST value
                    throw new InvalidCastException(
                        $"Column '{_fieldDescription.Name}' is an unchanged TOASTed value (actual value not sent).");
                }

                // TODO: Add non-generic GetAsync? Not allow passing object in the generic like NpgsqlDataReader? Not allow nullable?
                var value = NullableHandler<T>.Exists
                    ? await NullableHandler<T>.ReadAsync(_fieldDescription.Handler, _readBuffer, Length, async: true, _fieldDescription)
                    : typeof(T) == typeof(object)
                        ? (T)await _fieldDescription.Handler.ReadAsObject(_readBuffer, Length, async: true, _fieldDescription)
                        : await _fieldDescription.Handler.Read<T>(_readBuffer, Length, async: true, _fieldDescription);

                _bytePos += Length;
                return value;

                // try
                // {
                //     // TODO: Check if already consumed or in the middle of streaming
                //
                //     switch (_kind)
                //     {
                //     case TupleDataKind.TextValue:
                //     {
                //         // if (typeof(T) != typeof(object) && typeof(T).IsAssignableFrom(typeof(Stream)))
                //         //     return (T)(object)GetStreamInternal();
                //
                //         if (typeof(T) != typeof(string))
                //             throw new NotSupportedException("Replication data is in text format, only strings can be read.");
                //
                //         using var tokenRegistration = _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                //         await _readBuffer.Ensure(Length, async);
                //
                //         return (T)(object)_readBuffer.ReadString(Length);
                //     }
                //     case TupleDataKind.BinaryValue:
                //     {
                //         if (typeof(T) != typeof(object))
                //         {
                //             if (typeof(T).IsAssignableFrom(typeof(Stream)))
                //                 return (T)(object)GetStreamInternal();
                //             if (typeof(T).IsAssignableFrom(typeof(IDataReader)))
                //             {
                //                 // ToDo: NpgsqlNestedDataReader
                //                 throw new NotSupportedException();
                //             }
                //         }
                //
                //         using var tokenRegistration = IsBuffered
                //             ? default
                //             : _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                //         await _readBuffer.Ensure(Length, async);
                //
                //         return NullableHandler<T>.Exists
                //             ? NullableHandler<T>.Read(_fieldDescription.Handler, _readBuffer, Length, _fieldDescription)
                //             : typeof(T) == typeof(object)
                //                 ? (T)_fieldDescription.Handler.ReadAsObject(_readBuffer, Length, _fieldDescription)
                //                 : _fieldDescription.Handler.Read<T>(_readBuffer, Length, _fieldDescription);
                //     }
                //     case TupleDataKind.Null:
                //     {
                //         if (NullableHandler<T>.Exists)
                //             return default!;
                //
                //         if (typeof(T) == typeof(object) || typeof(T) == typeof(DBNull))
                //             return (T)(object)DBNull.Value;
                //
                //         throw new InvalidOperationException($"You can not convert {nameof(DBNull)} to {nameof(T)}.");
                //     }
                //     case TupleDataKind.UnchangedToastedValue:
                //     {
                //         if (typeof(T) == typeof(object) || typeof(T) == typeof(UnchangedToasted))
                //             return (T)(object)UnchangedToasted.Value;
                //
                //         throw new InvalidOperationException("You can not access an unchanged toasted value.");
                //     }
                //     default:
                //         throw new NpgsqlException(
                //             $"Unexpected {nameof(TupleDataKind)} with value '{_tupleDataKind}'. Please report this as bug.");
                //     }
                // }
                // catch
                // {
                //     if (_readBuffer.Connector.State != ConnectorState.Broken)
                //     {
                //         var bytesRead = _readBuffer.ReadPosition - _startPosition;
                //         var remainingBytes = Length - bytesRead;
                //         if (remainingBytes > 0)
                //             await _readBuffer.Skip(remainingBytes, async);
                //     }
                //
                //     throw;
                // }
            }
        }

        internal async Task Consume(CancellationToken cancellationToken)
        {
            if (_bytePos < Length)
            {
                if (_readBuffer.ReadBytesLeft < 4)
                {
                    using var tokenRegistration = _readBuffer.Connector.StartNestedCancellableOperation(cancellationToken);
                    await _readBuffer.Skip(Length - _bytePos, async: true);
                }
                else
                {
                    await _readBuffer.Skip(Length - _bytePos, async: true);
                }
            }
        }
    }
}