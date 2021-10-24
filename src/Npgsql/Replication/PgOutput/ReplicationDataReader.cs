using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal;
using Npgsql.Internal.TypeHandling;
using Npgsql.Replication.PgOutput.Messages;
using Npgsql.Util;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.Replication.PgOutput
{
    public class ReplicationDataReader : DbDataReader
    {
        readonly InsertMessage _insertMessage;
        readonly NpgsqlConnector Connector;
        readonly NpgsqlReadBuffer Buffer;

        RowDescriptionMessage _rowDescription = null!;
        bool _isSequential;
        ReaderState _state;
        int _column;
        TupleDataKind _kind;

        /// <summary>
        /// The number of columns in the current row
        /// </summary>
        int _numColumns;

        /// <summary>
        /// Records, for each column, its starting offset and length in the current row. Used only in non-sequential mode.
        /// </summary>
        readonly List<(int Offset, int Length)> _columns = new();

        /// <summary>
        /// For streaming types (e.g. bytea), holds the byte length of the column. Does not include the length prefix.
        /// </summary>
        internal int ColumnLen;
        NpgsqlReadBuffer.ColumnStream? _columnStream;
        internal int PosInColumn;

        internal ReplicationDataReader(InsertMessage insertMessage, NpgsqlConnector connector)
            => (_insertMessage, Connector, Buffer) = (insertMessage, connector, connector.ReadBuffer);

        internal void Reset(RowDescriptionMessage rowDescription)
        {
            _rowDescription = rowDescription;
            _state = ReaderState.InResult;
            _numColumns = rowDescription.Count;
            _isSequential = true;
            _column = -1;
        }

        internal ValueTask<ReplicationDataReader> BufferRow()
        {
            _isSequential = false;
            throw new NotImplementedException();
        }

        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            // if (typeof(T) == typeof(Stream))
            //     return (Task<T>)(object)GetStreamAsync(ordinal, cancellationToken);
            //
            // if (typeof(T) == typeof(TextReader))
            //     return (Task<T>)(object)GetTextReaderAsync(ordinal, cancellationToken);

            // In non-sequential, we know that the column is already buffered - no I/O will take place
            if (!_isSequential)
                return Task.FromResult(GetFieldValue<T>(ordinal));

            using (NoSynchronizationContextScope.Enter())
                return GetFieldValueSequential<T>(ordinal, true, cancellationToken).AsTask();
        }

        async ValueTask<T> GetFieldValueSequential<T>(int column, bool async, CancellationToken cancellationToken = default)
        {
            using var registration = Connector.StartNestedCancellableOperation(cancellationToken, attemptPgCancellation: false);

            var field = CheckRowAndGetField(column);
            await SeekToColumnSequential(column, async, CancellationToken.None);
            CheckColumnStart();

            switch (_kind)
            {
            case TupleDataKind.Null:
                // When T is a Nullable<T> (and only in that case), we support returning null
                if (NullableHandler<T>.Exists)
                    return default!;

                if (typeof(T) == typeof(object))
                    return (T)(object)DBNull.Value;

                ThrowHelper.ThrowInvalidCastException_NoValue(field);
                break;
            case TupleDataKind.UnchangedToastedValue:
                throw new InvalidCastException("Cannot read an unchanged TOASTed value.");
            }

            var position = Buffer.ReadPosition;
            try
            {
                return NullableHandler<T>.Exists
                    ? ColumnLen <= Buffer.ReadBytesLeft
                        ? NullableHandler<T>.Read(field.Handler, Buffer, ColumnLen, field)
                        : await NullableHandler<T>.ReadAsync(field.Handler, Buffer, ColumnLen, async, field)
                    : typeof(T) == typeof(object)
                        ? ColumnLen <= Buffer.ReadBytesLeft
                            ? (T)field.Handler.ReadAsObject(Buffer, ColumnLen, field)
                            : (T)await field.Handler.ReadAsObject(Buffer, ColumnLen, async, field)
                        : ColumnLen <= Buffer.ReadBytesLeft
                            ? field.Handler.Read<T>(Buffer, ColumnLen, field)
                            : await field.Handler.Read<T>(Buffer, ColumnLen, async, field);
            }
            catch
            {
                if (Connector.State != ConnectorState.Broken)
                {
                    var writtenBytes = Buffer.ReadPosition - position;
                    var remainingBytes = ColumnLen - writtenBytes;
                    if (remainingBytes > 0)
                        await Buffer.Skip(remainingBytes, async);
                }
                throw;
            }
            finally
            {
                // Important: position must still be updated
                PosInColumn += ColumnLen;
            }
        }

        public override object GetValue(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override bool IsDBNull(int ordinal) => throw new NotImplementedException();

        public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken)
        {
            CheckRowAndGetField(ordinal);

            if (!_isSequential)
                return IsDBNull(ordinal) ? PGUtil.TrueTask : PGUtil.FalseTask;

            using (NoSynchronizationContextScope.Enter())
                return IsDBNullAsyncInternal(ordinal, cancellationToken);

            // ReSharper disable once InconsistentNaming
            async Task<bool> IsDBNullAsyncInternal(int ordinal, CancellationToken cancellationToken)
            {
                using var registration = Connector.StartNestedCancellableOperation(cancellationToken, attemptPgCancellation: false);

                await SeekToColumn(ordinal, true, cancellationToken);
                return _kind == TupleDataKind.Null;
            }
        }

        // TODO: Sync of these?
        public Task<bool> IsUnchangedToastedValueAsync(int ordinal) => throw new NotImplementedException();
        public ValueTask<TupleDataKind> GetDataKindAsync(int ordinal) => throw new NotImplementedException();

        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotImplementedException();
        public override Stream GetStream(int ordinal) => throw new NotImplementedException();
        public override TextReader GetTextReader(int ordinal) => throw new NotImplementedException();
        public override DataTable? GetSchemaTable() => throw new NotImplementedException();

        public override string GetDataTypeName(int ordinal) => GetField(ordinal).TypeDisplayName;
        public override Type GetFieldType(int ordinal) => GetField(ordinal).FieldType;
        public override string GetName(int ordinal) => GetField(ordinal).Name;

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        /// <returns>The zero-based column ordinal.</returns>
        public override int GetOrdinal(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("name cannot be empty", nameof(name));
            CheckClosedOrDisposed();
            if (_rowDescription is null)
                throw new InvalidOperationException("No resultset is currently being traversed");

            return _rowDescription.GetFieldIndex(name);
        }

        public override int GetValues(object[] values) => throw new NotImplementedException();

        public override int FieldCount => _numColumns;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override bool HasRows => true;
        public override int RecordsAffected => 1;

        public override bool IsClosed => throw new NotImplementedException();
        public override bool NextResult() => false;
        public override bool Read() => false;

        public override T GetFieldValue<T>(int ordinal) => throw new NotImplementedException();

        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            try
            {
                switch (_state)
                {
                case ReaderState.InResult:
                    using (Connector.StartNestedCancellableOperation(cancellationToken))
                    {
                        await ConsumeRow(async: true);
                    }
                    break;

                case ReaderState.Consumed:
                case ReaderState.Closed:
                case ReaderState.Disposed:
                    break;

                default:
                    throw new ArgumentOutOfRangeException();
                }

                return false;
            }
            catch
            {
                _state = ReaderState.Consumed;
                throw;
            }
        }

        public override int Depth => 0;

        public override IEnumerator GetEnumerator() => throw new NotImplementedException();

        #region Simple value getters

        public override bool GetBoolean(int ordinal) => GetFieldValue<bool>(ordinal);
        public override byte GetByte(int ordinal) => GetFieldValue<byte>(ordinal);
        public override char GetChar(int ordinal) => GetFieldValue<char>(ordinal);
        public override DateTime GetDateTime(int ordinal) => GetFieldValue<DateTime>(ordinal);
        public override decimal GetDecimal(int ordinal) => GetFieldValue<decimal>(ordinal);
        public override double GetDouble(int ordinal) => GetFieldValue<double>(ordinal);
        public override float GetFloat(int ordinal) => GetFieldValue<float>(ordinal);
        public override Guid GetGuid(int ordinal) => GetFieldValue<Guid>(ordinal);
        public override short GetInt16(int ordinal) => GetFieldValue<short>(ordinal);
        public override int GetInt32(int ordinal) => GetFieldValue<int>(ordinal);
        public override long GetInt64(int ordinal) => GetFieldValue<long>(ordinal);
        public override string GetString(int ordinal) => GetFieldValue<string>(ordinal);

        #endregion Simple value getters

        #region Seeking

        Task SeekToColumn(int column, bool async, CancellationToken cancellationToken = default)
        {
            if (_isSequential)
                return SeekToColumnSequential(column, async, cancellationToken);
            SeekToColumnNonSequential(column);
            return Task.CompletedTask;
        }

        void SeekToColumnNonSequential(int column)
        {
            // Shut down any streaming going on on the column
            if (_columnStream != null)
            {
                _columnStream.Dispose();
                _columnStream = null;
            }

            for (var lastColumnRead = _columns.Count; column >= lastColumnRead; lastColumnRead++)
            {
                int lastColumnLen;
                (Buffer.ReadPosition, lastColumnLen) = _columns[lastColumnRead-1];
                if (lastColumnLen != -1)
                    Buffer.ReadPosition += lastColumnLen;
                var len = Buffer.ReadInt32();
                _columns.Add((Buffer.ReadPosition, len));
            }

            (Buffer.ReadPosition, ColumnLen) = _columns[column];
            _column = column;
            PosInColumn = 0;
        }

        /// <summary>
        /// Seeks to the given column. The 4-byte length is read and stored in <see cref="ColumnLen"/>.
        /// </summary>
        async Task SeekToColumnSequential(int column, bool async, CancellationToken cancellationToken = default)
        {
            if (column < 0 || column >= _numColumns)
                throw new IndexOutOfRangeException("Column index out of range");

            if (column < _column)
                throw new InvalidOperationException($"Invalid attempt to read from column ordinal '{column}'. With CommandBehavior.SequentialAccess, you may only read from column ordinal '{_column}' or greater.");

            if (column == _column)
                return;

            // Need to seek forward

            // Shut down any streaming going on on the column
            if (_columnStream != null)
            {
                _columnStream.Dispose();
                _columnStream = null;
                // Disposing the stream leaves us at the end of the column
                PosInColumn = ColumnLen;
            }

            // Skip to end of column if needed
            // TODO: Simplify by better initializing _columnLen/_posInColumn
            var remainingInColumn = ColumnLen == -1 ? 0 : ColumnLen - PosInColumn;
            if (remainingInColumn > 0)
                await Buffer.Skip(remainingInColumn, async);

            // Skip over unwanted fields
            for (; _column < column - 1; _column++)
            {
                await Buffer.Ensure(4, async);
                var len = Buffer.ReadInt32();
                if (len != -1)
                    await Buffer.Skip(len, async);
            }

            await Buffer.Ensure(5, async);
            _kind = (TupleDataKind)Buffer.ReadByte();
            ColumnLen = _kind switch
            {
                TupleDataKind.Null => 0,
                TupleDataKind.UnchangedToastedValue => 0,
                TupleDataKind.TextValue => Buffer.ReadInt32(),
                TupleDataKind.BinaryValue => Buffer.ReadInt32(),
                _ => throw new ArgumentOutOfRangeException()
            };

            PosInColumn = 0;
            _column = column;
        }

        Task SeekInColumn(int posInColumn, bool async, CancellationToken cancellationToken = default)
        {
            if (_isSequential)
                return SeekInColumnSequential(posInColumn, async);

            if (posInColumn > ColumnLen)
                posInColumn = ColumnLen;

            Buffer.ReadPosition = _columns[_column].Offset + posInColumn;
            PosInColumn = posInColumn;
            return Task.CompletedTask;

            async Task SeekInColumnSequential(int posInColumn, bool async)
            {
                Debug.Assert(_column > -1);

                if (posInColumn < PosInColumn)
                    throw new InvalidOperationException("Attempt to read a position in the column which has already been read");

                if (posInColumn > ColumnLen)
                    posInColumn = ColumnLen;

                if (posInColumn > PosInColumn)
                {
                    await Buffer.Skip(posInColumn - PosInColumn, async);
                    PosInColumn = posInColumn;
                }
            }
        }

        #endregion

        async Task ConsumeRow(bool async)
        {
            Debug.Assert(_state == ReaderState.InResult);

            if (_columnStream != null)
            {
                _columnStream.Dispose();
                _columnStream = null;
                // Disposing the stream leaves us at the end of the column
                PosInColumn = ColumnLen;
            }

            // TODO: Potential for code-sharing with ReadColumn above, which also skips
            // Skip to end of column if needed
            var remainingInColumn = _kind switch
            {
                TupleDataKind.Null => 0,
                TupleDataKind.UnchangedToastedValue => 0,
                _ => ColumnLen - PosInColumn
            };
            if (remainingInColumn > 0)
                await Buffer.Skip(remainingInColumn, async);

            // Skip over the remaining columns in the row
            for (; _column < _numColumns - 1; _column++)
            {
                await Buffer.Ensure(4, async);
                var len = Buffer.ReadInt32();
                if (len != -1)
                    await Buffer.Skip(len, async);
            }
        }

        /// <summary>
        /// Checks that we have a RowDescription, but not necessary an actual resultset
        /// (for operations which work in SchemaOnly mode.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        FieldDescription GetField(int column)
        {
            if (_rowDescription == null)
                throw new InvalidOperationException("No resultset is currently being traversed");

            if (column < 0 || column >= _rowDescription.Count)
                throw new IndexOutOfRangeException($"Column must be between {0} and {_rowDescription.Count - 1}");

            return _rowDescription[column];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        FieldDescription CheckRowAndGetField(int column)
        {
            switch (_state)
            {
            case ReaderState.InResult:
                break;
            case ReaderState.Closed:
                throw new InvalidOperationException("The reader is closed");
            case ReaderState.Disposed:
                throw new ObjectDisposedException(nameof(NpgsqlDataReader));
            default:
                throw new InvalidOperationException("No row is available");
            }

            if (column < 0 || column >= _rowDescription!.Count)
                throw new IndexOutOfRangeException($"Column must be between {0} and {_rowDescription!.Count - 1}");

            return _rowDescription[column];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void CheckColumnStart()
        {
            Debug.Assert(_isSequential);
            if (PosInColumn != 0)
                throw new InvalidOperationException("Attempt to read a position in the column which has already been read");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void CheckClosedOrDisposed()
        {
            switch (_state)
            {
            case ReaderState.Closed:
                throw new InvalidOperationException("The reader is closed");
            case ReaderState.Disposed:
                throw new ObjectDisposedException(nameof(NpgsqlDataReader));
            }
        }
    }
}
