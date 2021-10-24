using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.BackendMessages;
using Npgsql.Internal;

#pragma warning disable 1591
#pragma warning disable RS0016

namespace Npgsql.Replication.PgOutput.Messages
{
    /// <summary>
    /// Logical Replication Protocol insert message
    /// </summary>
    public sealed class InsertMessage : TransactionalMessage
    {
        readonly ReplicationDataReader _reader;

        RelationMessage _relation = null!;
        RowState _rowState;

        /// <summary>
        /// ID of the relation corresponding to the ID in the relation message.
        /// </summary>
        [Obsolete("Use Relation.RelationId")]
        public uint RelationId => Relation.RelationId;

        /// <summary>
        /// The relation for this <see cref="InsertMessage" />.
        /// </summary>
        public RelationMessage Relation
            => _relation ?? throw new InvalidOperationException("The relation for this message could not be resolved");

        /// <summary>
        /// Returns a sequential <see cref="ReplicationDataReader" /> that can be used to access the column data of the newly inserted row.
        /// </summary>
        /// <remarks>
        /// The returned <see cref="ReplicationDataReader" /> is sequential (does not buffer), and must therefore be traversed from first
        /// to last column; seeking back and re-reading columns is not supported.
        /// </remarks>
        public ReplicationDataReader GetNewRow()
        {
            _reader.Reset(_relation.RowDescription);
            return _reader;
        }

        /// <summary>
        /// Returns a buffered <see cref="ReplicationDataReader" /> that can be used to access the column data of the newly inserted row.
        /// </summary>
        /// <remarks>
        /// The returned <see cref="ReplicationDataReader" /> is buffered. While this allow accessing columns in random order, it may also
        /// use up considerable memory if big columns are involved.
        /// </remarks>
        public ValueTask<ReplicationDataReader> GetNewRowBuffered(CancellationToken cancellationToken)
        {
            _reader.Reset(_relation.RowDescription);
            return _reader.BufferRow();
        }

        /// <summary>
        /// Creates a new instance of <see cref="InsertMessage" />.
        /// </summary>
        internal InsertMessage(NpgsqlConnector connector)
            => _reader = new(this, connector);

        internal InsertMessage Populate(
            NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock, uint? transactionXid,
            RelationMessage relation, ushort numColumns)
        {
            base.Populate(walStart, walEnd, serverClock, transactionXid);

            _relation = relation;
            _rowState = RowState.NotRead;

            return this;
        }

        internal Task Consume(CancellationToken cancellationToken)
            => _reader.ReadAsync(cancellationToken);

        /// <inheritdoc />
#if NET5_0_OR_GREATER
        public override InsertMessage Clone()
#else
        public override PgOutputReplicationMessage Clone()
#endif
        {
            throw new NotImplementedException();
            // var clone = new InsertMessage();
            // clone.Populate(WalStart, WalEnd, ServerClock, TransactionXid, Relation.Clone(), NewRow.ToArray());
            // return clone;
        }

        class TupleEnumerable : IAsyncEnumerable<ReplicationValue>
        {
            readonly InsertMessage _insertMessage;
            readonly TupleEnumerator _tupleEnumerator;

            // TODO: This needs to be exposed to the user.
            ushort _numColumns;
            RowDescriptionMessage _rowDescription = null!;

            internal TupleEnumerable(InsertMessage insertMessage, NpgsqlReadBuffer readBuffer)
                => (_insertMessage, _tupleEnumerator) = (insertMessage, new(insertMessage, readBuffer));

            internal void Reset(ushort numColumns, RowDescriptionMessage rowDescription)
                => (_numColumns, _rowDescription) = (numColumns, rowDescription);

            public IAsyncEnumerator<ReplicationValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
            {
                switch (_insertMessage._rowState)
                {
                case RowState.NotRead:
                    _insertMessage._rowState = RowState.Reading;
                    _tupleEnumerator.Reset(_numColumns, _rowDescription, cancellationToken);
                    return _tupleEnumerator;
                case RowState.Reading:
                    throw new InvalidOperationException("The row is already been read.");
                case RowState.Consumed:
                    throw new InvalidOperationException("The row has already been consumed.");
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }

            internal async Task Consume(CancellationToken cancellationToken)
            {
                switch (_insertMessage._rowState)
                {
                case RowState.NotRead:
                    _insertMessage._rowState = RowState.Reading;
                    _tupleEnumerator.Reset(_numColumns, _rowDescription, cancellationToken);
                    while (await _tupleEnumerator.MoveNextAsync()) { }
                    break;
                case RowState.Reading:
                    while (await _tupleEnumerator.MoveNextAsync()) { }
                    break;
                case RowState.Consumed:
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }
        }

        class TupleEnumerator : IAsyncEnumerator<ReplicationValue>
        {
            readonly InsertMessage _insertMessage;
            readonly NpgsqlReadBuffer _readBuffer;
            readonly ReplicationValue _value;

            ushort _numColumns;
            int _pos;
            RowDescriptionMessage _rowDescription = null!;
            CancellationToken _cancellationToken;

            internal TupleEnumerator(InsertMessage insertMessage, NpgsqlReadBuffer readBuffer)
            {
                _insertMessage = insertMessage;
                _readBuffer = readBuffer;
                _value = new(_readBuffer);
            }

            internal void Reset(ushort numColumns, RowDescriptionMessage rowDescription, CancellationToken cancellationToken)
            {
                _pos = -1;
                _numColumns = numColumns;
                _rowDescription = rowDescription;
                _cancellationToken = cancellationToken;
            }

            public ValueTask<bool> MoveNextAsync()
            {
                if (_insertMessage._rowState != RowState.Reading)
                    throw new ObjectDisposedException(null);

                using (NoSynchronizationContextScope.Enter())
                    return MoveNextCore();

                async ValueTask<bool> MoveNextCore()
                {
                    // Consume the previous column
                    if (_pos != -1)
                        await _value.Consume(_cancellationToken);

                    if (_pos + 1 == _numColumns)
                        return false;
                    _pos++;

                    // Read the next column
                    await _readBuffer.Ensure(1, async: true);
                    var kind = (TupleDataKind)_readBuffer.ReadByte();
                    int len;
                    switch (kind)
                    {
                    case TupleDataKind.Null:
                        len = -1;
                        break;
                    case TupleDataKind.UnchangedToastedValue:
                        len = -2;
                        break;
                    case TupleDataKind.TextValue:
                    case TupleDataKind.BinaryValue:
                        if (_readBuffer.ReadBytesLeft < 4)
                        {
                            using var tokenRegistration = _readBuffer.Connector.StartNestedCancellableOperation(_cancellationToken);
                            await _readBuffer.Ensure(4, async: true);
                        }
                        len = _readBuffer.ReadInt32();
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                    }

                    _value.Reset(len, _rowDescription[_pos]);

                    return true;
                }
            }

            public ReplicationValue Current => _insertMessage._rowState switch
            {
                RowState.NotRead => throw new ObjectDisposedException(null),
                RowState.Reading => _value,
                RowState.Consumed => throw new ObjectDisposedException(null),
                _ => throw new ArgumentOutOfRangeException()
            };

            public async ValueTask DisposeAsync()
            {
                if (_insertMessage._rowState == RowState.Reading)
                    while (await MoveNextAsync()) { /* Do nothing, just iterate the enumerator */ }

                _insertMessage._rowState = RowState.Consumed;
            }
        }

        enum RowState
        {
            NotRead,
            Reading,
            Consumed
        }
    }
}
