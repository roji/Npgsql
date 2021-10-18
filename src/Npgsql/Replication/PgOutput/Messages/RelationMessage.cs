using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Npgsql.BackendMessages;

namespace Npgsql.Replication.PgOutput.Messages
{
    /// <summary>
    /// Logical Replication Protocol relation message
    /// </summary>
    public sealed class RelationMessage : TransactionalMessage
    {
        /// <summary>
        /// ID of the relation.
        /// </summary>
        public uint RelationId { get; private set; }

        /// <summary>
        /// Namespace (empty string for pg_catalog).
        /// </summary>
        public string Namespace { get; private set; } = string.Empty;

        /// <summary>
        /// Relation name.
        /// </summary>
        public string RelationName { get; private set; } = string.Empty;

        /// <summary>
        /// Replica identity setting for the relation (same as relreplident in pg_class).
        /// </summary>
        public char RelationReplicaIdentitySetting { get; private set; }

        /// <summary>
        /// Relation columns
        /// </summary>
        public IReadOnlyList<Column> Columns => InternalColumns;

        internal ReadOnlyArrayBuffer<Column> InternalColumns { get; private set; } = ReadOnlyArrayBuffer<Column>.Empty;

        internal RowDescriptionMessage RowDescription { get; set; } = null!;

        internal RelationMessage Populate(
            NpgsqlLogSequenceNumber walStart, NpgsqlLogSequenceNumber walEnd, DateTime serverClock, uint? transactionXid, uint relationId, string ns,
            string relationName, char relationReplicaIdentitySetting)
        {
            base.Populate(walStart, walEnd, serverClock, transactionXid);

            RelationId = relationId;
            Namespace = ns;
            RelationName = relationName;
            RelationReplicaIdentitySetting = relationReplicaIdentitySetting;

            return this;
        }

        /// <inheritdoc />
#if NET5_0_OR_GREATER
        public override RelationMessage Clone()
#else
        public override PgOutputReplicationMessage Clone()
#endif
        {
            var clone = new RelationMessage();
            clone.Populate(WalStart, WalEnd, ServerClock, TransactionXid, RelationId, Namespace, RelationName, RelationReplicaIdentitySetting);
            clone.InternalColumns = ((ReadOnlyArrayBuffer<Column>)Columns).Clone();
            clone.RowDescription = RowDescription.Clone();
            return clone;
        }

        /// <summary>
        /// Represents a column in a Logical Replication Protocol relation message
        /// </summary>
        public readonly struct Column
        {
            internal Column(ColumnFlags flags, string columnName, uint dataTypeId, int typeModifier)
            {
                Flags = flags;
                ColumnName = columnName;
                DataTypeId = dataTypeId;
                TypeModifier = typeModifier;
            }

            /// <summary>
            /// Flags for the column.
            /// </summary>
            public ColumnFlags Flags { get; }

            /// <summary>
            /// Name of the column.
            /// </summary>
            public string ColumnName { get; }

            /// <summary>
            /// ID of the column's data type.
            /// </summary>
            public uint DataTypeId { get; }

            /// <summary>
            /// Type modifier of the column (atttypmod).
            /// </summary>
            public int TypeModifier { get; }

            /// <summary>
            /// Flags for the column.
            /// </summary>
            public enum ColumnFlags
            {
                /// <summary>
                /// No flags.
                /// </summary>
                None = 0,

                /// <summary>
                /// Marks the column as part of the key.
                /// </summary>
                PartOfKey = 1
            }
        }
    }
}
