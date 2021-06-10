﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Npgsql.BackendMessages;

namespace Npgsql
{
    /// <summary>
    /// Represents a single SQL statement within Npgsql.
    ///
    /// Instances aren't constructed directly; users should construct an <see cref="NpgsqlCommand"/>
    /// object and populate its <see cref="NpgsqlCommand.CommandText"/> property as in standard ADO.NET.
    /// Npgsql will analyze that property and construct instances of <see cref="NpgsqlStatement"/>
    /// internally.
    ///
    /// Users can retrieve instances from <see cref="NpgsqlDataReader.Statements"/>
    /// and access information about statement execution (e.g. affected rows).
    /// </summary>
    public sealed class NpgsqlStatement
    {
        /// <summary>
        /// The SQL text of the statement.
        /// </summary>
        public string SQL { get; set; } = string.Empty;

        /// <summary>
        /// Specifies the type of query, e.g. SELECT.
        /// </summary>
        public StatementType StatementType { get; internal set; }

        /// <summary>
        /// The number of rows affected or retrieved.
        /// </summary>
        /// <remarks>
        /// See the command tag in the CommandComplete message,
        /// https://www.postgresql.org/docs/current/static/protocol-message-formats.html
        /// </remarks>
        public uint Rows => (uint)LongRows;

        /// <summary>
        /// The number of rows affected or retrieved.
        /// </summary>
        /// <remarks>
        /// See the command tag in the CommandComplete message,
        /// https://www.postgresql.org/docs/current/static/protocol-message-formats.html
        /// </remarks>
        public ulong LongRows { get; internal set; }

        /// <summary>
        /// For an INSERT, the object ID of the inserted row if <see cref="Rows"/> is 1 and
        /// the target table has OIDs; otherwise 0.
        /// </summary>
        public uint OID { get; internal set; }

        /// <summary>
        /// The input parameters sent with this statement.
        /// </summary>
        [AllowNull]
        public List<NpgsqlParameter> InputParameters
        {
            get => _inputParameters!;
            internal set
            {
                _inputParameters = value;
                IsParameterListOwned = false;
            }
        }

        List<NpgsqlParameter>? _inputParameters = new();

        internal bool IsParameterListOwned { get; set; } = true;

        /// <summary>
        /// The RowDescription message for this query. If null, the query does not return rows (e.g. INSERT)
        /// </summary>
        internal RowDescriptionMessage? Description
        {
            get => PreparedStatement == null ? _description : PreparedStatement.Description;
            set
            {
                if (PreparedStatement == null)
                    _description = value;
                else
                    PreparedStatement.Description = value;
            }
        }

        RowDescriptionMessage? _description;

        /// <summary>
        /// If this statement has been automatically prepared, references the <see cref="PreparedStatement"/>.
        /// Null otherwise.
        /// </summary>
        internal CachedSqlEntry? PreparedStatement
        {
            get => _preparedStatement != null && _preparedStatement.State == PreparedState.Unprepared
                ? _preparedStatement = null
                : _preparedStatement;
            set => _preparedStatement = value;
        }

        CachedSqlEntry? _preparedStatement;

        internal bool IsPreparing;

        /// <summary>
        /// Holds the server-side (prepared) statement name. Empty string for non-prepared statements.
        /// </summary>
        internal string StatementName => PreparedStatement?.Name ?? "";

        /// <summary>
        /// Whether this statement has already been prepared (including automatic preparation).
        /// </summary>
        internal bool IsPrepared => PreparedStatement?.IsPrepared == true;

        internal void Reset(bool requireOwnedParameterList = true)
        {
            SQL = string.Empty;
            StatementType = StatementType.Select;
            _description = null;
            LongRows = 0;
            OID = 0;
            PreparedStatement = null;

            if (IsParameterListOwned)
                InputParameters.Clear();
            else if (requireOwnedParameterList)
                InputParameters = new();
            else
                InputParameters = null;
        }

        internal void ApplyCommandComplete(CommandCompleteMessage msg)
        {
            StatementType = msg.StatementType;
            LongRows = msg.Rows;
            OID = msg.OID;
        }

        /// <summary>
        /// Returns the SQL text of the statement.
        /// </summary>
        public override string ToString() => SQL ?? "<none>";
    }
}
