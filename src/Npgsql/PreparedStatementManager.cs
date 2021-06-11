﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using Npgsql.Internal;
using Npgsql.Logging;

namespace Npgsql
{
    class PreparedStatementManager
    {
        internal int MaxAutoPrepared { get; }
        internal int UsagesBeforePrepare { get; }

        internal Dictionary<string, CachedSqlEntry> BySql { get; } = new();
        readonly CachedSqlEntry[] _autoPrepared;
        int _numAutoPrepared;

        readonly CachedSqlEntry?[] _candidates;

        /// <summary>
        /// Total number of current prepared statements (whether explicit or automatic).
        /// </summary>
        internal int NumPrepared;

        readonly NpgsqlConnector _connector;

        internal string NextPreparedStatementName() => "_p" + (++_preparedStatementIndex);
        ulong _preparedStatementIndex;

        static readonly NpgsqlLogger Log = NpgsqlLogManager.CreateLogger(nameof(PreparedStatementManager));

        internal const int CandidateCount = 100;

        internal PreparedStatementManager(NpgsqlConnector connector)
        {
            _connector = connector;
            MaxAutoPrepared = connector.Settings.MaxAutoPrepare;
            UsagesBeforePrepare = connector.Settings.AutoPrepareMinUsages;
            if (MaxAutoPrepared > 0)
            {
                if (MaxAutoPrepared > 256)
                    Log.Warn($"{nameof(MaxAutoPrepared)} is over 256, performance degradation may occur. Please report via an issue.", connector.Id);
                _autoPrepared = new CachedSqlEntry[MaxAutoPrepared];
                _candidates = new CachedSqlEntry[CandidateCount];
            }
            else
            {
                _autoPrepared = null!;
                _candidates = null!;
            }
        }

        internal CachedSqlEntry? GetOrAddExplicit(NpgsqlStatement statement)
        {
            var sql = statement.SQL;

            CachedSqlEntry? statementBeingReplaced = null;
            if (BySql.TryGetValue(sql, out var pStatement))
            {
                Debug.Assert(pStatement.State != PreparedState.Unprepared);
                if (pStatement.IsExplicit)
                {
                    // Great, we've found an explicit prepared statement.
                    // We just need to check that the parameter types correspond, since prepared statements are
                    // only keyed by SQL (to prevent pointless allocations). If we have a mismatch, simply run unprepared.
                    return pStatement.DoParametersMatch(statement.InputParameters)
                        ? pStatement
                        : null;
                }

                // We've found an autoprepare statement (candidate or otherwise)
                switch (pStatement.State)
                {
                case PreparedState.NotPrepared:
                    // Found a candidate for autopreparation. Remove it and prepare explicitly.
                    RemoveCandidate(pStatement);
                    break;
                case PreparedState.Prepared:
                    // The statement has already been autoprepared. We need to "promote" it to explicit.
                    statementBeingReplaced = pStatement;
                    break;
                case PreparedState.Unprepared:
                    throw new InvalidOperationException($"Found unprepared statement in {nameof(PreparedStatementManager)}");
                default:
                    throw new ArgumentOutOfRangeException();
                }
            }

            // Statement hasn't been prepared yet
            return BySql[sql] = CachedSqlEntry.CreateExplicit(this, sql, NextPreparedStatementName(), statement.InputParameters, statementBeingReplaced);
        }

        internal CachedSqlEntry CreateAutoPrepareCandidate(string sql)
        {
            // New candidate. Find an empty candidate slot or eject a least-used one.
            int slotIndex = -1, leastUsages = int.MaxValue;
            var lastUsed = DateTime.MaxValue;
            for (var i = 0; i < _candidates.Length; i++)
            {
                var candidate = _candidates[i];
                // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                // ReSharper disable HeuristicUnreachableCode
                if (candidate == null)  // Found an unused candidate slot, return immediately
                {
                    slotIndex = i;
                    break;
                }
                // ReSharper restore HeuristicUnreachableCode
                if (candidate.Usages < leastUsages)
                {
                    leastUsages = candidate.Usages;
                    slotIndex = i;
                    lastUsed = candidate.LastUsed;
                }
                else if (candidate.Usages == leastUsages && candidate.LastUsed < lastUsed)
                {
                    slotIndex = i;
                    lastUsed = candidate.LastUsed;
                }
            }

            var leastUsed = _candidates[slotIndex];
            // ReSharper disable once ConditionIsAlwaysTrueOrFalse
            if (leastUsed != null)
                BySql.Remove(leastUsed.Sql);

            return BySql[sql] = _candidates[slotIndex] = CachedSqlEntry.CreateAutoPrepareCandidate(this, sql);
        }

        internal bool TryAutoPrepare(CachedSqlEntry cachedSqlEntry, List<NpgsqlParameter> parameters)
        {
            switch (cachedSqlEntry.State)
            {
            case PreparedState.NotPrepared:
                break;

            case PreparedState.Prepared:
            case PreparedState.BeingPrepared:
                // The statement has already been prepared (explicitly or automatically), or has been selected
                // for preparation (earlier identical statement in the same command).
                // We just need to check that the parameter types correspond, since prepared statements are
                // only keyed by SQL (to prevent pointless allocations). If we have a mismatch, simply run unprepared.
                if (!cachedSqlEntry.DoParametersMatch(parameters))
                    return false;

                // Prevent this statement from being replaced within this batch
                cachedSqlEntry.LastUsed = DateTime.MaxValue;
                return true;

            case PreparedState.BeingUnprepared:
                // The statement is being replaced by an earlier statement in this same batch.
                return false;

            default:
                Debug.Fail($"Unexpected {nameof(PreparedState)} in auto-preparation: {cachedSqlEntry.State}");
                break;
            }

            if (++cachedSqlEntry.Usages < UsagesBeforePrepare)
            {
                // Statement still hasn't passed the usage threshold, no automatic preparation.
                // Return null for unprepared execution.
                cachedSqlEntry.LastUsed = DateTime.UtcNow;
                return false;
            }

            // Bingo, we've just passed the usage threshold, statement should get prepared
            Log.Trace($"Automatically preparing statement: {cachedSqlEntry.Sql}", _connector.Id);

            if (_numAutoPrepared < MaxAutoPrepared)
            {
                // We still have free slots
                _autoPrepared[_numAutoPrepared++] = cachedSqlEntry;
                cachedSqlEntry.Name = "_auto" + _numAutoPrepared;
            }
            else
            {
                // We already have the maximum number of prepared statements.
                // Find the least recently used prepared statement and replace it.
                var foundUnpreparedSlot = false;
                var oldestTimestamp = DateTime.MaxValue;
                var oldestIndex = -1;
                for (var i = 0; i < _autoPrepared.Length; i++)
                {
                    var slot = _autoPrepared[i];

                    switch (slot.State)
                    {
                    case PreparedState.Prepared:
                        if (slot.LastUsed < oldestTimestamp)
                        {
                            oldestIndex = i;
                            oldestTimestamp = slot.LastUsed;
                        }
                        break;

                    case PreparedState.BeingPrepared:
                        // Slot has already been selected for preparation by an earlier statement in this batch. Skip it.
                        continue;

                    case PreparedState.Unprepared:
                        // Found an unprepared statement slot; this can occur if a previous preparation failed because of an error.
                        // Use that immediately, no need to continue looking for an LRU.
                        cachedSqlEntry.Name = slot.Name;
                        _autoPrepared[i] = cachedSqlEntry;
                        foundUnpreparedSlot = true;
                        break;

                    case PreparedState.BeingUnprepared:
                    case PreparedState.NotPrepared:
                        throw new Exception(
                            $"Invalid {nameof(PreparedState)} state {slot.State} encountered when scanning prepared statement slots");

                    default:
                        throw new ArgumentOutOfRangeException($"Unknown {nameof(PreparedState)}: {slot.State}");
                    }
                }

                if (!foundUnpreparedSlot)
                {
                    if (oldestIndex == -1)
                    {
                        // We're here if we couldn't find a prepared statement to replace, because all of them are already
                        // being prepared in this batch.
                        return false;
                    }

                    var selectedSlot = _autoPrepared[oldestIndex];
                    cachedSqlEntry.Name = selectedSlot.Name;
                    cachedSqlEntry.StatementBeingReplaced = selectedSlot;
                    selectedSlot.State = PreparedState.BeingUnprepared;
                    _autoPrepared[oldestIndex] = cachedSqlEntry;
                }
            }

            RemoveCandidate(cachedSqlEntry);

            // Make sure this statement isn't replaced by a later statement in the same batch.
            cachedSqlEntry.LastUsed = DateTime.MaxValue;

            // Note that the parameter types are only set at the moment of preparation - in the candidate phase
            // there's no differentiation between overloaded statements, which are a pretty rare case, saving
            // allocations.

            cachedSqlEntry.SetParamTypes(parameters);

            return true;
        }

        void RemoveCandidate(CachedSqlEntry candidate)
        {
            var i = 0;
            for (; i < _candidates.Length; i++)
            {
                if (_candidates[i] == candidate)
                {
                    _candidates[i] = null;
                    return;
                }
            }
            Debug.Assert(i < _candidates.Length);
        }

        internal void ClearAll()
        {
            BySql.Clear();
            NumPrepared = 0;
            _preparedStatementIndex = 0;
            _numAutoPrepared = 0;
            if (_candidates != null)
                for (var i = 0; i < _candidates.Length; i++)
                    _candidates[i] = null;
        }
    }
}
