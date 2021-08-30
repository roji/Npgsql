namespace Npgsql
{
    /// <summary>
    /// Holds well-known, built-in PostgreSQL type OIDs.
    /// </summary>
    /// <remarks>
    /// Source: <see href="https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat" />
    /// </remarks>
    static class PostgresTypeOIDs
    {
        // Numeric
        internal const uint Int8         = 20;
        internal const uint Int8Array    = 1016;
        internal const uint Float8       = 701;
        internal const uint Float8Array  = 1022;
        internal const uint Int4         = 23;
        internal const uint Int4Array    = 1007;
        internal const uint Numeric      = 1700;
        internal const uint NumericArray = 1231;
        internal const uint Float4       = 700;
        internal const uint Float4Array  = 1021;
        internal const uint Int2         = 21;
        internal const uint Int2Array    = 1005;
        internal const uint Money        = 790;
        internal const uint MoneyArray   = 791;

        // Boolean
        internal const uint Bool        = 16;

        // Geometric
        internal const uint Box         = 603;
        internal const uint Circle      = 718;
        internal const uint Line        = 628;
        internal const uint LSeg        = 601;
        internal const uint Path        = 602;
        internal const uint Point       = 600;
        internal const uint Polygon     = 604;

        // Character
        internal const uint BPChar      = 1042;
        internal const uint Text        = 25;
        internal const uint Varchar     = 1043;
        internal const uint Name        = 19;
        internal const uint Char        = 18;

        // Binary data
        internal const uint Bytea       = 17;

        // Date/Time
        internal const uint Date        = 1082;
        internal const uint Time        = 1083;
        internal const uint Timestamp   = 1114;
        internal const uint TimestampTz = 1184;
        internal const uint Interval    = 1186;
        internal const uint TimeTz      = 1266;
        internal const uint Abstime     = 702;

        // Network address
        internal const uint Inet        = 869;
        internal const uint Cidr        = 650;
        internal const uint Macaddr     = 829;
        internal const uint Macaddr8    = 774;

        // Bit string
        internal const uint Bit         = 1560;
        internal const uint Varbit      = 1562;

        // Text search
        internal const uint TsVector    = 3614;
        internal const uint TsQuery     = 3615;
        internal const uint Regconfig   = 3734;

        // UUID
        internal const uint Uuid        = 2950;

        // XML
        internal const uint Xml         = 142;

        // JSON
        internal const uint Json        = 114;
        internal const uint Jsonb       = 3802;
        internal const uint JsonPath    = 4072;

        // Internal
        internal const uint Refcursor   = 1790;
        internal const uint Oidvector   = 30;
        internal const uint Int2vector  = 22;
        internal const uint Oid         = 26;
        internal const uint Xid         = 28;
        internal const uint Xid8        = 5069;
        internal const uint Cid         = 29;
        internal const uint Regtype     = 2206;
        internal const uint Tid         = 27;

        // Special
        internal const uint Record      = 2249;
        internal const uint Void        = 2278;
        internal const uint Unknown     = 705;
    }
}
