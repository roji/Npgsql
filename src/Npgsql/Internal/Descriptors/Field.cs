using Npgsql.PostgresTypes;

namespace Npgsql.Internal.Descriptors;

/// Base field type shared between tables and composites.
public readonly struct Field
{
    public Field(string name, PgTypeId pgTypeId, int typeModifier)
    {
        Name = name;
        PgTypeId = pgTypeId;
        TypeModifier = typeModifier;
    }

    public string Name { get; }
    public PgTypeId PgTypeId { get; }
    public int TypeModifier { get; }
}
