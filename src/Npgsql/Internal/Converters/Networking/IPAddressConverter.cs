using System.Net;
using System.Net.Sockets;

// ReSharper disable once CheckNamespace
namespace Npgsql.Internal.Converters;

sealed class IPAddressConverter : PgBufferedConverter<IPAddress>
{
    public override Size GetSize(SizeContext context, IPAddress value, ref object? writeState)
        => NpgsqlInetConverter.DoGetSize(context, value, ref writeState);

    protected override IPAddress ReadCore(PgReader reader)
        => NpgsqlInetConverter.DoReadCore(reader, shouldBeCidr: false).Address;

    protected override void WriteCore(PgWriter writer, IPAddress value)
        => NpgsqlInetConverter.DoWriteCore(
            writer,
            (value, (byte)(value.AddressFamily == AddressFamily.InterNetwork ? 32 : 128)),
            isCidr: false);
}
