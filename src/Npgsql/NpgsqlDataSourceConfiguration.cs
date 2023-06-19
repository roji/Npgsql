using System;
using System.Collections.Generic;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Npgsql.Internal;
using Npgsql.TypeMapping;

namespace Npgsql;

sealed record NpgsqlDataSourceConfiguration(
    NpgsqlLoggingConfiguration LoggingConfiguration,
    EncryptionHandler EncryptionHandler,
    RemoteCertificateValidationCallback? UserCertificateValidationCallback,
    Action<X509CertificateCollection>? ClientCertificatesCallback,
    Func<NpgsqlConnectionStringBuilder, CancellationToken, ValueTask<string>>? PeriodicPasswordProvider,
    TimeSpan PeriodicPasswordSuccessRefreshInterval,
    TimeSpan PeriodicPasswordFailureRefreshInterval,
    List<IPgTypeInfoResolver> ResolverChain,
    IList<UserTypeMapping> UserTypeMappings,
    INpgsqlNameTranslator DefaultNameTranslator,
    Action<NpgsqlConnection>? ConnectionInitializer,
    Func<NpgsqlConnection, Task>? ConnectionInitializerAsync);
