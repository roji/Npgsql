using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using NpgsqlTypes;

namespace Npgsql.TypeMapping
{
    abstract class TypeMapperBase : INpgsqlTypeMapper
    {
        public INpgsqlNameTranslator DefaultNameTranslator { get; }

        protected TypeMapperBase(INpgsqlNameTranslator defaultNameTranslator)
        {
            if (defaultNameTranslator == null)
                throw new ArgumentNullException(nameof(defaultNameTranslator));

            DefaultNameTranslator = defaultNameTranslator;
        }

        #region Mapping management

        public abstract INpgsqlTypeMapper AddMapping(NpgsqlTypeMapping mapping);
        public abstract bool RemoveMapping(string pgTypeName);
        public abstract IEnumerable<NpgsqlTypeMapping> Mappings { get; }
        public abstract void Reset();

        /// <inheritdoc />
        public abstract INpgsqlTypeMapper MapEnum<TEnum>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where TEnum : struct, Enum;

        /// <inheritdoc />
        public abstract bool UnmapEnum<TEnum>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
            where TEnum : struct, Enum;

        /// <inheritdoc />
        [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        public abstract INpgsqlTypeMapper MapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null);

        /// <inheritdoc />
        [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        public abstract INpgsqlTypeMapper MapComposite(Type clrType, string? pgName = null, INpgsqlNameTranslator? nameTranslator = null);

        /// <inheritdoc />
        public abstract bool UnmapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null);

        /// <inheritdoc />
        public abstract bool UnmapComposite(Type clrType, string? pgName = null, INpgsqlNameTranslator? nameTranslator = null);

        /// <inheritdoc />
        public abstract void AddTypeResolverFactory(ITypeHandlerResolverFactory resolverFactory);

        // [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        // public INpgsqlTypeMapper MapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
        //     => MapComposite(pgName, nameTranslator, typeof(T), t => new CompositeTypeHandlerFactory<T>(t));
        //
        // [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        // public INpgsqlTypeMapper MapComposite(Type clrType, string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
        //     => MapComposite(pgName, nameTranslator, clrType, t => (NpgsqlTypeHandlerFactory)
        //         Activator.CreateInstance(typeof(CompositeTypeHandlerFactory<>).MakeGenericType(clrType), BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic, null, new object[] { t }, null)!);
        //
        // INpgsqlTypeMapper MapComposite(string? pgName, INpgsqlNameTranslator? nameTranslator, Type type, Func<INpgsqlNameTranslator, NpgsqlTypeHandlerFactory> factory)
        // {
        //     if (pgName != null && string.IsNullOrWhiteSpace(pgName))
        //         throw new ArgumentException("pgName can't be empty.", nameof(pgName));
        //
        //     nameTranslator ??= DefaultNameTranslator;
        //     pgName ??= GetPgName(type, nameTranslator);
        //
        //     return AddMapping(
        //         new NpgsqlTypeMappingBuilder
        //         {
        //             PgTypeName = pgName,
        //             ClrTypes = new[] { type },
        //             TypeHandlerFactory = factory(nameTranslator),
        //         }
        //         .Build());
        // }
        //
        // [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        // public bool UnmapComposite<T>(string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
        //     => UnmapComposite(typeof(T), pgName, nameTranslator);
        //
        // [RequiresUnreferencedCode("Composite type mapping currently isn't trimming-safe.")]
        // public bool UnmapComposite(Type clrType, string? pgName = null, INpgsqlNameTranslator? nameTranslator = null)
        // {
        //     if (pgName != null && string.IsNullOrWhiteSpace(pgName))
        //         throw new ArgumentException("pgName can't be empty.", nameof(pgName));
        //
        //     nameTranslator ??= DefaultNameTranslator;
        //     pgName ??= GetPgName(clrType, nameTranslator);
        //
        //     return RemoveMapping(pgName);
        // }

        #endregion Composite mapping

        #region Misc

        private protected static string GetPgName(Type clrType, INpgsqlNameTranslator nameTranslator)
            => clrType.GetCustomAttribute<PgNameAttribute>()?.PgName
               ?? nameTranslator.TranslateTypeName(clrType.Name);

        #endregion Misc
    }
}
