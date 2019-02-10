using Microsoft.Extensions.Caching.Memory;
using System;
using System.Data;
using System.Text;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Кеш запросов к БД.
    /// </summary>
    public class CacheQuery
    {
        private readonly MySqlAdapter adapter;
        private readonly MemoryCache cache;
        private readonly MemoryCacheEntryOptions entryOptions;

        private readonly string queryTemplate;

        public CacheQuery(MySqlAdapter adapter, string queryTemplate, CacheOptions options)
        {
            this.queryTemplate = queryTemplate;
            this.adapter = adapter;

            cache = new MemoryCache(new MemoryCacheOptions() { SizeLimit = options.CacheSize } );
            entryOptions = new MemoryCacheEntryOptions()
            {
                AbsoluteExpirationRelativeToNow = options.AbsoluteExpirationRelativeToNow,
                SlidingExpiration = options.SlidingExpiration,
                Size = 1
            };
        }

        /// <summary>
        /// Запрос на получение данных.
        /// </summary>
        /// <param name="query">Форматированный запрос String.Format</param>
        /// <param name="param">Параметры подстанавки</param>
        /// <returns></returns>
        public DataTable Get(bool cacheNull, params object[] param)
        {
            DataTable result = null;

            StringBuilder hashBuilder = new StringBuilder();
            for (int i = 0; i < param.Length; i++)
            {
                hashBuilder.Append(param[i]);
                hashBuilder.Append('_');
            }
            string paramStr = hashBuilder.ToString();

            if (!cache.TryGetValue(paramStr, out result))
            {
                string query = string.Format(queryTemplate, param);
                result = adapter.Select(query);

                if (cacheNull)
                {
                    cache.Set(paramStr, result, entryOptions);
                }
                else if (result != null)
                {
                    cache.Set(paramStr, result, entryOptions);
                }
            }

            return result;
        }

        /// <summary>
        /// Запрос на полчение скалярной переменной.
        /// </summary>
        /// <param name="query">Форматированный запрос как через String.Format</param>
        /// <param name="param">Параметры подстанавливаемые в запросю</param>
        /// <returns></returns>
        public ScalarResult<T> GetScalar<T>(bool cacheNull, params object[] param)
        {
            ScalarResult<T> result = null;

            StringBuilder hashBuilder = new StringBuilder();
            for (int i = 0; i < param.Length; i++)
            {
                hashBuilder.Append(param[i]);
                hashBuilder.Append('_');
            }
            string paramStr = hashBuilder.ToString();

            if (!cache.TryGetValue(paramStr, out result))
            {
                string query = string.Format(queryTemplate, param);
                result = adapter.SelectScalar<T>(query);

                if (cacheNull)
                {
                    cache.Set(paramStr, result, entryOptions);
                }
                else if (result != null)
                {
                    cache.Set(paramStr, result, entryOptions);
                }
            }

            return result;
        }
    }

    /// <summary>
    /// Параметры кеша.
    /// </summary>
    public class CacheOptions
    {
        /// <summary>
        /// Максимальный размер кеша.
        /// </summary>
        public int CacheSize { get; set; }
        /// <summary>
        /// Время существования записи в кеше.
        /// </summary>
        public TimeSpan? AbsoluteExpirationRelativeToNow { get; set; }
        /// <summary>
        /// Время существования записи в кеше с момента последнего обращения.
        /// </summary>
        public TimeSpan? SlidingExpiration { get; set; }
    }
}
