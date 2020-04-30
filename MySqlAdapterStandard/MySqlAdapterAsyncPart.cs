using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ITsoft.Extensions.MySql
{
    public partial class MySqlAdapter
    {
        /// <summary>
        /// Выполняет запрос и возвращает количество затронутых строк.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns></returns>
        public async Task<int> ExecuteAsync(string query, int? timeOut, bool? retryOnError)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };

            result = await _ExecuteAsync(context, async (connection, commandTimeOut) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    return await command.ExecuteNonQueryAsync();
                }
            });

            return result;
        }

        /// <summary>
        /// Выпоняет запрос и возвращает количество затронутых строк.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns></returns>
        public Task<int> ExecuteAsync(string query, int? timeOut = null)
        {
            return ExecuteAsync(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде таблицы <see cref="DataTable"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Табица с данными или null в случае ошибки выполнения запроса.</returns>
        public async Task<DataTable> SelectAsync(string query, int? timeOut, bool? retryOnError)
        {
            DataTable result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = await _ExecuteAsync(context, async (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataTable();
                    await adapter.FillAsync(result);

                    return result;
                }
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде таблицы <see cref="DataTable"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Табица с данными или null в случае ошибки выполнения запроса.</returns>
        public Task<DataTable> SelectAsync(string query, int? timeOut = null)
        {
            return SelectAsync(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде набора данных <see cref="DataSet"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="retryOnError"></param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Набор данных или null в случае ошибки выполнения запроса.</returns>
        public async Task<DataSet> SelectDataSetAsync(string query, int? timeOut, bool? retryOnError)
        {
            DataSet result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = await _ExecuteAsync(context, async (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataSet();
                    await adapter.FillAsync(result);

                    return result;
                }
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде набора данных <see cref="DataSet"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Набор данных или null в случае ошибки выполнения запроса.</returns>
        public Task<DataSet> SelectDataSetAsync(string query, int? timeOut = null)
        {
            return SelectDataSetAsync(query, timeOut);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде одной строки <see cref="DataRow"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Строка или null в случае ошибки выполнения запроса.</returns>
        public async Task<DataRow> SelectRowAsync(string query, int? timeOut, bool? retryOnError)
        {
            DataRow result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = await _Execute(context, async (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    var tmp = new DataTable();
                    await adapter.FillAsync(tmp);
                    if (tmp.Rows.Count > 0)
                    {
                        return tmp.Rows[0];
                    }

                    return null;
                }
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде одной строки <see cref="DataRow"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Строка или null в случае ошибки выполнения запроса.</returns>
        public Task<DataRow> SelectRowAsync(string query, int? timeOut = null)
        {
            return SelectRowAsync(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде экземпяра <see cref="ScalarResult{T}"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Строка или null в случае ошибки выполнения запроса.</returns>
        public async Task<ScalarResult<T>> SelectScalarAsync<T>(string query, int? timeOut, bool? retryOnError)
        {
            ScalarResult<T> result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = await _Execute(context, async (connection, commandTimeOut) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    object queryResult = await command.ExecuteScalarAsync();
                    var tmp = new ScalarResult<T>() { Value = default };

                    if (!Convert.IsDBNull(queryResult) && queryResult != null)
                    {
                        var convertType = typeof(T);
                        tmp.Value = (T)Convert.ChangeType(queryResult, convertType);
                    }
                    else
                    {
                        tmp.DbNull = true;
                    }

                    return tmp;
                }
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде экземпяра <see cref="ScalarResult{T}"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Экземпяр <see cref="ScalarResult{T}"/> или null в случае ошибки выполнения запроса.</returns>
        public Task<ScalarResult<T>> SelectScalarAsync<T>(string query, int? timeOut = null)
        {
            return SelectScalarAsync<T>(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и позвояет читать данные построчно.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Количество прочитанных строк.</returns>
        public async Task<int> SelectReaderAsync(string query, Action<Dictionary<string, object>> handler, int? timeOut, bool? retryOnError)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = await _Execute(context, async (connection, commandTimeOut) =>
            {
                int counter = 0;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    var reader = await command.ExecuteReaderAsync();
                    while (reader.Read())
                    {
                        Dictionary<string, object> row = new Dictionary<string, object>(reader.FieldCount);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string name = reader.GetName(i);
                            row.Add(name, reader.GetValue(i));
                        }

                        handler?.Invoke(row);

                        counter++;
                    }
                }

                return counter;
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и позвояет читать данные построчно.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Количество прочитанных строк.</returns>
        public Task<int> SelectReaderAsync(string query, Action<Dictionary<string, object>> handler, int? timeOut = null)
        {
            return SelectReaderAsync(query, handler, timeOut, RetryOnError);
        }

        private async Task<T> _ExecuteAsync<T>(QueryContext queryContext, Func<MySqlConnection, int, Task<T>> queryFunc)
        {
            Interlocked.Increment(ref _runningQueries);

            T result = default;
            try
            {
                do
                {
                    try
                    {
                        using (MySqlConnection connection = new MySqlConnection(_connectionString))
                        {
                            await connection.OpenAsync();
                            result = await queryFunc(connection, queryContext.CommandTimeOut);
                            await connection.CloseAsync();
                            break;
                        }
                    }
                    catch (MySqlException ex)
                    {
                        //получаем обработчик ошибки
                        if (_exceptionHandlers.TryGetValue(ex.Number, out Action<Exception, QueryContext> action))
                        {
                            action?.Invoke(ex, queryContext);
                            ErrorProcessed?.Invoke(ex, queryContext);
                        }
                        else
                        {
                            Error?.Invoke(ex, queryContext);
                            return result;
                        }
                    }
                    catch (Exception ex)
                    {
                        if (_exceptionHandlers.TryGetValue(-1, out Action<Exception, QueryContext> action))
                        {
                            action?.Invoke(ex, queryContext);
                            ErrorProcessed?.Invoke(ex, queryContext);
                        }
                        else
                        {
                            Error?.Invoke(ex, queryContext);
                            return result;
                        }
                    }

                    //Если разрешено зацикливание запроса
                    if (queryContext.Retry)
                    {
                        Thread.Sleep(LoopTimeOut);//делаем паузу в запросах
                    }
                } while (queryContext.Retry);
            }
            finally
            {
                Interlocked.Decrement(ref _runningQueries);
            }

            return result;
        }
    }
}
