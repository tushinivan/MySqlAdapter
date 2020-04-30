using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Клиент доступа к серверу MySQL.
    /// </summary>
    public class MySqlAdapter
    {
        /// <summary>
        /// Выполнять запрос выдавший ошибку, через интервал LoopTimeOut.
        /// </summary>
        public bool RetryOnError { get; set; } = true;

        /// <summary>
        /// Время между повторами запросов в мс. По умолчанию 10000 мс. (10 сек)
        /// </summary>
        public TimeSpan LoopTimeOut { get; set; } = new TimeSpan(0, 0, 10);

        /// <summary>
        /// Время ожидания выполнения запроса. По умолчанию 30 сек.
        /// </summary>
        public int DefaultTimeOut { get; set; } = 300;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ex">Ошибка.</param>
        /// <param name="queryContext">Контекст выпонения запроса.</param>
        public delegate void ErrorArgs(Exception ex, QueryContext queryContext);

        /// <summary>
        /// Событие происходящее при возникновении ошибки при выполнении запроса.
        /// </summary>
        public event ErrorArgs Error;

        /// <summary>
        /// Событие происходящее при стандартной обработке ошибки выполнения запроса.
        /// </summary>
        public event ErrorArgs ErrorProcessed;

        private int _runningQueries;
        private readonly string _connectionString;
        private Dictionary<int, Action<Exception, QueryContext>> _exceptionHandlers = new Dictionary<int, Action<Exception, QueryContext>>();
        private static Dictionary<string, CacheQuery> _caches = new Dictionary<string, CacheQuery>();//кешированные запросы

        /// <summary>
        /// Создает экземпляр из строки подключения.
        /// </summary>
        /// <param name="connectionString">Строка подключения.</param>
        public MySqlAdapter(string connectionString)
        {
            if (connectionString != null)
            {
                this._connectionString = connectionString;
                AddDefaultHandlers();
            }
            else
            {
                throw new Exception("Не задана строка подключения.");
            }
        }

        /// <summary>
        /// Создает экземпляр читая строку подключения из файла.
        /// </summary>
        /// <param name="connectionName">Идентификатор подключения.</param>
        /// <param name="connectionFile">Файл содержащий строку подключения.</param>
        public MySqlAdapter(string connectionName, string connectionFile)
        {
            if (connectionName == null || connectionName.Length == 0)
            {
                throw new NullReferenceException("Connection name must not null or empty.");
            }

            if (connectionFile == null || connectionFile.Length == 0)
            {
                throw new NullReferenceException("Connection file must not null or empty.");
            }

            Regex regexStrings = new Regex("(?<name>\".+?\"):(?<connectionString>\".+?\")", RegexOptions.IgnoreCase);

            var lines = File.ReadAllLines(connectionFile, Encoding.UTF8);
            foreach (var line in lines)
            {
                var match = regexStrings.Match(line);
                if (match.Success)
                {
                    if (connectionName != null)
                    {
                        if (match.Groups["name"].Value.Trim('"') == connectionName)
                        {
                            _connectionString = match.Groups["connectionString"].Value.Trim('"');
                            break;
                        }
                    }
                    else
                    {
                        _connectionString = match.Groups["connectionString"].Value.Trim('"');
                        break;
                    }
                }
            }

            if (_connectionString == null)
            {
                throw new Exception($"Can not found connection name \"{connectionName}\" in file \"{connectionFile}\"");
            }
            else
            {
                AddDefaultHandlers();
            }
        }

        /// <summary>
        /// Выполняет запрос и возвращает количество затронутых строк.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns></returns>
        public int Execute(string query, int? timeOut, bool? retryOnError)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };

            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    return command.ExecuteNonQuery();
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
        public int Execute(string query, int? timeOut = null)
        {
            return Execute(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде таблицы <see cref="DataTable"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <returns>Табица с данными или null в случае ошибки выполнения запроса.</returns>
        public DataTable Select(string query, int? timeOut, bool? retryOnError)
        {
            DataTable result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataTable();
                    adapter.Fill(result);

                    return result;
                }
            });

            return result;
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде таблицы <see cref="DataTable"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut"></param>
        /// <returns>Табица с данными или null в случае ошибки выполнения запроса.</returns>
        public DataTable Select(string query, int? timeOut = null)
        {
            return Select(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде набора данных <see cref="DataSet"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="retryOnError"></param>
        /// <param name="timeOut"></param>
        /// <returns>Набор данных или null в случае ошибки выполнения запроса.</returns>
        public DataSet SelectDataSet(string query, int? timeOut, bool? retryOnError)
        {
            DataSet result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    result = new DataSet();
                    adapter.Fill(result);

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
        public DataSet SelectDataSet(string query, int? timeOut = null)
        {
            return SelectDataSet(query, timeOut);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде одной строки <see cref="DataRow"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Строка или null в случае ошибки выполнения запроса.</returns>
        public DataRow SelectRow(string query, int? timeOut, bool? retryOnError)
        {
            DataRow result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlDataAdapter adapter = new MySqlDataAdapter(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    adapter.SelectCommand.CommandTimeout = commandTimeOut;

                    var tmp = new DataTable();
                    adapter.Fill(tmp);
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
        public DataRow SelectRow(string query, int? timeOut = null)
        {
            return SelectRow(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и возвращает данные в виде экземпяра <see cref="ScalarResult{T}"/>.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Строка или null в случае ошибки выполнения запроса.</returns>
        public ScalarResult<T> SelectScalar<T>(string query, int? timeOut, bool? retryOnError)
        {
            ScalarResult<T> result = null;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    object queryResult = command.ExecuteScalar();
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
        public ScalarResult<T> SelectScalar<T>(string query, int? timeOut = null)
        {
            return SelectScalar<T>(query, timeOut, RetryOnError);
        }

        /// <summary>
        /// Выполняет запрос и позвояет читать данные построчно.
        /// </summary>
        /// <param name="query">SQL запрос.</param>
        /// <param name="timeOut">Таймаут выполнения SQL запроса.</param>
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Количество прочитанных строк.</returns>
        public int SelectReader(string query, Action<Dictionary<string, object>> handler, int? timeOut, bool? retryOnError)
        {
            int result = -1;

            QueryContext context = new QueryContext()
            {
                Query = query,
                Retry = retryOnError.HasValue ? retryOnError.Value : RetryOnError,
                CommandTimeOut = timeOut.HasValue ? timeOut.Value : DefaultTimeOut
            };
            result = _Execute(context, (connection, commandTimeOut) =>
            {
                int counter = 0;
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                    command.CommandTimeout = commandTimeOut;

                    var reader = command.ExecuteReader();
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
        /// <param name="retryOnError">Повторять выполнение запроса при возникновении ошибки.</param>
        /// <returns>Количество прочитанных строк.</returns>
        public int SelectReader(string query, Action<Dictionary<string, object>> handler, int? timeOut = null)
        {
            return SelectReader(query, handler, timeOut, RetryOnError);
        }

        /// <summary>
        /// Получает кеш запрсов по имени или создает новый.
        /// </summary>
        /// <param name="name">Название</param>
        /// <returns></returns>
        public CacheQuery GetCacheQuery(string name, string query, CacheOptions options)
        {
            CacheQuery result = null;
            lock (_caches)
            {
                if (!_caches.TryGetValue(name, out result))
                {
                    result = new CacheQuery(this, query, options);
                    _caches.Add(name, result);
                }
            }
            return result;
        }

        /// <summary>
        /// Получить текущее время сервера MySQL.
        /// </summary>
        /// <returns></returns>
        public DateTime GetServerDateTime()
        {
            var result = SelectScalar<DateTime>("SELECT NOW();");
            if (result != null && !result.DbNull)
            {
                return result.Value;
            }

            throw new Exception("Execute error");
        }

        /// <summary>
        /// Экранирует симвлы в строке.
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        public static string EscapeString(string str)
        {
            if (str != null)
            {
                return MySqlHelper.EscapeString(str);
            }
            return str;
        }

        private T _Execute<T>(QueryContext queryContext, Func<MySqlConnection, int, T> queryFunc)
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
                            connection.Open();
                            result = queryFunc(connection, queryContext.CommandTimeOut);
                            connection.Close();
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

        private void AddDefaultHandlers()
        {
            //ER_BAD_HOST_ERROR
            _exceptionHandlers.Add(1042, null);

            //ER_LOCK_WAIT_TIMEOUT
            _exceptionHandlers.Add(1205, null);

            //ER_LOCK_DEADLOCK
            _exceptionHandlers.Add(1213, null);

            //ER_TIMEOUT
            _exceptionHandlers.Add(0, (ex, context) =>
            {
                context.CommandTimeOut = 0;
            });

            //Прочие ошибки
            _exceptionHandlers.Add(-1, null);
        }
    }

    /// <summary>
    /// Скалярное значение.
    /// </summary>
    /// <typeparam name="T">Возвращаемый тип данных.</typeparam>
    public class ScalarResult<T>
    {
        /// <summary>
        /// Значение DbNull.
        /// </summary>
        public bool DbNull { get; set; }

        /// <summary>
        /// Значение.
        /// </summary>
        public T Value { get; set; }
    }

    /// <summary>
    /// Контекст выполнения запроса.
    /// </summary>
    public class QueryContext
    {
        /// <summary>
        /// SQL SQL запрос.
        /// </summary>
        public string Query { get; set; }

        /// <summary>
        /// Таймаут выполнения SQL запроса.
        /// </summary>
        public int CommandTimeOut { get; set; }

        /// <summary>
        /// Повторить выполнение запроса при возникновении ошибки.
        /// </summary>
        public bool Retry { get; set; }
    }
}
