using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Extensions.MySql
{
    class MySqlAdapter
    {
        /// <summary>
        /// Выполнять запрос выдавший ошибку, через интервал LoopTimeOut.
        /// </summary>
        public bool LoopQuery = true;

        /// <summary>
        /// Время между повторами запросов в мс. По умолчанию 10000 мс. (10 сек)
        /// </summary>
        public int LoopTimeOut = 10000;

        /// <summary>
        /// Время ожидания выполнения запроса. По умолчанию 300 сек.
        /// </summary>
        public int TimeOut = 300;

        /// <summary>
        /// Максимальное время выполнения запроса. По умолчанию 3600 сек.
        /// </summary>
        public int MaximumTimeOut = 3600;

        private int runningQueries;
        private readonly string connectionString;

        public MySqlAdapter(string connectionString)
        {
            if (connectionString != null)
            {
                this.connectionString = connectionString;
            }
            else
            {
                throw new Exception("Не задана строка подключения.");
            }
        }

        
        public int Execute(string query, int timeOut)
        {
            int result = -1;

            result = _Execute(query, true, timeOut, (command) => 
            {
                return command.ExecuteNonQuery();
            });

            return result;
        }

        private T _Execute<T>(string query, bool loopQuery, int timeOut, Func<MySqlCommand, T> func)
        {
            Interlocked.Increment(ref runningQueries);
            int _timeOut = timeOut >= 0 ? timeOut : TimeOut;

            T result = default(T);
            do
            {
                bool hasError = false;
                try
                {
                    using (MySqlConnection connection = new MySqlConnection(connectionString))
                    {
                        connection.Open();
                        using (MySqlCommand command = new MySqlCommand(query, connection))
                        {
                            //Если значение таймаута больше или равно нулю - то это значение берем из текущего вызова функции
                            command.CommandTimeout = _timeOut;

                            //Если есть параметры то используем их
                            //if (parameters != null)
                            //{
                            //    foreach (var item in parameters)
                            //    {
                            //        command.Parameters.Add(item);
                            //    }
                            //}


                            result = func(command);
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    hasError = true;
                }

                if (hasError && loopQuery)
                {
                    Thread.Sleep(LoopTimeOut);//делаем паузу в запросах
                }
                else
                {
                    break;//выходим из цикла обработки ошибок
                }
            } while (loopQuery);

            Interlocked.Decrement(ref runningQueries);
            return result;
        }
    }
}
