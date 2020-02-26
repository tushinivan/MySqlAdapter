using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace ITsoft.Extensions.MySql
{
    /// <summary>
    /// Буфер пакетной вставки запросов.
    /// </summary>
    public class QueryBuffer
    {
        /// <summary>
        /// Количество запросов в очереди.
        /// </summary>
        public int QueueCount { get => counter; }
        public string Query
        {
            get
            {
                StringBuilder result = null;

                if (counter > 0)
                {
                    lock (queryBuilder)
                    {
                        var query = queryBuilder.ToString();
                        if (useTransaction)
                        {
                            query = string.Concat(query, "END TRANSACTION;");
                        }

                        return query;
                    }
                }

                return result?.ToString();
            }
        }

        public delegate void ExecutedArgs(int RowsCount);
        public event ExecutedArgs Executed;

        private readonly MySqlAdapter adapter;

        private StringBuilder queryBuilder = new StringBuilder();

        private int counter = 0;
        private int packageSize = 0;
        private bool useTransaction = false;

        private List<int> operationOffsets = new List<int>();

        private Task syncTask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="packageSize">Размер пакета</param>
        public QueryBuffer(MySqlAdapter adapter, int packageSize, bool useTransaction)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;
            this.useTransaction = useTransaction;

            if (useTransaction)
            {
                StartTransaction();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="syncInterval">Интервал по истечению которого произойдет выполнение буферизированных запросов.</param>
        /// <param name="packageSize">Размер пакета</param>
        /// <param name="useTransaction">Использовать транзакции для вставки буферизированных запросов.</param>
        public QueryBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int packageSize, bool useTransaction)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;
            this.useTransaction = useTransaction;

            if (useTransaction)
            {
                StartTransaction();
            }

            if (syncInterval > TimeSpan.Zero)
            {
                syncTask = Task.Run(() =>
                {
                    while (true)
                    {
                        try
                        {
                            Execute();
                        }
                        catch
                        {
                        }
                        finally
                        {
                            Thread.Sleep(syncInterval);
                        }
                    }
                });
            }
        }

        /// <summary>
        /// Начало транзакцию.
        /// </summary>
        public void StartTransaction()
        {
            queryBuilder.AppendLine("START TRANSACTION;");
        }

        /// <summary>
        /// Конец транзакции.
        /// </summary>
        public void EndTransaction()
        {
            queryBuilder.AppendLine("COMMIT;");
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(string query)
        {
            if (query?.Length > 0)
            {
                lock (queryBuilder)
                {
                    Interlocked.Increment(ref counter);
                    operationOffsets.Add(queryBuilder.Length);

                    var trimQuery = query.Trim('\r', '\n');
                    queryBuilder.Append(trimQuery);

                    if (trimQuery[trimQuery.Length - 1] != ';')
                    {
                        queryBuilder.Append(";");
                    }
                    queryBuilder.AppendLine();

                    int result = 0;
                    if (packageSize > 0 && counter >= packageSize)
                    {
                        result = Execute();
                    }

                    return result;
                }
            }

            return 0;
        }

        /// <summary>
        /// Отмена последних N операций вставки, которые еще не были выполенены.
        /// </summary>
        /// <param name="lastOperationsNumber"></param>
        public void Reject(int lastOperationsNumber)
        {
            lock (queryBuilder)
            {
                var index = operationOffsets.Count - lastOperationsNumber;
                var offset = operationOffsets[index];
                var length = queryBuilder.Length - offset;

                queryBuilder.Remove(offset, length);
                operationOffsets.RemoveRange(index, operationOffsets.Count - index);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="replacement"></param>
        public void Replace(Regex regex, string replacement)
        {
            lock (queryBuilder)
            {
                var tmp = queryBuilder.ToString();
                regex.Replace(tmp, replacement);

                queryBuilder = new StringBuilder(tmp);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="evalutor"></param>
        public void Replace(Regex regex, MatchEvaluator evalutor)
        {
            lock (queryBuilder)
            {
                var tmp = queryBuilder.ToString();
                regex.Replace(tmp, evalutor);

                queryBuilder = new StringBuilder(tmp);
            }
        }

        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public int Execute()
        {
            lock (queryBuilder)
            {
                int result = -1;
                if (counter > 0)
                {
                    if (useTransaction)
                    {
                        EndTransaction();
                    }

                    //вставка
                    result = adapter.Execute(queryBuilder.ToString());

                    queryBuilder.Clear();
                    if (useTransaction)
                    {
                        StartTransaction();
                    }

                    Interlocked.Exchange(ref counter, 0);
                }

                Executed?.Invoke(result);
                return result;
            }
        }
    }
}
