using System;
using System.Text;
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
                        return queryBuilder.ToString();
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

        private Task syncTask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="packageSize">Размер пакета</param>
        /// <param name="table">Таблица для вставки</param>
        /// <param name="insertIgnore">Вставка с игнорированием</param>
        /// <param name="columns">Столбцы</param>
        public QueryBuffer(MySqlAdapter adapter, int packageSize)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;
        }
        public QueryBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int packageSize)
        {
            this.packageSize = packageSize;
            this.adapter = adapter;

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
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(string query)
        {
            lock (queryBuilder)
            {
                Interlocked.Increment(ref counter);

                queryBuilder.Append(query);
                if (!query.EndsWith(";"))
                {
                    queryBuilder.AppendLine(";");
                }
                else
                {
                    queryBuilder.AppendLine();
                }

                int result = 0;
                if (packageSize > 0 && counter >= packageSize)
                {
                    result = Execute();
                }

                return result;
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
                    //вставка
                    result = adapter.Execute(queryBuilder.ToString());

                    queryBuilder.Clear();
                    Interlocked.Exchange(ref counter, 0);
                }

                Executed?.Invoke(result);
                return result;
            }
        }
    }
}
