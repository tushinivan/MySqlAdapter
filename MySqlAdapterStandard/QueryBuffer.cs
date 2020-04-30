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
        public int QueueCount { get => _counter; }

        /// <summary>
        /// SQL запрос.
        /// </summary>
        public string Query
        {
            get
            {
                StringBuilder result = null;

                if (_counter > 0)
                {
                    lock (_queryBuilder)
                    {
                        var query = _queryBuilder.ToString();
                        if (_useTransaction)
                        {
                            query = string.Concat(query, "COMMIT;");
                        }

                        return query;
                    }
                }

                return result?.ToString();
            }
        }

        public delegate void ExecutedArgs(int RowsCount);
        public event ExecutedArgs Executed;

        private readonly MySqlAdapter _adapter;
        private StringBuilder _queryBuilder = new StringBuilder();
        private int _counter = 0;
        private int _packageSize = 0;
        private bool _useTransaction = false;
        private List<int> _operationOffsets = new List<int>();
        private Task _syncTask;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="adapter">MySqlAdapter адаптер</param>
        /// <param name="packageSize">Размер пакета</param>
        public QueryBuffer(MySqlAdapter adapter, int packageSize, bool useTransaction = false)
        {
            this._packageSize = packageSize;
            this._adapter = adapter;
            this._useTransaction = useTransaction;

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
        public QueryBuffer(MySqlAdapter adapter, TimeSpan syncInterval, int packageSize, bool useTransaction = false)
        {
            this._packageSize = packageSize;
            this._adapter = adapter;
            this._useTransaction = useTransaction;

            if (useTransaction)
            {
                StartTransaction();
            }

            if (syncInterval > TimeSpan.Zero)
            {
                _syncTask = Task.Run(() =>
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
            _queryBuilder.AppendLine("START TRANSACTION;");
        }

        /// <summary>
        /// Конец транзакции.
        /// </summary>
        public void EndTransaction()
        {
            _queryBuilder.AppendLine("COMMIT;");
        }

        /// <summary>
        /// Добавить данные к запросу.
        /// </summary>
        /// <param name="values">Значения, через запятую.</param>
        public int Add(string query)
        {
            if (query?.Length > 0)
            {
                lock (_queryBuilder)
                {
                    Interlocked.Increment(ref _counter);
                    _operationOffsets.Add(_queryBuilder.Length);

                    var trimQuery = query.Trim('\r', '\n');
                    _queryBuilder.Append(trimQuery);

                    if (trimQuery[trimQuery.Length - 1] != ';')
                    {
                        _queryBuilder.Append(";");
                    }
                    _queryBuilder.AppendLine();

                    int result = 0;
                    if (_packageSize > 0 && _counter >= _packageSize)
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
            lock (_queryBuilder)
            {
                var index = _operationOffsets.Count - lastOperationsNumber;
                var offset = _operationOffsets[index];
                var length = _queryBuilder.Length - offset;

                _queryBuilder.Remove(offset, length);
                _operationOffsets.RemoveRange(index, _operationOffsets.Count - index);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="replacement"></param>
        public void Replace(Regex regex, string replacement)
        {
            lock (_queryBuilder)
            {
                var tmp = _queryBuilder.ToString();
                regex.Replace(tmp, replacement);

                _queryBuilder = new StringBuilder(tmp);
            }
        }

        /// <summary>
        /// Заменить значение.
        /// </summary>
        /// <param name="regex"></param>
        /// <param name="evalutor"></param>
        public void Replace(Regex regex, MatchEvaluator evalutor)
        {
            lock (_queryBuilder)
            {
                var tmp = _queryBuilder.ToString();
                regex.Replace(tmp, evalutor);

                _queryBuilder = new StringBuilder(tmp);
            }
        }

        /// <summary>
        /// Принудительно выполнить запрос и очистить очередь.
        /// </summary>
        public int Execute()
        {
            lock (_queryBuilder)
            {
                int result = -1;
                if (_counter > 0)
                {
                    if (_useTransaction)
                    {
                        EndTransaction();
                    }

                    //вставка
                    result = _adapter.Execute(_queryBuilder.ToString());

                    _queryBuilder.Clear();
                    if (_useTransaction)
                    {
                        StartTransaction();
                    }

                    Interlocked.Exchange(ref _counter, 0);
                }

                Executed?.Invoke(result);
                return result;
            }
        }
    }
}
