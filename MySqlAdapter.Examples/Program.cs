
using ITsoft.Extensions.MySql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MySqlAdapterExaples
{
    class Program
    {
        static MySqlAdapter adapter = new MySqlAdapter("User Id=test; Password=test; Host=localhost;Character Set=utf8;");
        static void Main(string[] args)
        {
            adapter.Error += Adapter_Error1;
            adapter.ErrorProcessed += Adapter_ErrorProcessed;

            adapter.SelectReader("SELECT * FROM test.test_table WHERE id = 1", (row) =>
            {
                int id = Convert.ToInt32(row["id"]);
            });
        }

        private static void Adapter_ErrorProcessed(Exception ex, QueryContext queryContext)
        {
            Console.WriteLine(ex.Message);
        }

        private static void Adapter_Error1(Exception ex, QueryContext queryContext)
        {
            
        }
    }
}
