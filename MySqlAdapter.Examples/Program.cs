﻿
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

            //adapter.SelectReader("SELECT * FROM test.test_table WHERE id = 1", (row) =>
            //{
            //    int id = Convert.ToInt32(row["id"]);
            //});

            QueryBuffer buffer = new QueryBuffer(adapter, 0, true);
            buffer.Add("query one");
            buffer.Add("query two;");
            buffer.Add("query three;\r\n");
            buffer.Add("query four\r\n");
            buffer.Reject(1);
            buffer.Reject(2);

            using (QueryBuffer buf = new QueryBuffer(adapter, new TimeSpan(0,1,0), 0))
            {
                
            }

            var t = buffer.Query;
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
