// <copyright file="MySqlAdapterTest.cs">Copyright ©  2019</copyright>
using System;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ITsoft.Extensions.MySql.Tests
{
    /// <summary>Этот класс содержит параметризованные модульные тесты для MySqlAdapter</summary>
    [TestClass]
    public partial class MySqlAdapterTest
    {
        MySqlAdapter adapter;
        string testrResult = null;

        [TestInitialize]
        public void Initialize()
        {
            adapter = new MySqlAdapter("User Id=test; Password=test; Host=localhost;Character Set=utf8;");
            adapter.Error += Adapter_Error;
            adapter.ErrorProcessed += Adapter_ErrorProcessed;
        }

        private void Adapter_ErrorProcessed(Exception ex, QueryContext queryContext)
        {
            
        }

        private void Adapter_Error(Exception ex, QueryContext queryContext)
        {
            testrResult = queryContext.Query;
        }


        [TestMethod]
        public void CreateAdapterFromFileTest()
        {
            if (!File.Exists("connections.config"))
            {
                //write config file format "connection_name":"connection string"
                File.WriteAllText("connections.config", "\"test_connection\":\"User Id = test; Password = test; Host = localhost; Character Set = utf8;\"");
            }

            try
            {
                adapter = new MySqlAdapter("test_connection", "connections.config");
            }
            catch (Exception)
            {
                Assert.Fail();
            }
        }

        [TestMethod]
        public void SelectDataTableTest()
        {
            var dt = adapter.Select("SELECT * FROM test.test_table WHERE id = 1");

            Assert.IsNotNull(dt);
            Assert.AreEqual(dt.Rows.Count, 1);
        }

        [TestMethod]
        public void SelectReaderTest()
        {
            adapter.SelectReader("SELECT * FROM test.test_table WHERE id = 1", (row) => 
            {
                int id = Convert.ToInt32(row["id"]);
                Assert.AreEqual(id, 1);
            });
        }

        [TestMethod]
        public void SelectRowTest()
        {
            var dt = adapter.SelectRow("SELECT * FROM test.test_table WHERE id = 1");

            Assert.IsNotNull(dt);
        }

        [TestMethod]
        public void SelectScalarTest()
        {
            var result = adapter.SelectScalar<string>("SELECT value FROM test.test_table WHERE id = 1");

            Assert.IsNotNull(result);
            Assert.IsFalse(result.DbNull);
            Assert.AreEqual(result.Value, "test");
        }

        [TestMethod]
        public void SelectDataSetTest()
        {
            var dt = adapter.SelectDataSet("SELECT * FROM test.test_table WHERE id = 1");

            Assert.IsNotNull(dt);
            Assert.AreEqual(dt.Tables.Count, 1);
            Assert.AreEqual(dt.Tables[0].Rows.Count, 1);
        }

        [TestMethod]
        public void ExecuteWithSyntaxError()
        {
            var dt = adapter.SelectRow("syntax error");
            Assert.IsNull(dt);
            Assert.AreEqual(testrResult, "syntax error");
        }

        [TestMethod]
        public void GetServerDateTimeTest()
        {
            var time = adapter.GetServerDateTime();
        }
    }
}
