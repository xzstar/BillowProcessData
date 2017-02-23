using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ProcessData
{

    /// <summary>
    /// MongoDB帮助类
    /// </summary>
    internal static class MongoDbHepler
    {
        static MongoDatabase sDB;
        /// <summary>
        /// 获取数据库实例对象
        /// </summary>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <returns>数据库实例对象</returns>
        private static MongoDatabase GetDatabase(string connectionString, string dbName)
        {
            if (sDB == null)
            {
                MongoClient client = new MongoClient(connectionString);
                sDB = client.GetServer().GetDatabase(dbName);
            }

            return sDB;
        }

        #region 新增

        /// <summary>
        /// 插入一条记录
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="model">数据对象</param>
        public static void Insert<T>(string connectionString, string dbName, string collectionName, T model)
        {
            if (model == null)
            {
                throw new ArgumentNullException("model", "待插入数据不能为空");
            }
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection<T>(collectionName);
            WriteConcernResult result = collection.Insert(model);
        }

        #endregion

        #region 更新

        /// <summary>
        /// 更新数据
        /// </summary>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="query">查询条件</param>
        /// <param name="dictUpdate">更新字段</param>
        public static void Update(string connectionString, string dbName, string collectionName, IMongoQuery query, Dictionary<string, BsonValue> dictUpdate)
        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection(collectionName);
            var update = new UpdateBuilder();
            if (dictUpdate != null && dictUpdate.Count > 0)
            {
                foreach (var item in dictUpdate)
                {
                    update.Set(item.Key, item.Value);
                }
            }
            var d = collection.Update(query, update, UpdateFlags.Multi);
        }

        #endregion

        #region 查询

        /// <summary>
        /// 根据ID获取数据对象
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="id">ID</param>
        /// <returns>数据对象</returns>
        public static T GetById<T>(string connectionString, string dbName, string collectionName, ObjectId id)

        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection<T>(collectionName);
            return collection.FindOneById(id);
        }

        /// <summary>
        /// 根据查询条件获取一条数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="query">查询条件</param>
        /// <returns>数据对象</returns>
        public static T GetOneByCondition<T>(string connectionString, string dbName, string collectionName, IMongoQuery query)

        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection<T>(collectionName);
            return collection.FindOne(query);
        }

        /// <summary>
        /// 根据查询条件获取多条数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="query">查询条件</param>
        /// <returns>数据对象集合</returns>
        public static List<T> GetManyByCondition<T>(string connectionString, string dbName, string collectionName, IMongoQuery query)

        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection<T>(collectionName);
            return collection.Find(query).ToList();
        }

        /// <summary>
        /// 根据集合中的所有数据
        /// </summary>
        /// <typeparam name="T">数据类型</typeparam>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <returns>数据对象集合</returns>
        public static List<T> GetAll<T>(string connectionString, string dbName, string collectionName)

        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection<T>(collectionName);
            return collection.FindAll().ToList();
        }

        #endregion

        #region 删除

        /// <summary>
        /// 删除集合中符合条件的数据
        /// </summary>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        /// <param name="query">查询条件</param>
        public static void DeleteByCondition(string connectionString, string dbName, string collectionName, IMongoQuery query)
        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection(collectionName);
            collection.Remove(query);
        }

        /// <summary>
        /// 删除集合中的所有数据
        /// </summary>
        /// <param name="connectionString">数据库连接串</param>
        /// <param name="dbName">数据库名称</param>
        /// <param name="collectionName">集合名称</param>
        public static void DeleteAll(string connectionString, string dbName, string collectionName)
        {
            var db = GetDatabase(connectionString, dbName);
            var collection = db.GetCollection(collectionName);
            collection.RemoveAll();
        }

        #endregion

    }
    class Program
    {

        [Serializable]
        /*public class UnitData   
        {
            //Todo datetime 加上日期
            public string datetime;
            public double high;
            public double low;
            public double open;
            public double close;

        }*/

        public class UnitData
        {
            public ObjectId Id { get; set; }
            public string datetime { get; set; }
            public double high { get; set; }
            public double low { get; set; }
            public double open { get; set; }
            public double close { get; set; }
            public double avg_480 { get; set; }
        }

        /// <summary>
        /// 数据库连接
        /// </summary>
        private const string connectionString = "mongodb://127.0.0.1:27017";
        /// <summary>
        /// 指定的数据库
        /// </summary>
        private const string dbName = "data_15min";
        /// <summary>
        /// 指定的表
        /// </summary>
        //private const string tbName = "table_text";

        static String instrument = "FG705";
        //static String instrument_1m = "_1m";
        static String instrument_15m = "_15m";
        static LinkedList<UnitData> unitDataList = new LinkedList<UnitData>();
        static String originalPath = System.AppDomain.CurrentDomain.BaseDirectory + "1m\\";
        static String newPath = System.AppDomain.CurrentDomain.BaseDirectory + "15m\\";
        static void Main(string[] args)
        {
            /*Dictionary<string, BsonValue> dict = new Dictionary<string, BsonValue>();
            BsonValue value = 1000;
            dict.Add("open", value);
            IMongoQuery query = Query.EQ("datetime", "2016/10/10 13:30:00");
            MongoDbHepler.Update(connectionString, dbName, instrument + instrument_15m, query,dict);*/
            DirectoryInfo folder = new DirectoryInfo(originalPath);



            foreach (FileInfo file in folder.GetFiles("*.csv"))
            {
                Console.WriteLine(file.FullName);
                instrument = file.Name.Substring(0, file.Name.IndexOf("_"));
                start(file.FullName);
                unitDataList.Clear();

            }

        }

        private static string buildFilePath(string instrument, string time)
        {
            return System.AppDomain.CurrentDomain.BaseDirectory + "data\\" + instrument + time + ".csv";
        }
        private static string buildJsonFilePath(string instrument, string time)
        {
            return newPath + instrument + time + ".json";
        }
        private static bool isNextStartMin(DateTime dt)
        {
            DateTime newDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
            newDt = newDt.AddMinutes(1);
            return isStartMin(newDt);
        }

        private static bool isStartMin(DateTime dt)
        {
            if ((dt.Hour == 9 && dt.Minute == 0)
                || (dt.Hour == 10 && dt.Minute == 30)
                || (dt.Hour == 13 && dt.Minute == 30)
                || (dt.Hour == 21 && dt.Minute == 0))
                return true;
            else if ((dt.Hour == 9 && dt.Minute == 1)
                || (dt.Hour == 10 && dt.Minute == 31)
                || (dt.Hour == 13 && dt.Minute == 31)
                || (dt.Hour == 21 && dt.Minute == 1))
                return false;
            else if (dt.Minute % 15 == 0)
                return true;
            else
                return false;
        }

        //Todo lastMin 加上日期时间，避免涨跌停无数据，需要判断时差超过15分钟也要新bar
        private static bool isNewBar(int lastMin, DateTime dt)
        {
            if (lastMin == -1 || (lastMin != dt.Minute && isStartMin(dt)))
                return true;
            return false;
        }
        public static void start(String filename)
        {
            FileStream fs = new FileStream(filename, System.IO.FileMode.Open, System.IO.FileAccess.Read);

            //StreamReader sr = new StreamReader(fs, Encoding.UTF8);
            StreamReader sr = new StreamReader(fs, Encoding.UTF8);
            //string fileContent = sr.ReadToEnd();
            //encoding = sr.CurrentEncoding;
            //记录每次读取的一行记录
            string strLine = "";
            string strLastLine = "";
            //记录每行记录中的各字段内容
            string[] aryLine = null;

            double open = 0;
            double high = Double.MinValue;
            double low = Double.MaxValue;
            DateTime curBarDateTime = DateTime.Now;
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("日期"));
            dt.Columns.Add(new DataColumn("时间"));
            dt.Columns.Add(new DataColumn("开盘价"));
            dt.Columns.Add(new DataColumn("最高价"));
            dt.Columns.Add(new DataColumn("最低价"));
            dt.Columns.Add(new DataColumn("收盘价"));
            DataRow dr = null;

            double curHigh;
            double curLow;
            double curOpen;
            double curClose = 0;
            double curTime;
            while ((strLine = sr.ReadLine()) != null)
            {

                //strLine = Common.ConvertStringUTF8(strLine, encoding);
                //strLine = Common.ConvertStringUTF8(strLine);

                //Console.WriteLine(strLine);
                {
                    char split = '\t';
                    if (strLine.IndexOf(',') > 0)
                        split = ',';
                    aryLine = strLine.Split(split);
                    bool result = false;

                    result = double.TryParse(aryLine[1], out curTime);
                    if (result == false || curTime < 0)
                        continue;
                    curTime += 0.00001;

                    result = double.TryParse(aryLine[2], out curOpen);
                    if (result == false || curOpen <= 0)
                        continue;

                    result = double.TryParse(aryLine[3], out curHigh);
                    if (result == false || curHigh <= 0)
                        continue;

                    result = double.TryParse(aryLine[4], out curLow);
                    if (result == false || curLow <= 0)
                        continue;

                    result = double.TryParse(aryLine[5], out curClose);
                    if (result == false || curClose <= 0)
                        continue;

                    string date = aryLine[0].Substring(0, 4) + "-" + aryLine[0].Substring(4, 2) + "-" + aryLine[0].Substring(6);


                    int year;
                    int month;
                    int day;

                    result = int.TryParse(aryLine[0].Substring(0, 4), out year);
                    if (result == false)
                        continue;
                    result = int.TryParse(aryLine[0].Substring(4, 2), out month);
                    if (result == false)
                        continue;
                    result = int.TryParse(aryLine[0].Substring(6, 2), out day);
                    if (result == false)
                        continue;


                    int hour = (int)(curTime * 100);
                    int hourmin = (int)(curTime * 10000);
                    int min = hourmin % 100;

                    DateTime datetime = new DateTime(year, month, day, hour, min, 0);

                    if (isStartMin(datetime))
                    {
                        Console.WriteLine("start min " + strLine);

                        dr = dt.NewRow();
                        dr["日期"] = date;
                        dr["时间"] = hour * 100 + min;
                        dr["开盘价"] = curOpen;
                        dr["最高价"] = curHigh;
                        dr["最低价"] = curLow;
                        dr["收盘价"] = curClose;
                        high = curHigh;
                        low = curLow;
                        open = curOpen;
                        curBarDateTime = datetime;

                    }
                    else if (dr == null)
                    {
                        Console.WriteLine("lost time " + strLine);
                        dr = dt.NewRow();
                        dr["日期"] = date;
                        dr["时间"] = hour * 100 + min;
                        dr["开盘价"] = curOpen;
                        dr["最高价"] = curHigh;
                        dr["最低价"] = curLow;
                        dr["收盘价"] = curClose;
                        high = curHigh;
                        low = curLow;
                        open = curOpen;
                    }
                    if (curHigh > high)
                    {
                        high = curHigh;
                    }

                    if (curLow < low)
                    {
                        low = curLow;
                    }

                    if (isNextStartMin(datetime))
                    {
                        dr["最高价"] = high;
                        dr["最低价"] = low;
                        dr["收盘价"] = curClose;
                        dt.Rows.Add(dr);

                        UnitData unitData = new UnitData();
                        unitData.close = curClose;
                        unitData.high = high;
                        unitData.low = low;
                        unitData.open = open;
                        unitData.datetime = curBarDateTime.ToString();

                        unitDataList.AddLast(unitData);
                        //Console.WriteLine(dr);
                        high = Double.MinValue;
                        low = Double.MaxValue;
                        dr = null;
                    }


                }
                strLastLine = strLine;
            }
            if (dr != null)
            {
                dr["最高价"] = high;
                dr["最低价"] = low;
                dr["收盘价"] = curClose;
                dt.Rows.Add(dr);

                UnitData unitData = new UnitData();
                unitData.close = curClose;
                unitData.high = high;
                unitData.low = low;
                unitData.open = open;
                unitData.datetime = curBarDateTime.ToString();

                unitDataList.AddLast(unitData);
                //Console.WriteLine(dr);
                high = Double.MinValue;
                low = Double.MaxValue;
            }
            saveJson();
            //CSVFileHelper.SaveCSV(dt, (buildFilePath(instrument, instrument_15m)));
        }


        private static void saveJson()
        {

            if (unitDataList == null)
                return;
            string fileNameSerialize = buildJsonFilePath(instrument, instrument_15m);
            string jsonString = JsonConvert.SerializeObject(unitDataList);
            File.WriteAllText(fileNameSerialize, jsonString, Encoding.UTF8);
            foreach (UnitData data in unitDataList)
            {
                MongoDbHepler.Insert<UnitData>(connectionString, dbName, instrument + instrument_15m, data);
            }

        }

    }



    public class CSVFileHelper
    {
        /// <summary>
        /// 将DataTable中数据写入到CSV文件中
        /// </summary>
        /// <param name="dt">提供保存数据的DataTable</param>
        /// <param name="fileName">CSV的文件路径</param>
        public static void SaveCSV(DataTable dt, string fullPath)
        {
            FileInfo fi = new FileInfo(fullPath);
            if (!fi.Directory.Exists)
            {
                fi.Directory.Create();
            }
            FileStream fs = new FileStream(fullPath, System.IO.FileMode.Create, System.IO.FileAccess.Write);
            //StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.Default);
            StreamWriter sw = new StreamWriter(fs, System.Text.Encoding.UTF8);
            string data = "";
            //写出列名称
            for (int i = 0; i < dt.Columns.Count; i++)
            {
                data += dt.Columns[i].ColumnName.ToString();
                if (i < dt.Columns.Count - 1)
                {
                    data += ",";
                }
            }
            sw.WriteLine(data);
            //写出各行数据
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                data = "";
                for (int j = 0; j < dt.Columns.Count; j++)
                {
                    string str = dt.Rows[i][j].ToString();
                    str = str.Replace("\"", "\"\"");//替换英文冒号 英文冒号需要换成两个冒号
                    if (str.Contains(',') || str.Contains('"')
                        || str.Contains('\r') || str.Contains('\n')) //含逗号 冒号 换行符的需要放到引号中
                    {
                        str = string.Format("\"{0}\"", str);
                    }

                    data += str;
                    if (j < dt.Columns.Count - 1)
                    {
                        data += ",";
                    }
                }
                sw.WriteLine(data);
            }
            sw.Close();
            fs.Close();
            //DialogResult result = MessageBox.Show("CSV文件保存成功！");
            //if (result == DialogResult.OK)
            //{
            //    System.Diagnostics.Process.Start("explorer.exe", Common.PATH_LANG);
            //}
        }

        /// <summary>
        /// 将CSV文件的数据读取到DataTable中
        /// </summary>
        /// <param name="fileName">CSV文件路径</param>
        /// <returns>返回读取了CSV数据的DataTable</returns>
        public static DataTable OpenCSV(string filePath)
        {
            Encoding encoding = Encoding.ASCII; // Common.GetType(filePath); //Encoding.ASCII;//
            DataTable dt = new DataTable();
            FileStream fs = new FileStream(filePath, System.IO.FileMode.Open, System.IO.FileAccess.Read);

            //StreamReader sr = new StreamReader(fs, Encoding.UTF8);
            StreamReader sr = new StreamReader(fs, encoding);
            //string fileContent = sr.ReadToEnd();
            //encoding = sr.CurrentEncoding;
            //记录每次读取的一行记录
            string strLine = "";
            //记录每行记录中的各字段内容
            string[] aryLine = null;
            string[] tableHead = null;
            //标示列数
            int columnCount = 0;
            //标示是否是读取的第一行
            bool IsFirst = true;
            //逐行读取CSV中的数据
            while ((strLine = sr.ReadLine()) != null)
            {
                //strLine = Common.ConvertStringUTF8(strLine, encoding);
                //strLine = Common.ConvertStringUTF8(strLine);

                if (IsFirst == true)
                {
                    tableHead = strLine.Split(',');
                    IsFirst = false;
                    columnCount = tableHead.Length;
                    //创建列
                    for (int i = 0; i < columnCount; i++)
                    {
                        DataColumn dc = new DataColumn(tableHead[i]);
                        dt.Columns.Add(dc);
                    }
                }
                else
                {
                    aryLine = strLine.Split(',');
                    DataRow dr = dt.NewRow();
                    for (int j = 0; j < columnCount; j++)
                    {
                        dr[j] = aryLine[j];
                    }
                    dt.Rows.Add(dr);
                }
            }
            if (aryLine != null && aryLine.Length > 0)
            {
                dt.DefaultView.Sort = tableHead[0] + " " + "asc";
            }

            sr.Close();
            fs.Close();
            return dt;
        }
    }
}

