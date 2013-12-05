using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlServerCe;
using System.IO;

namespace GradProject
{
    public class Analyze
    {
        static string _sqlConnection = "Data Source=C:\\GradProject\\GradProject\\GradProject\\TweetStock.sdf;";
        static int _dayThreshold = 3;

        public static double PredictStockPrice(string SearchTerm, string StockCode, DateTime FromDate, DateTime ToDate)
        {
            Console.WriteLine("Retrieving historic data ({0} days available)", ToDate.Subtract(FromDate).Days);

            var samples = RetrieveEnties(SearchTerm, StockCode, FromDate, ToDate);

            Console.WriteLine("Calculating multiple regression");

            int nAttributes = 3;
            int nSamples = samples.Count;
            double[,] tsData = new double[nSamples, nAttributes];
            double[] resultData = new double[nSamples];

            for (int i = 0; i < samples.Count; i++)
            {
                tsData[i, 0] = samples[i].FollowerCount;
                tsData[i, 1] = samples[i].MaxTweetDensity;
                tsData[i, 2] = samples[i].PositiveSentiment;

                resultData[i] = samples[i].ClosingPrice;
            }

            double[] weights = null;
            int fitResult = 0;
            alglib.lsfit.lsfitreport rep = new alglib.lsfit.lsfitreport();
            alglib.lsfit.lsfitlinear(resultData, tsData, nSamples, nAttributes, ref fitResult, ref weights, rep);


            Console.WriteLine("Retrieving latest tweets");
            var tweets = DataRetrieve.SearchTwitter(SearchTerm);
            var dayData = from t in tweets
                          where t.CreatedAt.Date > DateTime.Today.AddDays(-_dayThreshold)
                                      && t.CreatedAt.Date <= DateTime.Today
                          select t;

            var followerCount = (from row in dayData
                                 select row.FollowersCount + 1)
                                   .Sum();
            var positive = (from row in dayData
                            select row.Positive)
                           .Sum() / dayData.Count();
            var maxDensity = 0d;
            var time = DateTime.Now.AddHours(7);
            for (int i = 0; i <= 24 * _dayThreshold; i++)
            {
                var select = (from row in dayData
                              where row.CreatedAt >= time.AddDays(-_dayThreshold).AddHours(i)
                              && row.CreatedAt < time.AddDays(-_dayThreshold).AddHours(i + 1)
                              select row.Positive);
                var density = select.Sum();
                maxDensity = Math.Max(maxDensity, density);
            }

            
            var predictedPrice = weights[0] * followerCount +
                weights[1] * maxDensity +
                weights[2] * positive;

            return predictedPrice;
        }

        public static List<DayEntry> RetrieveEnties(string SearchTerm, string StockCode, DateTime FromDate, DateTime? ToDate = null)
        {
            var result = new List<DayEntry>();

            if (ToDate == null)
                ToDate = DateTime.Now;

            using (var conn = new SqlCeConnection(_sqlConnection))
            {
                var comm = new SqlCeCommand(@"SELECT * FROM Tweets 
                                                WHERE Text LIKE @Search 
                                                AND CreatedAt >= @FromDate
                                                AND CreatedAt <= @ToDate
                                                AND Positive IS NOT NULL
                                                AND Language = 'English'", conn);
                comm.Parameters.AddWithValue("@Search", "%" + SearchTerm + "%");
                comm.Parameters.AddWithValue("@FromDate", FromDate.AddDays(-_dayThreshold));
                comm.Parameters.AddWithValue("@ToDate", ToDate);
                var adpt = new SqlCeDataAdapter(comm);
                var data = new DataTable();
                adpt.Fill(data);

                var currentDate = FromDate;
                while (currentDate <= ToDate)
                {
                    var closing = DataRetrieve.GetStockQuote(StockCode, currentDate).Close;
                    if (closing == 0)
                    {
                        currentDate = currentDate.AddDays(1);
                        continue;
                    }
                    var dayData = (from row in data.AsEnumerable()
                                   where row.Field<DateTime>("CreatedAt") > currentDate.AddDays(-_dayThreshold)
                                      && row.Field<DateTime>("CreatedAt") <= currentDate
                                   select row);
                    var followerCount = (from row in dayData
                                         select row.Field<int>("UserFollowers") + 1)
                                   .Sum();
                    var positive = (from row in dayData
                                    select row.Field<double>("Positive"))
                                   .Sum() / dayData.Count();
                    var maxDensity = 0d;
                    for (int i = 0; i <= 24 * _dayThreshold; i++)
                    {
                        var density = (from row in dayData
                                       where row.Field<DateTime>("CreatedAt") >= currentDate.AddDays(-_dayThreshold).AddHours(i)
                                       && row.Field<DateTime>("CreatedAt") < currentDate.AddDays(-_dayThreshold).AddHours(i + 1)
                                       select row.Field<double>("Positive"))
                                       .Sum();
                        maxDensity = Math.Max(maxDensity, density);
                    }
                    var entry = new DayEntry
                    {
                        Date = currentDate.Date,
                        ClosingPrice = closing,
                        FollowerCount = followerCount,
                        MaxTweetDensity = maxDensity,
                        PositiveSentiment = positive
                    };
                    result.Add(entry);
                    currentDate = currentDate.AddDays(1);
                }

            }

            return result;
        }

        public class DayEntry
        {
            public DateTime Date { get; set; }
            public double PositiveSentiment { get; set; }
            public double ClosingPrice { get; set; }
            public int FollowerCount { get; set; }
            public double MaxTweetDensity { get; set; }
        }
    }
}
