 using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlServerCe;
using System.Globalization;
using LinqToTwitter;
using Tweetinvi;
using Streaminvi;
using System.Web;
using System.Net;
using System.IO;


using System.Threading;
namespace GradProject
{
    public class DataRetrieve
    {
        static string _accessToken = "";
        static string _accessTokenSecret = "";
        static string _consumerKey = "";
        static string _consumerSecret = "";
        static string api_key = "";
        static string _sqlConnection = "Data Source=C:\\GradProject\\GradProject\\GradProject\\TweetStock.sdf;";
        static int _limit = 2000;

        public static void RunLangDetect(int RequestLimit)
        {
            using (var conn = new SqlCeConnection(_sqlConnection))
            {
                var sql = "SELECT * FROM Tweets WHERE Language is NULL";
                conn.Open();
                using (var reader = new SqlCeCommand(sql, conn).ExecuteReader())
                {
                    var text = new List<Tuple<long, string>>();
                    var batchCount = 0;
                    var requestCount = 0;
                    while (reader.Read())
                    {
                        text.Add(new Tuple<long, string>(long.Parse(reader["TweetId"].ToString()), reader["Text"].ToString()));
                        batchCount++;
                        if (batchCount == _limit)
                        {
                            LangDetectBatch(text, conn);
                            batchCount = 0;
                            text.Clear();
                            requestCount++;
                            if (requestCount == RequestLimit)
                                break;
                        }
                    }
                    if (text.Count() > 0)
                        LangDetectBatch(text, conn);
                }
            }
        }

        public static void LangDetectBatch(List<Tuple<long, string>> batch, SqlCeConnection conn)
        {
            var ids = new List<long>();
            var texts = new List<string>();
            foreach (var tuple in batch)
            {
                ids.Add(tuple.Item1);
                texts.Add(tuple.Item2);
            }
            var langs = GetLanguageMulti(texts);
            var cmd = new SqlCeCommand("UPDATE Tweets SET Language=@Language WHERE TweetId=@TweetId", conn);
            for (int i = 0; i < langs.Count(); i++)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@TweetId", ids[i]);
                cmd.Parameters.AddWithValue("@Language", langs[i]);
                cmd.ExecuteNonQuery();
            }
        }

        public static void RunSentimentAnalysis(int RequestLimit)
        {
            using (var conn = new SqlCeConnection(_sqlConnection))
            {
                var sql = "SELECT * FROM Tweets WHERE Language = 'English' AND Positive IS NULL";
                conn.Open();
                using (var reader = new SqlCeCommand(sql, conn).ExecuteReader())
                {
                    var text = new List<Tuple<long, string>>();
                    var batchCount = 0;
                    var requestCount = 0;
                    while (reader.Read())
                    {
                        text.Add(new Tuple<long, string>(long.Parse(reader["TweetId"].ToString()), reader["Text"].ToString()));
                        batchCount++;
                        if (batchCount == _limit)
                        {
                            SentimentAnalysisBatch(text, conn);
                            batchCount = 0;
                            text.Clear();
                            requestCount++;
                            if (requestCount == RequestLimit)
                                break;
                        }
                    }
                    if (text.Count() > 0)
                        SentimentAnalysisBatch(text, conn);
                }
            }
        }

        private static void SentimentAnalysisBatch(List<Tuple<long, string>> batch, SqlCeConnection conn)
        {
            var ids = new List<long>();
            var texts = new List<string>();
            foreach (var tuple in batch)
            {
                ids.Add(tuple.Item1);
                texts.Add(tuple.Item2);
            }
            var pos = GetPositiveSentimentMulti(texts);
            var cmd = new SqlCeCommand("UPDATE Tweets SET Positive=@Positive WHERE TweetId=@TweetId", conn);
            for (int i = 0; i < pos.Count(); i++)
            {
                cmd.Parameters.Clear();
                cmd.Parameters.AddWithValue("@TweetId", ids[i]);
                cmd.Parameters.AddWithValue("@Positive", pos[i]);
                cmd.ExecuteNonQuery();
            }
        }
        
        public static double GetPositiveSentiment(string text)
        {
            var api_key = "8dgtSKjkQVuPuVzUiT2E9nfVs8";
            var textBytes = Encoding.UTF8.GetBytes(text);
            var text64 = Convert.ToBase64String(textBytes);

            var post = WebRequest.Create("http://api.uclassify.com");
            post.Method = "POST";
            post.ContentType = "text/xml";
            using (var postStream = post.GetRequestStream())
            {
                var xml = @"<?xml version='1.0' encoding='utf-8' ?>
                                <uclassify xmlns='http://api.uclassify.com/1/RequestSchema' version='1.01'>
                                  <texts>
                                    <textBase64 id='TextId'>{0}</textBase64>
                                  </texts>
                                  <readCalls readApiKey='{1}'>
                                    <classify id='Classify' username='uClassify' classifierName='Sentiment' textId='TextId'/>
                                  </readCalls>
                                </uclassify>";
                var bytes = Encoding.UTF8.GetBytes(string.Format(xml, text64, api_key));
                postStream.Write(bytes, 0, bytes.Length);
            }
            var response = post.GetResponse();
            var responseText = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                responseText = reader.ReadToEnd();
            }

            var responseXml = XDocument.Parse(responseText);
            var posStr = (from c in responseXml.Root.Descendants()
                          where c.Name.LocalName == "class" &&
                          c.Attribute("className").Value == "positive"
                          select c.Attribute("p").Value).FirstOrDefault();
            var negStr = (from c in responseXml.Root.Descendants()
                          where c.Name.LocalName == "class" &&
                          c.Attribute("className").Value == "negative"
                          select c.Attribute("p").Value).FirstOrDefault();
            double positive, negative;
            double.TryParse(posStr, out positive);
            double.TryParse(negStr, out negative);

            return positive;
        }

        private static List<decimal> GetPositiveSentimentMulti(List<string> texts)
        {
            var result = new List<decimal>();
            var post = WebRequest.Create("http://api.uclassify.com");
            post.Method = "POST";
            post.ContentType = "text/xml";
            var xml = new StringBuilder(@"<?xml version='1.0' encoding='utf-8' ?>
                                <uclassify xmlns='http://api.uclassify.com/1/RequestSchema' version='1.01'>
                                  <texts>");
            using (var postStream = post.GetRequestStream())
            {
                int textId = 0;
                foreach (var text in texts)
                {
                    var textBytes = Encoding.UTF8.GetBytes(text);
                    var text64 = Convert.ToBase64String(textBytes);
                    xml.Append(string.Format("<textBase64 id='{0:0000}'>{1}</textBase64>", textId, text64));
                    textId++;
                }
                xml.Append(string.Format(@"</texts>
                                  <readCalls readApiKey='{0}'>", api_key));

                for (int i = 0; i < textId; i++)
                {
                    xml.Append(string.Format(@"<classify id='Classify{0:0000}' username='uClassify' classifierName='Sentiment' textId='{0:0000}'/>", i));
                }
                xml.Append(@"</readCalls>
                                </uclassify>");
                var bytes = Encoding.UTF8.GetBytes(xml.ToString());
                postStream.Write(bytes, 0, bytes.Length);
            }

            var response = post.GetResponse();
            var responseText = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                responseText = reader.ReadToEnd();
            }
            responseText = responseText.Replace("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>", "")
                .Replace(" xmlns=\"http://api.uclassify.com/1/ResponseSchema\" version=\"1.01\"", "");
            var responseXml = XDocument.Parse(responseText);

            var posStr = from cls in
                             (from classify in responseXml.Root.Descendants("classify")
                              orderby classify.Attribute("id").Value
                              select classify).Descendants("class")
                         where cls.Attribute("className").Value == "positive"
                         select cls.Attribute("p").Value;
            foreach (var s in posStr)
            {
                decimal pos = 0;
                decimal.TryParse(s, out pos);
                result.Add(pos);
            }
            return result;
        }

        private static List<string> GetLanguageMulti(List<string> texts)
        {
            var result = new List<string>();
            var post = WebRequest.Create("http://api.uclassify.com");
            post.Method = "POST";
            post.ContentType = "text/xml";
            var xml = new StringBuilder(@"<?xml version='1.0' encoding='utf-8' ?>
                                <uclassify xmlns='http://api.uclassify.com/1/RequestSchema' version='1.01'>
                                  <texts>");
            using (var postStream = post.GetRequestStream())
            {
                int textId = 0;
                foreach (var text in texts)
                {
                    var textBytes = Encoding.UTF8.GetBytes(text);
                    var text64 = Convert.ToBase64String(textBytes);
                    xml.Append(string.Format("<textBase64 id='{0:0000}'>{1}</textBase64>", textId, text64));
                    textId++;
                }
                xml.Append(string.Format(@"</texts>
                                  <readCalls readApiKey='{0}'>", api_key));

                for (int i = 0; i < textId; i++)
                {
                    xml.Append(string.Format(@"<classify id='Classify{0:0000}' username='uClassify' classifierName='Text Language' textId='{0:0000}'/>", i));
                }
                xml.Append(@"</readCalls>
                                </uclassify>");
                var bytes = Encoding.UTF8.GetBytes(xml.ToString());
                postStream.Write(bytes, 0, bytes.Length);
            }

            var response = post.GetResponse();
            var responseText = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                var outFlag = false;
                var timeoutCount = 0;
                while (!outFlag)
                {
                    try
                    {
                        responseText = reader.ReadToEnd();
                    }
                    catch (Exception ex)
                    {
                        timeoutCount++;
                        if (timeoutCount >= 5)
                            outFlag = true;
                    }
                }
            }
            responseText = responseText.Replace("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>", "")
                .Replace(" xmlns=\"http://api.uclassify.com/1/ResponseSchema\" version=\"1.01\"", "");
            var responseXml = XDocument.Parse(responseText);

            var classifies = from classify in responseXml.Root.Descendants("classify")
                             orderby classify.Attribute("id").Value
                             select classify;

            foreach (var cls in classifies)
            {
                var lang = (from c in cls.Descendants("class")
                            orderby double.Parse(c.Attribute("p").Value) descending
                            select c.Attribute("className").Value).FirstOrDefault();
                result.Add(lang);
            }
            return result;
        }

        private static void GetLanguage(string text)
        {
            var api_key = "8dgtSKjkQVuPuVzUiT2E9nfVs8";
            var textBytes = Encoding.UTF8.GetBytes(text);
            var text64 = Convert.ToBase64String(textBytes);

            var post = WebRequest.Create("http://api.uclassify.com");
            post.Method = "POST";
            post.ContentType = "text/xml";
            using (var postStream = post.GetRequestStream())
            {
                var xml = @"<?xml version='1.0' encoding='utf-8' ?>
                                <uclassify xmlns='http://api.uclassify.com/1/RequestSchema' version='1.01'>
                                  <texts>
                                    <textBase64 id='TextId'>{0}</textBase64>
                                  </texts>
                                  <readCalls readApiKey='{1}'>
                                    <classify id='Classify' username='uClassify' classifierName='Text Language' textId='TextId'/>
                                  </readCalls>
                                </uclassify>";
                var bytes = Encoding.UTF8.GetBytes(string.Format(xml, text64, api_key));
                postStream.Write(bytes, 0, bytes.Length);
            }
            var response = post.GetResponse();
            var responseText = "";
            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                responseText = reader.ReadToEnd();
            }
            //Console.WriteLine(responseText);
            var responseXml = XDocument.Parse(responseText);
            var lang = (from c in responseXml.Root.Descendants()
                        where c.Name.LocalName == "class"
                        orderby double.Parse(c.Attribute("p").Value) descending
                        select c.Attribute("className").Value).FirstOrDefault();

            //Console.WriteLine(lang);
        }

        public static List<Tweet> SearchTwitter(string SearchString, DateTime? date = null)
        {
            if (date == null) date = DateTime.Today;

            var singleUserAuth = new SingleUserAuthorizer
            {
                Credentials = new SingleUserInMemoryCredentials
                {
                    ConsumerKey = _consumerKey,
                    ConsumerSecret = _consumerSecret,
                    TwitterAccessToken = _accessToken,
                    TwitterAccessTokenSecret = _accessTokenSecret
                }
            };

            var twitterCtx = new LinqToTwitter.TwitterContext(singleUserAuth);
            var results = from search in twitterCtx.Search
                          where search.Query == SearchString &&
                                search.Type == SearchType.Search &&
                                search.ResultType == ResultType.Recent &&
                                search.SearchLanguage == "en" &&
                                search.Until == date.GetValueOrDefault().AddDays(1) &&                                
                                search.Count == 100
                          select search;

            Search srch = results.Single();

            var result = new List<Tweet>();
            //Console.WriteLine(srch.Statuses.Count);
            srch.Statuses
                .ForEach(entry =>
                                    result.Add(new Tweet
                                    {
                                        Text = entry.Text,
                                        FollowersCount = entry.User.FollowersCount,
                                        CreatedAt = entry.CreatedAt,
                                        FavoriteCount = entry.Favorited ? (int)entry.FavoriteCount : 0,
                                        RetweetCount = entry.Retweeted ? (int)entry.RetweetCount : 0,
                                        Positive = GetPositiveSentiment(entry.Text)
                                    })
                                );
            return result;
        }

        public static void SearchTwitterStreaming()
        {
            var token = new TwitterToken.Token(_accessToken, _accessTokenSecret, _consumerKey, _consumerSecret);

            // Create the stream  
            var myStream = new FilteredStream();
            myStream.AddTrack("#apple");
            myStream.AddTrack("#google");
            myStream.AddTrack("#twitter");
            myStream.AddTrack("#tesla");
            myStream.AddTrack("#ford");
            myStream.AddTrack("#shell");
            myStream.AddTrack("#bks");


            // Starting the stream by specifying credentials thanks to the Token and a delegate specifying what you want to do when you receive a tweet 
            using (var conn = new SqlCeConnection(_sqlConnection))
            {
                conn.Open();
                var insert = new SqlCeCommand("INSERT INTO Tweets(Text, UserFollowers, RetweetCount, CreatedAt) Values(@Text, @UserFollowers, @RetweetCount, @CreatedAt)", conn);
                try
                {
                    myStream.StartStream(token, tweet =>
                    {

                        insert.Parameters.Clear();
                        insert.Parameters.AddWithValue("@Text", tweet.Text);
                        insert.Parameters.AddWithValue("@UserFollowers", tweet.Creator.FollowersCount);
                        insert.Parameters.AddWithValue("@RetweetCount", tweet.RetweetCount);
                        insert.Parameters.AddWithValue("@CreatedAt", tweet.CreatedAt);
                        var status = insert.ExecuteNonQuery();
                        //Console.WriteLine("{0}: {1}", status, tweet.Text);
                    });
                }
                catch
                {
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        public static StockPrice GetStockQuote(string StockCode, DateTime date)
        {
            var post = WebRequest.Create("http://ichart.yahoo.com/table.csv?s=" + StockCode);
            post.Method = "GET";

            var response = post.GetResponse();

            DateTimeFormatInfo dtfi = new DateTimeFormatInfo();
            dtfi.ShortDatePattern = "yyyy-MM-dd";
            dtfi.DateSeparator = "-";

            var result = new StockPrice();

            using (var reader = new StreamReader(response.GetResponseStream()))
            {
                while (reader.Peek() > -1)
                {
                    var line = reader.ReadLine();
                    var cols = line.Split(',');
                    if (cols.Count() < 7)
                        break;
                    if (cols[0].ToLowerInvariant() != "date")
                    {
                        var quoteDate = Convert.ToDateTime(cols[0], dtfi);
                        if (quoteDate.Date == date.Date)
                        {
                            result.Open = Convert.ToDouble(cols[1]);
                            result.Close = Convert.ToDouble(cols[2]);
                            result.High = Convert.ToDouble(cols[3]);
                            result.Low = Convert.ToDouble(cols[4]);
                            result.Volume = Convert.ToInt32(cols[5]);
                            result.AdjClose = Convert.ToDouble(cols[6]);
                        }
                    }
                }
            }
            return result;
        }

    }

    public class Tweet
    {
        public string Text { get; set; }
        public DateTime CreatedAt { get; set; }
        public int FollowersCount { get; set; }
        public int FavoriteCount { get; set; }
        public int RetweetCount { get; set; }
        public double Positive { get; set; }
        public double Negative
        {
            get
            {
                return 1 - Positive;
            }
        }
    }

    public class StockPrice
    {
        public double Open { get; set; }
        public double Close { get; set; }
        public double High { get; set; }
        public double Low { get; set; }
        public double AdjClose { get; set; }
        public int Volume { get; set; }
    }
}
