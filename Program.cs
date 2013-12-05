using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using System.Net;
using System.IO;
using System.Xml;
using System.Xml.Linq;


namespace GradProject
{
    class Program
    {

        static void Main(string[] args)
        {
            //var stock = "TSLA";
            //var searchQuery = "tesla OR TSLA";
            //var searchDate = DateTime.Today.AddDays(-6);
            //var tweets = SearchTwitter(searchQuery, searchDate);
            //var quote = GetStockQuote(stock, searchDate);
            //foreach (var tweet in tweets)
            //{
            //    Console.WriteLine("{0} R:{1} F:{2} P:{3} UF:{4}", tweet.CreatedAt, tweet.RetweetCount, tweet.FavoriteCount, tweet.Positive, tweet.FollowersCount);
            //}
            //Console.WriteLine("{0}: {1} - {2}", stock, quote.Open, quote.Close);
            //Console.WriteLine(SearchTwitter("google").First().Text);
            //while (true)
            //{
            //    SearchTwitterStreaming();
            //    Thread.Sleep(30000);
            //}
            //var x = GetPositiveSentimentMulti(new List<string> { "I am happy to see you", 
            //    "That makes feel very sad", 
            //    "Thank you very much" });
            //RunLangDetect(496);
            //RunSentimentAnalysis(496);
            var fromDate = new DateTime(2013, 11, 22, 5, 0, 0);
            Console.WriteLine("Predicting price of stock AAPL using hashtag #apple:");
            Console.WriteLine("Expected price: {0}", Analyze.PredictStockPrice("#apple", "AAPL", fromDate, DateTime.Now));

        }

    }


}
