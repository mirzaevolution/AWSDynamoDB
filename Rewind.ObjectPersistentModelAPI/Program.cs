using System;
using System.Collections.Generic;
using Amazon;
using Amazon.DynamoDBv2;
namespace Rewind.ObjectPersistentModelAPI
{
    class Program
    {
        private static readonly IAmazonDynamoDB _amazonDynamoDB;
        static Program()
        {
            _amazonDynamoDB = new AmazonDynamoDBClient(RegionEndpoint.APSoutheast1);
        }
        static void InvokeObjectPersistentLevelOperations()
        {
            ObjectPersistentLevelOperations op = new ObjectPersistentLevelOperations(_amazonDynamoDB);
            //op.ScanAll().GetAwaiter().GetResult();
            //op.ScanByConditions().GetAwaiter().GetResult();
            //op.GetItem("ID", "Mirza Ghulam Rasyid").GetAwaiter().GetResult();
            //op.QueryByCountryId().GetAwaiter().GetResult();
            //op.QueryByConditions().GetAwaiter().GetResult();
            //op.InsertNewItem(new Profile
            //{
            //    Name = "Vanquish Jonathan",
            //    Email = "vanq.jnt@gmail.com",
            //    CountryId = "UK",
            //    Phones = new List<string>
            //    {
            //        "+44891122345"
            //    }
            //}).GetAwaiter().GetResult();

            //op.BatchInserts(new List<Profile>
            //{
            //    new Profile("Rama Vino", "ramavino85@gmail.com","ID", new List<string>{ "+62857890112" }),
            //    new Profile("Sekar Asri", "sekarasri@outlook.co.id","ID", new List<string>{ "+62897221153" })

            //}).GetAwaiter().GetResult();

            //op.UpdateEmail("ID", "Rama Vino", "ramavino.1985@gmail.com").GetAwaiter().GetResult();
            op.UpdateName("ID", "Ayu Sekar Asri", "Vani Sekar Asri").GetAwaiter().GetResult();

        }
        static void Main(string[] args)
        {
            try
            {
                InvokeObjectPersistentLevelOperations();
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                Console.ReadLine();
            }
        }
    }
}
