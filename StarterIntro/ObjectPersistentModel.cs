using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace StarterIntro
{
    public class ObjectPersistentModel
    {
        private readonly DynamoDBContext _context;
        public ObjectPersistentModel(DynamoDBContext context)
        {
            _context = context;
        }
       
        public async Task ScanItems()
        {
            try
            {

                #region All items
                var result = await _context.ScanAsync<Profile>(null).GetRemainingAsync();
                Console.WriteLine("#All items:");
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion

                #region People lived in ID
                Console.WriteLine("\n#People lived in ID: ");
                result = await _context.ScanAsync<Profile>(new List<ScanCondition>
                {
                    new ScanCondition(nameof(Profile.CountryId),ScanOperator.Equal,"ID")
                }).GetRemainingAsync();
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion


                #region People which the names start with 'Mi'
                Console.WriteLine("\n#People which the names start with 'Mi': ");
                result = await _context.ScanAsync<Profile>(new List<ScanCondition>
                {
                    new ScanCondition(nameof(Profile.Name),ScanOperator.BeginsWith,"Mi")
                }).GetRemainingAsync();
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion



                #region People which the names start with 'Mi' and CountryId is 'ID'
                Console.WriteLine("\n#People which the names start with 'Mi' and CountryId is 'ID': ");
                result = await _context.ScanAsync<Profile>(new List<ScanCondition>
                {
                    new ScanCondition(nameof(Profile.Name),ScanOperator.BeginsWith,"Mi"),
                    new ScanCondition(nameof(Profile.CountryId), ScanOperator.Equal, "ID")
                }).GetRemainingAsync();
                foreach (var item in result)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public async Task QueryItem()
        {
         
            #region Query based on partition key: 'ID' in asc order
            List<Profile> profiles = await _context.QueryAsync<Profile>("ID").GetRemainingAsync();
            Console.WriteLine("\nQuery based on partition key: 'ID' in asc order");
            foreach (var item in profiles)
            {
                Console.WriteLine(item.ToString());
            }
            #endregion

            #region Query based on partition key: 'ID' in desc order
            profiles = await _context.QueryAsync<Profile>("ID", new DynamoDBOperationConfig
            {
                BackwardQuery = true
            }).GetRemainingAsync();
            Console.WriteLine("\nQuery based on partition key: 'ID' in desc order");
            foreach (var item in profiles)
            {
                Console.WriteLine(item.ToString());
            }
            #endregion


            #region Query based on partition key: 'ID' and the name must start with string 'Ra'
            try
            {
                profiles = await _context.QueryAsync<Profile>("ID", new DynamoDBOperationConfig
                {
                    QueryFilter = new List<ScanCondition>
                {
                    new ScanCondition(nameof(Profile.Name),ScanOperator.BeginsWith,"Ra")
                }
                }).GetRemainingAsync();
                Console.WriteLine("\nQuery based on partition key: 'ID' and the name must contain string 'Ra'");
                foreach (var item in profiles)
                {
                    Console.WriteLine(item.ToString());
                }
            }
            catch (AmazonDynamoDBException ex)
            {
                Console.WriteLine(ex);
            }
            #endregion

        }

        public async Task LoadItems()
        {
            Profile result = await _context.LoadAsync<Profile>("ID", "Mirza Ghulam Rasyid");
            if (result != null)
            {
                Console.WriteLine(result.ToString());
            }
        }
        public async Task InsertNewItems(List<Profile> profiles)
        {
            if (profiles != null && profiles.Count > 0)
            {
                try
                {
                    BatchWrite<Profile> batchWrite = _context.CreateBatchWrite<Profile>();
                    batchWrite.AddPutItems(profiles);
                    Console.WriteLine("Inserting list of profiles...");
                    await batchWrite.ExecuteAsync();
                    Console.WriteLine("Done");

                }
                catch (Exception ex)
                {
                    Console.WriteLine("[!] Insert data failed");
                    Console.WriteLine(ex);
                }
            }
        }
        public async Task InsertNewItem()
        {
            try
            {
                Profile jsonBarley = new Profile("Json Barley", "jbarley@hotmail.com", "US", new[]
                {
                    "+1-431-212",
                    "+1-653-552"
                });
                await _context.SaveAsync(jsonBarley);
                Console.WriteLine("New item has been added");

            }
            catch (AmazonDynamoDBException ex)
            {
                Console.WriteLine(ex);
            }
        }
        public async Task UpdateItem()
        {
           
            var profile = await _context.LoadAsync<Profile>("US", "Json Barley");
            if (profile != null)
            {
                profile.Email = "jsonbarley95@gmail.com";
                await _context.SaveAsync(profile);
            }
        }

    }
}
