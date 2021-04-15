using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace StarterIntro
{
    public class DocumentModel
    {
        private readonly IAmazonDynamoDB _dynamoDbClient;
        private string _tableName;
        private Table _table;
        public DocumentModel(IAmazonDynamoDB dynamoDbClient, string tableName)
        {
            _dynamoDbClient = dynamoDbClient;
            _tableName = tableName;
            _table = Table.LoadTable(dynamoDbClient, _tableName);
        }

        private Document ConvertToDocument(Profile profile)
        {
            return new Document()
            {
                { nameof(Profile.Id), profile.Id },
                { nameof(Profile.CountryId), profile.CountryId },
                { nameof(Profile.Name), profile.Name },
                { nameof(profile.Email), profile.Email },
                { nameof(Profile.Phones), profile.Phones }
            };
        }
        private Profile ConvertToProfile(Document document)
        {
            return new Profile(
                    id: document[nameof(Profile.Id)],
                    name: document[nameof(Profile.Name)],
                    email: document[nameof(Profile.Email)],
                    countryId: document[nameof(Profile.CountryId)],
                    phones: document[nameof(Profile.Phones)].AsListOfString()
                );
        }


        public async Task InsertNewItems(List<Profile> profiles)
        {
            try
            {
                List<Document> documents = profiles.Select(ConvertToDocument).ToList();
                DocumentBatchWrite batchWrite = _table.CreateBatchWrite();
                foreach (var document in documents)
                {
                    batchWrite.AddDocumentToPut(document);
                }
                await batchWrite.ExecuteAsync();
                Console.WriteLine("Documents have been inserted successfully");
            }
            catch (AmazonDynamoDBException ex)
            {
                Console.WriteLine(ex);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
        public async Task InsertOrUpdateItem(Profile profile, bool update=false)
        {
            try
            {
                Document document = ConvertToDocument(profile);
                document = update ? await _table.UpdateItemAsync(document) : await _table.PutItemAsync(document);
                Console.WriteLine($"Document has been {(update ? "updated" : "inserted")} successfully");

            }
            catch (AmazonDynamoDBException ex)
            {
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public async Task ScanItems()
        {
            try
            {
                #region All items
                var rawDocList = await _table.Scan(new ScanOperationConfig()).GetRemainingAsync();
                var profileList = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile) : new List<Profile>();
                foreach (var item in profileList)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion

                ScanFilter scanFilter = new ScanFilter();

                #region People lived in ID
                Console.WriteLine("\n#People lived in ID: ");
                scanFilter.AddCondition(nameof(Profile.CountryId), ScanOperator.Equal, "ID");
                rawDocList = await _table.Scan(scanFilter).GetRemainingAsync();
                profileList = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile) : new List<Profile>();
                foreach (var item in profileList)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion



                #region People which the names start with 'Mi'
                Console.WriteLine("\n#People which the names start with 'Mi': ");
                scanFilter.AddCondition(nameof(Profile.Name), ScanOperator.BeginsWith, "Mi");
                rawDocList = await _table.Scan(scanFilter).GetRemainingAsync();
                profileList = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile) : new List<Profile>();
                foreach (var item in profileList)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion

                #region People which the names start with 'Mi' and CountryId is 'ID'
                Console.WriteLine("\n#People which the names start with 'Mi' and CountryId is 'ID': ");
                scanFilter.AddCondition(nameof(Profile.CountryId), ScanOperator.Equal, "ID");
                scanFilter.AddCondition(nameof(Profile.Name), ScanOperator.BeginsWith, "Mi");
                rawDocList = await _table.Scan(scanFilter).GetRemainingAsync();
                profileList = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile) : new List<Profile>();
                foreach (var item in profileList)
                {
                    Console.WriteLine(item.ToString());
                }
                #endregion
            }
            catch (AmazonDynamoDBException ex)
            {
                Console.WriteLine(ex);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }


        public async Task QueryItems()
        {

            #region Query based on partition key: 'ID' in asc order
            QueryFilter queryFilter = new QueryFilter(nameof(Profile.CountryId), QueryOperator.Equal, "ID");
            var rawDocList = await _table.Query(queryFilter).GetRemainingAsync();
            List<Profile> profiles = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile).ToList() : new List<Profile>();
            Console.WriteLine("\nQuery based on partition key: 'ID' in asc order");
            foreach (var item in profiles)
            {
                Console.WriteLine(item.ToString());
            }
            #endregion

            #region Query based on partition key: 'ID' in desc order
            queryFilter = new QueryFilter(nameof(Profile.CountryId), QueryOperator.Equal, "ID");
            rawDocList = await _table.Query(new QueryOperationConfig
            {
                Filter = queryFilter,
                BackwardSearch = true
            }).GetRemainingAsync();
            profiles = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile).ToList() : new List<Profile>();
            Console.WriteLine("\nQuery based on partition key: 'ID' in desc order");
            foreach (var item in profiles)
            {
                Console.WriteLine(item.ToString());
            }
            #endregion


            #region Query based on partition key: 'ID' and the name must start with string 'Ra'
            try
            {
                queryFilter = new QueryFilter(nameof(Profile.CountryId), QueryOperator.Equal, "ID");
                queryFilter.AddCondition(nameof(Profile.Name), QueryOperator.BeginsWith, "Ra");
                rawDocList = await _table.Query(new QueryOperationConfig
                {
                    Filter = queryFilter
                }).GetRemainingAsync();
                profiles = rawDocList != null && rawDocList.Count > 0 ? rawDocList.Select(ConvertToProfile).ToList() : new List<Profile>();
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


        public async Task<Profile> LoadItems(string countryId, string name)
        {
            var rawDocument = await _table.GetItemAsync(countryId, name);
            Profile result = ConvertToProfile(rawDocument);
            if (result != null)
            {
                Console.WriteLine(result.ToString());
            }
            return result;
        }
    }
}
