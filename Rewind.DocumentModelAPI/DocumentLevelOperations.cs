using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Rewind.DocumentModelAPI
{

    public class DocumentLevelOperations
    {
        private readonly IAmazonDynamoDB _dynamoDBClient;
        private readonly string _tableName = "Profile";
        private Table _table;
        public DocumentLevelOperations(IAmazonDynamoDB amazonDynamoDB)
        {
            _dynamoDBClient = amazonDynamoDB;
            _table = Table.LoadTable(_dynamoDBClient, _tableName);
        }

        protected virtual Profile ConvertToProfile(Document document)
        {
            return new Profile(
                    id: document[nameof(Profile.Id)],
                    name: document[nameof(Profile.Name)],
                    email: document[nameof(Profile.Email)],
                    countryId: document[nameof(Profile.CountryId)],
                    phones: document[nameof(Profile.Phones)].AsListOfString()
                );
        }
        protected virtual Document ConvertToDocument(Profile profile)
        {
            return new Document
            {
                {
                    nameof(Profile.Id), profile.Id
                },
                {
                    nameof(Profile.Name), profile.Name
                },
                {
                    nameof(Profile.Email), profile.Email
                },
                {
                    nameof(Profile.CountryId), profile.CountryId
                },
                {
                    nameof(Profile.Phones), profile.Phones
                }
            };
        }
        private void IterateList(IEnumerable<Profile> profiles)
        {
            foreach(var profile in profiles)
            {
                Console.WriteLine(profile.ToString());
            }
        } 

        public async Task ScanAll()
        {
            List<Document> documents =  await _table.Scan(new ScanFilter()).GetRemainingAsync();
            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }
        }
        public async Task ScanByConditions()
        {
            Console.WriteLine("Grab all items where name starts with 'Mi': ");
            ScanFilter filter = new ScanFilter();
            filter.AddCondition(nameof(Profile.Name), ScanOperator.BeginsWith, "Mi");
            List<Document> documents = await _table.Scan(filter).GetRemainingAsync();
            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }
            Console.WriteLine("\n\nGrab all items where email in ('ghulamcyber@hotmail.com','raraanjani@gmail.com','hawk@gmail.com'): ");
            filter = new ScanFilter();
            filter.AddCondition(nameof(Profile.Email), ScanOperator.In, new List<AttributeValue>
            {
                new AttributeValue("ghulamcyber@hotmail.com"),
                new AttributeValue("raraanjani@gmail.com"),
                new AttributeValue("hawk@gmail.com")
            });
            documents = await _table.Scan(new ScanOperationConfig
            {
                Filter = filter
            }).GetRemainingAsync();
            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }
        }
        public async Task GetItem(string countryId, string name)
        {
            Document document = await _table.GetItemAsync(countryId, name);
            if (document != null)
            {
                Profile profile = ConvertToProfile(document);
                Console.WriteLine(profile);
            }

        }

        public async Task QueryByCountryId(string countryId)
        {
            QueryFilter filter = new QueryFilter();
            filter.AddCondition(nameof(Profile.CountryId), QueryOperator.Equal, countryId);
            List<Document> documents = await _table.Query(filter).GetRemainingAsync();
            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }
        }

        public async Task QueryByConditions()
        {
            Console.WriteLine("Query where name starts with 'M' for ID country sorted descending: ");
            QueryFilter filter = new QueryFilter();
            filter.AddCondition(nameof(Profile.CountryId), QueryOperator.Equal, "ID");
            filter.AddCondition(nameof(Profile.Name), QueryOperator.BeginsWith, "M");
            //List<Document> documents = await _table.Query("ID", filter).GetRemainingAsync();
            List<Document> documents = await _table.Query(new QueryOperationConfig
            {
                BackwardSearch = true,
                Filter = filter
            }).GetRemainingAsync();

            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }

            Console.WriteLine("\n\nQuery where email equals to 'ghulamcyber@hotmail.com'");
            filter = new QueryFilter(nameof(Profile.Email), QueryOperator.Equal, "ghulamcyber@hotmail.com");
            documents = await _table.Query("ID", filter).GetRemainingAsync();
            if (documents.Count > 0)
            {
                IterateList(documents.Select(ConvertToProfile));
            }
        }

        public async Task InsertNewItem(Profile profile)
        {
            try
            {
                Document document = ConvertToDocument(profile);
                var result = await _table.PutItemAsync(document, new PutItemOperationConfig
                {
                    ReturnValues = ReturnValues.AllNewAttributes
                });
                Console.WriteLine("Item inserted successfully");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public async Task InsertBatchItems(List<Profile> profiles)
        {
            try
            {
                DocumentBatchWrite request = _table.CreateBatchWrite();
                foreach(var profile in profiles)
                {
                    request.AddDocumentToPut(ConvertToDocument(profile));
                }
                await request.ExecuteAsync();
                Console.WriteLine("Items created successfully");
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        public async Task UpdateItem()
        {
            Document itemToUpdate = await _table.GetItemAsync("US", "James Gordon");
            if (itemToUpdate != null)
            {
                itemToUpdate[nameof(Profile.Email)] = "jamesgordon21@hotmail.com";
                await _table.UpdateItemAsync(itemToUpdate);
                Console.WriteLine("Item updated successfully");
            }
        }
    }
}
