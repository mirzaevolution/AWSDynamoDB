using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;

namespace Rewind.ObjectPersistentModelAPI
{
    public class ObjectPersistentLevelOperations
    {
        private readonly DynamoDBContext _context;
        public ObjectPersistentLevelOperations(IAmazonDynamoDB amazonDynamoDB)
        {
            _context = new DynamoDBContext(amazonDynamoDB);
        }
        private void IterateList(IEnumerable<Profile> profiles)
        {
            foreach (var profile in profiles)
            {
                Console.WriteLine(profile.ToString());
            }
        }
        
        public async Task ScanAll()
        {
            List<Profile> profiles = await _context.ScanAsync<Profile>(null).GetRemainingAsync();
            IterateList(profiles);
        }


        public async Task ScanByConditions()
        {
            Console.WriteLine("Grab all items where name starts with 'Mi': ");
            List<Profile> profiles = await _context.ScanAsync<Profile>(new[]
            {
                new ScanCondition(nameof(Profile.Name), ScanOperator.BeginsWith, "Mi")
            }).GetRemainingAsync();
            IterateList(profiles);
            Console.WriteLine("\n\nGrab all items where email in ('ghulamcyber@hotmail.com','raraanjani@gmail.com','hawk@gmail.com'): ");
            ScanFilter scanFilter = new ScanFilter();
            scanFilter.AddCondition(nameof(Profile.Email), ScanOperator.In, "ghulamcyber@hotmail.com", "raraanjani@gmail.com", "hawk@gmail.com");
            profiles = await _context.FromScanAsync<Profile>(new ScanOperationConfig
            {
                Filter = scanFilter
            }).GetRemainingAsync();
            IterateList(profiles);
            
        }

        public async Task GetItem(string countryId, string name)
        {
            Profile profile = await _context.LoadAsync<Profile>(countryId, name);
            if (profile != null)
            {
                Console.WriteLine(profile.ToString());
            }
            else
            {
                Console.WriteLine("No item matched");
            }
        }

        public async Task QueryByCountryId(string countryId = "ID")
        {
            List<Profile> profiles = await _context.QueryAsync<Profile>(countryId).GetRemainingAsync();
            IterateList(profiles);
        }

        public async Task QueryByConditions()
        {
            Console.WriteLine("Query where name starts with 'M' for ID country sorted descending: ");
            QueryFilter filter = new QueryFilter();
            filter.AddCondition(nameof(Profile.CountryId), QueryOperator.Equal, "ID");
            filter.AddCondition(nameof(Profile.Name), QueryOperator.BeginsWith, "M");
            List<Profile> profiles = await _context.FromQueryAsync<Profile>(new QueryOperationConfig
            {
                BackwardSearch = true,
                Filter = filter
            }).GetRemainingAsync();

            IterateList(profiles);

            Console.WriteLine("\n\nQuery where email in 'ghulamcyber@hotmail.com', 'raraanjani@gmail.com' sorted descending");
            profiles = await _context.QueryAsync<Profile>("ID", new DynamoDBOperationConfig
            {
                QueryFilter = new List<ScanCondition>
                {
                    new ScanCondition(nameof(Profile.Email), ScanOperator.In, "ghulamcyber@hotmail.com", "raraanjani@gmail.com")
                },
                BackwardQuery = true
            }).GetRemainingAsync();
            IterateList(profiles);
        }

        public async Task InsertNewItem(Profile profile)
        {
            await _context.SaveAsync(profile);
            Console.WriteLine("Item inserted");
        }

        public async Task BatchInserts(List<Profile> profiles)
        {
            BatchWrite<Profile> batchWrite = _context.CreateBatchWrite<Profile>();
            batchWrite.AddPutItems(profiles);
            await batchWrite.ExecuteAsync();
            Console.WriteLine("Items inserted successfully");
        }

        public async Task UpdateEmail(string countryId, string name, string email)
        {
            Profile profile = await _context.LoadAsync<Profile>(countryId, name);
            if (profile != null)
            {
                profile.Email = email;
                await _context.SaveAsync(profile);
                Console.WriteLine("Item updated");

                Console.WriteLine();
                await ScanAll();
            }
            else
            {
                Console.WriteLine("Item not found!");
            }
        }
        //we can't update sort key 
        //https://stackoverflow.com/questions/55474664/dynamoddb-how-to-update-sort-key/55474827
        public async Task UpdateName(string countryId, string oldName, string newName)
        {
            Profile profile = await _context.LoadAsync<Profile>(countryId, oldName);
            if (profile != null)
            {
                await _context.DeleteAsync<Profile>(countryId, oldName);
                profile.Name = newName;
                await _context.SaveAsync<Profile>(profile);
                Console.WriteLine("Item updated");
                Console.WriteLine();
                await ScanAll();
            }
            else
            {
                Console.WriteLine("Item not found!");
            }
        }
    }
}
