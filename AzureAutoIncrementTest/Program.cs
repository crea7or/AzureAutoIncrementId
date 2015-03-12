using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureAutoIncrementTest
{
	class Program
	{
		static void Main( string[] args )
		{
			// Account name and key. Modify for your account.
			// setup before use!
			string accountName = "";
			string accountKey = "";

			if ( accountName.Length == 0 || accountKey.Length == 0 )
			{
				Console.WriteLine( "Setup accountName and accountKey!" );
			}
			else
			{
				try
				{
					//Get a reference to the storage account, with authentication credentials.
					CloudStorageAccount storageAccount = new CloudStorageAccount( new StorageCredentials( accountName, accountKey ), true );

					CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
					// Retrieve a reference to a container. 
					CloudTable cloudTable = tableClient.GetTableReference( "system" );
					// Create the container if it does not already exist.
					cloudTable.CreateIfNotExists();

					// Output container URI to debug window.
					Console.WriteLine( cloudTable.Uri );

					// Read storage
					string partitionFilter = TableQuery.GenerateFilterCondition( "PartitionKey", QueryComparisons.Equal, "PK0123456789" );
					string rowFilter = TableQuery.GenerateFilterCondition( "RowKey", QueryComparisons.Equal, "RK0123456789" );
					string finalFilter = TableQuery.CombineFilters( partitionFilter, TableOperators.And, rowFilter );

					bool itemChanged;

					for ( int i = 0; i < 20; i++ )
					{
						itemChanged = false;

						DateTime dtm = DateTime.Now;

						TableQuery<IdEntity> query = new TableQuery<IdEntity>().Where( finalFilter );
						var list = cloudTable.ExecuteQuery( query ).ToList();

						IdEntity customer1 = null;

						// Check if exist
						if ( list.Count == 0 )
						{
							//
							customer1 = new IdEntity( "PK0123456789", "RK0123456789" );
							customer1.Id = 0;
							// Create the TableOperation that inserts the customer entity.
							var insertOperation = TableOperation.Insert( customer1 );

							// Execute the insert operation.
							cloudTable.Execute( insertOperation );
						}
						else
						{
							customer1 = list[ 0 ];
							customer1.Id++;

							try
							{
								TableResult tblr = cloudTable.Execute( TableOperation.InsertOrReplace( customer1 ), null, new OperationContext { UserHeaders = new Dictionary<String, String> { { "If-Match", customer1.ETag } } } );
							}
							catch ( StorageException ex )
							{
								if ( ex.RequestInformation.HttpStatusCode == 412 )
								{
									Console.WriteLine( "Optimistic concurrency violation – entity has changed since it was retrieved." );
									itemChanged = true;
								}
								else
								{
									throw;
								}
							}
						}

						if ( itemChanged )
						{
							continue;
						}

						TimeSpan tsp = DateTime.Now - dtm;

						query = new TableQuery<IdEntity>().Where( finalFilter );
						list = cloudTable.ExecuteQuery( query ).ToList();
						if ( list.Count == 1 )
						{
							customer1 = list[ 0 ];
							Console.WriteLine( "Id: {0}, Time: {1} ms, Etag: {2}", customer1.Id, ( int )tsp.Milliseconds, customer1.ETag );
						}
						else
						{
							Console.WriteLine( "No item found" );
						}
					}

				}
				catch ( Exception ex )
				{
					Console.WriteLine( ex.Message );
				}
			}

			Console.ReadKey();
		}

	}

	public class IdEntity : TableEntity
	{
		public long Id { get; set; }

		public IdEntity( string lastName, string firstName )
		{
			PartitionKey = lastName;
			RowKey = firstName;
		}

		public IdEntity() { }
	}
}
