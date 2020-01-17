using System;
using Dapper;
using Npgsql;

namespace EventStoreBasics
{
    public class EventStore: IDisposable, IEventStore
    {
        private readonly NpgsqlConnection databaseConnection;

        public EventStore(NpgsqlConnection databaseConnection)
        {
            this.databaseConnection = databaseConnection;
        }

        public void Dispose()
        {
            databaseConnection.Dispose();
        }

        public void Init()
        {
            // See more in Greg Young's "Building an Event Storage" article https://cqrs.wordpress.com/documents/building-event-storage/
            CreateStreamsTable();
        }

        private void CreateStreamsTable()
        {
            const string createStreamsTable = @"CREATE TABLE streams(
                    id uuid,
                    type text,
                    version bigint
                )";
            databaseConnection.Execute(createStreamsTable);
        }
    }
}
