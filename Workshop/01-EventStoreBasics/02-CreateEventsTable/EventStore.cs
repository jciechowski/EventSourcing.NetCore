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

        public void Init()
        {
            // See more in Greg Young's "Building an Event Storage" article https://cqrs.wordpress.com/documents/building-event-storage/
            CreateStreamsTable();
            CreateEventsTable();
        }

        private void CreateStreamsTable()
        {
            const string CreatStreamsTableSQL =
                @"CREATE TABLE IF NOT EXISTS streams(
                      id             UUID                      NOT NULL    PRIMARY KEY,
                      type           TEXT                      NOT NULL,
                      version        BIGINT                    NOT NULL
                  );";
            databaseConnection.Execute(CreatStreamsTableSQL);
        }

        private void CreateEventsTable()
        {
            const string createEventTable =
                @"CREATE TABLE Events(
                    id UUID NOT NULL PRIMARY KEY,
                    data JSONB,
                    stream_id UUID,
                    type TEXT,
                    version BIGINT,
                    created TIMESTAMP WITH TIME ZONE 
                );";
            databaseConnection.Execute(createEventTable);
        }

        public void Dispose()
        {
            databaseConnection.Dispose();
        }
    }
}
