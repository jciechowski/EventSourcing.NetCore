using System;
using System.Data;
using System.Text.Json;
using Dapper;
using Marten;
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
            CreateAppendEventFunction();
        }

        public bool AppendEvent<TStream>(Guid streamId, object @event, long? expectedVersion = null)
        {
            // 1. Pass unique event id as Guid (as it's the primary key)
            var eventId = Guid.NewGuid();

            //2. Serialize event data to JSON
            string eventData = JsonSerializer.Serialize(@event); // TODO: Add here @event serialization

            //3. Send event type
            string eventType = @event.GetType().ToString(); // TODO: Add here getting event type name

            //4. Send stream type
            string streamType = typeof(TStream).ToString(); // TODO: Add here getting stream type

            return databaseConnection.QuerySingle<bool>(
                "SELECT append_event(@Id, @Data::jsonb, @Type, @StreamId, @StreamType, @ExpectedVersion)",
                new
                {
                    Id = eventId,
                    Data = eventData,
                    Type = eventType,
                    StreamId = streamId,
                    StreamType = streamType,
                    ExpectedVersion = expectedVersion ?? 0
                },
                commandType: CommandType.Text
            );
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
            const string CreatEventsTableSQL =
                @"CREATE TABLE IF NOT EXISTS events(
                      id             UUID                      NOT NULL    PRIMARY KEY,
                      data           JSONB                     NOT NULL,
                      stream_id      UUID                      NOT NULL,
                      type           TEXT                      NOT NULL,
                      version        BIGINT                    NOT NULL,
                      created        timestamp with time zone  NOT NULL    default (now()),
                      FOREIGN KEY(stream_id) REFERENCES streams(id),
                      CONSTRAINT events_stream_and_version UNIQUE(stream_id, version)
                );";
            databaseConnection.Execute(CreatEventsTableSQL);
        }

        private void CreateAppendEventFunction()
        {
            const string AppendEventFunctionSQL =
                 @"CREATE OR REPLACE FUNCTION append_event(
                     id uuid,
                     data jsonb,
                     type text,
                     stream_id uuid,
                     stream_type text,
                     expected_stream_version bigint default null)
                 RETURNS boolean
                 LANGUAGE plpgsql
                 AS $$
                 DECLARE
                     stream_version int;
                 BEGIN
                     -- 1. get stream version
                     SELECT version INTO stream_version FROM streams s WHERE s.id = stream_id;

                     -- 2. if stream doesn't exist - create new one with version 0
                     IF NOT EXISTS (SELECT 1 FROM streams s WHERE s.id = stream_id) THEN
                         INSERT INTO streams (id, type, version) VALUES (stream_id, stream_type, 0);
                     END IF;

                     -- 3. check optimistic concurrency - return false if expected stream version is different than stream version
                     IF stream_version <> expected_stream_version THEN
                        RETURN FALSE;
                     END IF;

                     -- 4. increment stream_version
                     SELECT version INTO stream_version FROM streams s WHERE s.id = stream_id;
                     stream_version := stream_version + 1;

                     -- 5. append event to events table
                     INSERT INTO events (id, data, stream_id, type, version)
                             VALUES ((select s.id from streams s where s.id = stream_id), data, stream_id, stream_type, stream_version);

                     -- 6. update stream version in stream table
                     UPDATE streams s SET version = stream_version WHERE s.id = stream_id;

                     RETURN TRUE;
                 END;
                 $$;";
            databaseConnection.Execute(AppendEventFunctionSQL);
        }

        public void Dispose()
        {
            databaseConnection.Dispose();
        }
    }
}
