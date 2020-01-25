using System;
using System.Data;
using System.Text.Json;
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
            CreateAppendEventFunction();
        }

        public bool AppendEvent<TStream>(Guid streamId, object @event, long? expectedVersion = null)
        {
            // 1. Pass unique event id as Guid (as it's the primary key)
            var eventId = Guid.NewGuid();

            //2. Serialize event data to JSON
            string eventData = JsonSerializer.Serialize(@event);

            //3. Send event type
            string eventType = @event.GetType().ToString();

            //4. Send stream type
            string streamType = typeof(TStream).ToString();

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
                    expected_stream_version bigint default null
                ) RETURNS boolean
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    stream_version bigint;
                BEGIN

                    -- 1. Insert into stream table if there is no stream with provided streamId
                        IF NOT EXISTS (SELECT 1 FROM streams s WHERE s.id = stream_id) THEN
                          INSERT INTO streams (id, type, version) VALUES (stream_id, stream_type, expected_stream_version);
                        END IF;
                    -- 2. Insert new row into events table with version equal to expected_stream_version + 1
                       stream_version := 1;
                       INSERT INTO events (id, data, stream_id, type, version)
                            VALUES ((select s.id from streams s where s.id = stream_id), data, stream_id, stream_type, stream_version);
                    -- 3. Update stream version with expected_stream_version + 1
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
