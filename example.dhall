{ postgres =
  { url = "jdbc:postgresql://localhost:5432/example"
  , user = "replication"
  , pass = "replication"
  , batch_size = 100
  , batch_buffer = 10
  , slot_name = "test"
  }
, kafka =
  { bootstrap = [ "localhost:9092" ]
  , batch_size = 100 * 1000
  }
, topic.prefix = "localdb-cdc"
}
