(ns delta-sharing.client.proto
  "Core delta sharing client protocol")


(defprotocol Client
  "Marker protocol that indicates an object is a valid delta-sharing client interface."

  (list-shares
    [client opts]
    "Returns a list of all shares accessible to the recipient.")

  (get-share
    [client share]
    "Returns the information about a share.")

  (list-share-schemas
    [client share opts]
    "Returns a list of all the shemas in a share.")

  (list-schema-tables
    [client share schema opts]
    "Returns a list of tables in a schema.")

  (list-share-tables
    [client share opts]
    "Returns a list of tables within all schemas of a share.")

  (query-table-version
    [client share schema table opts]
    "Returns information about the version of a table.")

  (query-table-metadata
    [client share schema table opts]
    "Returns information about a table's metadata.")

  (read-table-data
    [client share schema table opts]
    "Returns a list of data files along with other metadata about the table and protocol.")

  (read-change-data-feed
    [client share schema table opts]
    "Returns a list of change data files along with other metadata about the table and protocol."))
