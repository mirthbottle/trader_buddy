:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'lob_indus_2023_annualtotal.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
// NOTE: The following constraint creation syntax is generated based on the current connected database version 5.20-aura.
CREATE CONSTRAINT `name_company_uniq` IF NOT EXISTS
FOR (n: `company`)
REQUIRE (n.`name`) IS UNIQUE;
CREATE CONSTRAINT `catcode_industry_uniq` IF NOT EXISTS
FOR (n: `industry`)
REQUIRE (n.`catcode`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`client` IN $idsToSkip AND NOT row.`client` IS NULL
CALL {
  WITH row
  MERGE (n: `company` { `name`: row.`client` })
  SET n.`name` = row.`client`
  SET n.`sub` = row.`sub`
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`catcode` IN $idsToSkip AND NOT row.`catcode` IS NULL
CALL {
  WITH row
  MERGE (n: `industry` { `catcode`: row.`catcode` })
  SET n.`catcode` = row.`catcode`
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row 
CALL {
  WITH row
  MATCH (source: `company` { `name`: row.`client` })
  MATCH (target: `industry` { `catcode`: row.`catcode` })
  MERGE (source)-[r: `PART_OF`]->(target)
  SET r.`lobbying total` = toFloat(trim(row.`total`))
  SET r.`year` = toInteger(trim(row.`year`))
} IN TRANSACTIONS OF 10000 ROWS;
