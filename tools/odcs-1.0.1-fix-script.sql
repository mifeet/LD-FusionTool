/* Fixes missing odcs:generatedGraph properties for ontology mappings named graphs in ODCS releases <= 1.0.1 */
SPARQL INSERT INTO <http://opendata.cz/infrastructure/odcleanstore/internal/metadata> 
{ ?g <http://opendata.cz/infrastructure/odcleanstore/generatedGraph> 1 } 
WHERE { GRAPH ?g {?s ?p ?o} FILTER (bif:starts_with(str(?g), 'http://opendata.cz/infrastructure/odcleanstore/internal/ontologyMappings/'))};

/* Fixes missing odcs:attachedGraph properties for attached graphs in ODCS releases <= 1.0.1 */
SPARQL INSERT INTO <http://opendata.cz/infrastructure/odcleanstore/internal/metadata>
{ `iri(bif:concat('http://opendata.cz/infrastructure/odcleanstore/data/', replace(str(?g), 'http://opendata.cz/infrastructure/odcleanstore/internal/generatedLinks/', '')))` 
	odcs:attachedGraph ?g }
WHERE { GRAPH ?g {?s ?p ?o} FILTER (bif:starts_with(str(?g), 'http://opendata.cz/infrastructure/odcleanstore/internal/generatedLinks/'))};
