cr-batch tools
==============

This directory contains tools and helper scripts.

odcs-1.0.1-fix-script.sql
-------------------------

This script will add metadata required by cr-batch which are missing in ODCleanStore releases <= 1.0.1.
Queries in this script should be run on an ODCleanStore clean database instance in order to

* make cr-batch load data from attached graphs,
* use ontology mappings defined from ODCleanStore administration frontend.

The script can be executed using the Virtuoso isql tool:

	isql <host>:<port> <username> <password> odcs-1.0.1-fix-script.sql
