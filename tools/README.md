ODCS-FusionTool tools
==============

This directory contains tools and helper scripts.

odcs-1.0.1-fix-script.sql
-------------------------

This script will add metadata required by ODCS-FusionTool which are missing in ODCleanStore releases <= 1.0.1.
Queries in this script should be run on an ODCleanStore clean database instance in order to

* make ODCS-FusionTool load data from attached graphs,
* use ontology mappings defined from ODCleanStore administration frontend.

The script can be executed using Virtuoso's isql tool:

	isql <host>:<port> <username> <password> odcs-1.0.1-fix-script.sql
