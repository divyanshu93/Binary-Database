# Binary-Database
Rudimentary Database using Index Files

To run the project

Go to src directory from command line and perform the following commands:
> javac MyDatabase.java

> java MyDatabase

Prompt:

Upon launch, engine should present a prompt similar to the mysql> prompt, where interactive
commands may be entered. The prompt should be:
davisql>

Supported Commands:

• SHOW SCHEMAS – Displays all schemas defined in your database.

• USE – Chooses a schema.

• SHOW TABLES – Displays all tables in the currently chosen schema.

• CREATE SCHEMA – Creates a new schema to hold tables.

• CREATE TABLE – Creates a new table schema, i.e. a new empty table.

• INSERT INTO TABLE – Inserts a row/record into a table.

• DROP TABLE – Remove a table schema, and all of its contained data.

• “SELECT-FROM-WHERE” -style query

• EXIT – Cleanly exits the program and saves all table and index information in non-volatile files.
