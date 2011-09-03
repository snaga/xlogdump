/*
 * xlogdump_oid2name.c
 *
 * a collection of functions to get database object names from the oids
 * by looking up the system catalog.
 */
#include "xlogdump_oid2name.h"

#include "pqexpbuffer.h"
#include "postgres.h"

static PGconn		*conn = NULL; /* Connection for translating oids of global objects */
static PGconn		*lastDbConn = NULL; /* Connection for translating oids of per database objects */
static PGresult		*res;
static PQExpBuffer	dbQry;

static char *pghost = NULL;
static char *pgport = NULL;
static char *pguser = NULL;
static char *pgpass = NULL;

/* Oids used for checking if we need to search for an objects name or if we can use the last one */
static Oid 		lastDbOid;
static Oid 		lastSpcOid;
static Oid 		lastRelOid;

/*
 * Open a database connection
 */
bool
DBConnect(const char *host, const char *port, char *database, const char *user)
{
	char	*password = NULL;
	char	*password_prompt = NULL;
	bool	need_pass;
	PGconn  *conn = NULL;

	pghost = strdup(host);
	pgport = strdup(port);
	pguser = strdup(user);

	/* loop until we have a password if requested by backend */
	do
	{
		need_pass = false;

		conn = PQsetdbLogin(host,
				    port,
				    NULL,
				    NULL,
				    database,
				    user,
				    password);

		if (PQstatus(conn) == CONNECTION_BAD &&
			strcmp(PQerrorMessage(conn), PQnoPasswordSupplied) == 0 &&
			!feof(stdin))
		{
			PQfinish(conn);
			need_pass = true;
			free(password);
			password = NULL;
			printf("\nPassword: ");
			password = simple_prompt(password_prompt, 100, false);
		}
	} while (need_pass);

	pgpass = strdup(password);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "Connection to database failed: %s",
			PQerrorMessage(conn));
		return false;
	}
	
	dbQry = createPQExpBuffer();

	return true;
}

/*
 * Atempt to read the name of tablespace into lastSpcName
 * (if there's a database connection and the oid changed since lastSpcOid)
 */
char *
getSpaceName(uint32 space, char *buf, size_t buflen)
{
	resetPQExpBuffer(dbQry);
	if((conn) && (lastSpcOid != space))
	{
		PQclear(res);
		appendPQExpBuffer(dbQry, "SELECT spcname FROM pg_tablespace WHERE oid = %i", space);
		res = PQexec(conn, dbQry->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		resetPQExpBuffer(dbQry);
		lastSpcOid = space;
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);
			return buf;
		}
	}
	else if(lastSpcOid == space)
		return buf;

	/* Didn't find the name, return string with oid */
	snprintf(buf, buflen, "%u", space);

	return buf;
}


/*
 * Atempt to get the name of database (if there's a database connection)
 */
char *
getDbName(uint32 db, char *buf, size_t buflen)
{
	resetPQExpBuffer(dbQry);
	if((conn) && (lastDbOid != db))
	{	
		PQclear(res);
		appendPQExpBuffer(dbQry, "SELECT datname FROM pg_database WHERE oid = %i", db);
		res = PQexec(conn, dbQry->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		resetPQExpBuffer(dbQry);
		lastDbOid = db;
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);

			// Database changed makes new connection
			PQfinish(lastDbConn);

			lastDbConn = PQsetdbLogin(pghost,
						  pgport,
						  NULL,
						  NULL,
						  buf,
						  pguser,
						  pgpass);

			return buf;
		}
	}
	else if(lastDbOid == db)
		return buf;

	/* Didn't find the name, return string with oid */
	snprintf(buf, buflen, "%u", db);
	return buf;
}

/*
 * Atempt to get the name of relation and copy to relName 
 * (if there's a database connection and the reloid changed)
 * Copy a string with oid if not found
 */
char *
getRelName(uint32 relid, char *buf, size_t buflen)
{
	resetPQExpBuffer(dbQry);
	if((conn) && (lastDbConn) && (lastRelOid != relid))
	{
		PQclear(res);
		/* Try the relfilenode and oid just in case the filenode has changed
		   If it has changed more than once we can't translate it's name */
		appendPQExpBuffer(dbQry, "SELECT relname, oid FROM pg_class WHERE relfilenode = %i OR oid = %i", relid, relid);
		res = PQexec(lastDbConn, dbQry->data);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		resetPQExpBuffer(dbQry);
		lastRelOid = relid;
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);
			/* copy the oid since it could be different from relfilenode */
			lastRelOid = (uint32) atoi(PQgetvalue(res, 0, 1));
			return buf;
		}
	}
	else if(lastRelOid == relid)
		return buf;
	
	/* Didn't find the name, return string with oid */
	snprintf(buf, buflen, "%u", relid);
	return buf;
}


int
relid2attr_begin(void)
{
	resetPQExpBuffer(dbQry);
	PQclear(res);

	appendPQExpBuffer(dbQry, "SELECT attname, atttypid from pg_attribute where attnum > 0 AND attrelid = '%i' ORDER BY attnum", lastRelOid);
	res = PQexec(lastDbConn, dbQry->data);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
		PQclear(res);
		return -1;
	}
	resetPQExpBuffer(dbQry);

	return PQntuples(res);
}

int
relid2attr_fetch(int i, char *attname, Oid *atttypid)
{
	snprintf(attname, NAMEDATALEN, PQgetvalue(res, i, 0));
	*atttypid = atoi( PQgetvalue(res, i, 1) );
	return i;
}

void
relid2attr_end()
{
	resetPQExpBuffer(dbQry);
	PQclear(res);
}

bool
do_oid2name(void)
{
	return (conn && lastDbConn);
}

void
DBDisconnect(void)
{
	destroyPQExpBuffer(dbQry);
	if(lastDbConn)
		PQfinish(lastDbConn);
	if(conn)
		PQfinish(conn);

}
