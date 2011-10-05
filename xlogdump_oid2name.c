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

static PGresult		*_res = NULL; /* a result set variable for relname2attr_*() functions */

static char *pghost = NULL;
static char *pgport = NULL;
static char *pguser = NULL;
static char *pgpass = NULL;

/*
 * Structure for the linked-list to hold oid-name lookup cache.
 */
struct oid2name_t {
	Oid oid;
	char *name;
	struct oid2name_t *next;
};

/*
 * Head element of the linked-list.
 */
struct oid2name_t *oid2name_table = NULL;

static char *cache_get(Oid);
static struct oid2name_t *cache_put(Oid, char *);

/*
 * cache_get()
 *
 * looks up the oid-name mapping cache table. If not found, returns NULL.
 */
static char *
cache_get(Oid oid)
{
	struct oid2name_t *curr = oid2name_table;

	while (curr!=NULL)
	{
	  //		printf("DEBUG: find %d %s\n", curr->oid, curr->name);

		if (curr->oid == oid)
			return curr->name;

		curr = curr->next;
	}

	return NULL;
}

/*
 * cache_put()
 *
 * puts a new entry to the tail of the oid-name mapping cache table.
 */
static struct oid2name_t *
cache_put(Oid oid, char *name)
{
	struct oid2name_t *curr = oid2name_table;

	if (!curr)
	{
		curr = (struct oid2name_t *)malloc( sizeof(struct oid2name_t) );
		memset(curr, 0, sizeof(struct oid2name_t));

		curr->oid = oid;
		curr->name = strdup(name);

		oid2name_table = curr;

		//		printf("DEBUG: put %d %s\n", curr->oid, curr->name);

		return curr;
	}

	while (curr->next!=NULL)
	{
		curr = curr->next;
	}

	curr->next = (struct oid2name_t *)malloc( sizeof(struct oid2name_t) );
	memset(curr->next, 0, sizeof(struct oid2name_t));
	curr->next->oid = oid;
	curr->next->name = strdup(name);

	//	printf("DEBUG: put %d %s\n", curr->next->oid, curr->next->name);

	return curr->next;
}

/*
 * Open a database connection
 */
bool
DBConnect(const char *host, const char *port, char *database, const char *user)
{
	char	*password = NULL;
	char	*password_prompt = NULL;
	bool	need_pass;

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

	if (password)
		pgpass = strdup(password);

	/* Check to see that the backend connection was successfully made */
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "Connection to database failed: %s",
			PQerrorMessage(conn));
		return false;
	}
	
	return true;
}

/*
 * Atempt to read the name of tablespace into lastSpcName
 * (if there's a database connection and the oid changed since lastSpcOid)
 */
char *
getSpaceName(uint32 space, char *buf, size_t buflen)
{
	char dbQry[1024];
	PGresult *res = NULL;

	if (cache_get(space))
	{
		snprintf(buf, buflen, "%s", cache_get(space));
		return buf;
	}

	if (conn)
	{
		//		printf("DEBUG: getSpaceName: SELECT spcname FROM pg_tablespace WHERE oid = %i\n", space);
		snprintf(dbQry, sizeof(dbQry), "SELECT spcname FROM pg_tablespace WHERE oid = %i", space);
		res = PQexec(conn, dbQry);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);
			//			printf("DEBUG: getSpaceName: spcname = %s\n", buf);
			cache_put(space, buf);
			PQclear(res);
			return buf;
		}
	}

	PQclear(res);

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
	char dbQry[1024];
	PGresult *res = NULL;

	if (cache_get(db))
	{
		snprintf(buf, buflen, "%s", cache_get(db));
		return buf;
	}

	if (conn)
	{	
		snprintf(dbQry, sizeof(dbQry), "SELECT datname FROM pg_database WHERE oid = %i", db);
		res = PQexec(conn, dbQry);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);

			cache_put(db, buf);

			// Database changed makes new connection
			if (lastDbConn)
				PQfinish(lastDbConn);

			lastDbConn = PQsetdbLogin(pghost,
						  pgport,
						  NULL,
						  NULL,
						  buf,
						  pguser,
						  pgpass);

			PQclear(res);
			return buf;
		}
	}

	PQclear(res);

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
	char dbQry[1024];
	PGresult *res = NULL;

	if (cache_get(relid))
	{
		snprintf(buf, buflen, "%s", cache_get(relid));
		return buf;
	}

	if (conn && lastDbConn)
	{
		/* Try the relfilenode and oid just in case the filenode has changed
		   If it has changed more than once we can't translate it's name */
		snprintf(dbQry, sizeof(dbQry), "SELECT relname, oid FROM pg_class WHERE relfilenode = %i OR oid = %i", relid, relid);
		res = PQexec(lastDbConn, dbQry);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
			PQclear(res);
			return NULL;
		}
		if(PQntuples(res) > 0)
		{
			strncpy(buf, PQgetvalue(res, 0, 0), buflen);
			cache_put(relid, buf);
			PQclear(res);
			return buf;
		}
	}

	PQclear(res);
	
	/* Didn't find the name, return string with oid */
	snprintf(buf, buflen, "%u", relid);
	return buf;
}

int
relname2attr_begin(const char *relname)
{
	char dbQry[1024];

	if ( _res!=NULL )
		PQclear(_res);

	snprintf(dbQry, sizeof(dbQry), "SELECT attname, atttypid FROM pg_attribute a, pg_class c WHERE attnum > 0 AND attrelid = c.oid AND c.relname='%s' ORDER BY attnum", relname);
	_res = PQexec(lastDbConn, dbQry);
	if (PQresultStatus(_res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "SELECT FAILED: %s", PQerrorMessage(conn));
		PQclear(_res);
		_res = NULL;
		return -1;
	}

	return PQntuples(_res);
}

int
relname2attr_fetch(int i, char *attname, Oid *atttypid)
{
	snprintf(attname, NAMEDATALEN, PQgetvalue(_res, i, 0));
	*atttypid = atoi( PQgetvalue(_res, i, 1) );
	return i;
}

void
relname2attr_end()
{
	PQclear(_res);
	_res = NULL;
}

bool
oid2name_enabled(void)
{
	return (conn!=NULL);
}

void
DBDisconnect(void)
{
	if(lastDbConn)
		PQfinish(lastDbConn);
	if(conn)
		PQfinish(conn);

}
